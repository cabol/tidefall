defmodule Tidefall.Buffer.Partition do
  @moduledoc """
  Buffer partition.

  The implementation is based on
  [OpenTelemetry Batch Processor][otel_batch_processor].
  The use case is very similar. The **"OTel batch processor"** buffers spans
  (large/massive amounts of them) and then exports them to an external source
  after some time (via OTLP). It is designed and implemented for efficiently
  handling large workloads. The partitioned buffer takes inspiration from the
  **"OTel batch processor"** to leverage all these properties.

  [otel_batch_processor]: https://github.com/open-telemetry/opentelemetry-erlang/blob/main/apps/opentelemetry/src/otel_batch_processor.erl

  ## Double-Buffering Processing Cycle

  ```asciidoc
  Phase 1 — Buffering          Phase 2 — Processing
  ===================          ====================

  Client writes                 Timer fires :processing
       |                              |
       v                              v
  +-----------+                 Allocate a fresh ETS table
  | Table A   |<-- write        and point Tidefall.Metadata
  | (current) |                 at it (writes flow there now)
  +-----------+                       |
                                      v
                                +-----------+
                                | Table B   |<-- write (new current)
                                +-----------+
                                | Table A   |-- give_away --> Task
                                +-----------+       |
                                                    v
                                              :ets.select (batches)
                                                    |
                                              processor(batch)
                                                    |
                                              batch mode:
                                                :ets.delete(Table A)
                                              :table mode:
                                                processor owns Table A
                                                    |
                                              :processing_completed
  ```

  ## ETS access

  This module is ETS-agnostic by design. It owns the two backing
  tables (allocation, swap, give-away on processing, teardown on
  terminate) but delegates the data-shape concerns — record
  layout, processor batch shape, conditional-update logic — to the
  buffer implementation modules (`Tidefall.Queue`, `Tidefall.HashMap`,
  …) through the `Tidefall.Buffer` behaviour callbacks
  `ets_table_opts/0` and `ets_match_spec/0`.

  Impl modules read the active write table by calling
  `current_table/1` and then issue their own `:ets.*` operations.
  """

  use GenServer

  alias Tidefall.Metadata

  # Internal state. The current write table lives in `Tidefall.Metadata`
  # (read on every write); the partition state only tracks the in-flight
  # `runner_task` so its completion/DOWN messages can be correlated.
  defstruct buffer: nil,
            partition: nil,
            partition_index: nil,
            module: nil,
            processor: nil,
            processing_interval: nil,
            processing_timeout: nil,
            processing_batch_size: nil,
            task_supervisor_name: nil,
            runner_task: nil,
            timer_ref: nil,
            processing?: false,
            start_time: nil

  # Telemetry prefix
  @telemetry_prefix [:tidefall, :partition]

  ## API

  @doc """
  Starts a buffer partition.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts)
  end

  @doc """
  Returns the partition's current write table.

  This is the **only sanctioned access** to the active table.
  Buffer implementations call this and then issue their own
  `:ets.*` operations against the returned table reference.

  > #### Do not cache {: .warning}
  >
  > The returned reference is single-use. The partition swaps
  > it on every processing tick; holding it across calls (e.g.
  > caching in a struct, sharing across processes, or pipelining
  > many ETS calls after a stale lookup) is a correctness bug.
  > Resolve it fresh inside each operation.
  """
  @spec current_table(partition :: atom()) :: :ets.table()
  def current_table(partition) do
    partition
    |> current_table_key()
    |> Metadata.get()
  end

  @doc """
  Returns the partition's buffer size.
  """
  @spec buffer_size(atom()) :: non_neg_integer()
  def buffer_size(partition) do
    partition
    |> current_table()
    |> :ets.info(:size)
  end

  @doc """
  Updates the options for the partition.
  """
  @spec update_options(GenServer.server(), keyword()) :: :ok
  def update_options(server, opts) do
    GenServer.call(server, {:update_options, opts})
  end

  ## GenServer callbacks

  @impl true
  def init(opts) do
    # Trap exit signals (make sure dying gracefully)
    Process.flag(:trap_exit, true)

    # Get options
    buffer = Keyword.fetch!(opts, :name)
    module = Keyword.fetch!(opts, :module)
    partition_index = Keyword.fetch!(opts, :partition_index)
    processing_interval = Keyword.fetch!(opts, :processing_interval)
    processing_timeout = Keyword.fetch!(opts, :processing_timeout)
    processing_batch_size = Keyword.fetch!(opts, :processing_batch_size)
    processor = Keyword.fetch!(opts, :processor)

    # Generate the partition name (used as Registry key, GenServer state,
    # and a debug hint for ETS table allocations)
    partition = Module.concat([buffer, to_string(partition_index)])

    # Allocate the initial write table. Subsequent tables are allocated
    # fresh on each processing swap — there is no fixed pair of named
    # tables to alternate between.
    :ok =
      partition
      |> new_processing_table(module)
      |> then(&put_current_table(partition, &1))

    # Register the partition
    with {:ok, _} <- Registry.register(Tidefall.Registry, buffer, partition) do
      start_time = System.monotonic_time()

      # Emit start event
      :telemetry.execute(
        @telemetry_prefix ++ [:start],
        %{system_time: System.system_time()},
        %{buffer: buffer, partition: partition}
      )

      # Build the state
      state = %__MODULE__{
        buffer: buffer,
        partition: partition,
        partition_index: partition_index,
        module: module,
        processor: processor,
        processing_interval: processing_interval,
        processing_timeout: processing_timeout,
        processing_batch_size: processing_batch_size,
        task_supervisor_name: Module.concat([buffer, TaskSupervisor]),
        start_time: start_time
      }

      {:ok, state, {:continue, :start_processing_timer}}
    end
  end

  @impl true
  def handle_continue(:start_processing_timer, state) do
    {:noreply, refresh_timer(state)}
  end

  @impl true
  def handle_call({:update_options, opts}, _from, state) do
    state =
      state
      |> struct!(opts)
      |> refresh_timer()

    {:reply, :ok, state}
  end

  @impl true
  def handle_info(message, state)

  # It's time to process the messages, but there is a processing cycle in progress already
  def handle_info(:processing, %__MODULE__{processing?: true} = state) do
    # Postpone the next processing
    {:noreply, refresh_timer(state)}
  end

  # It's time to process the messages
  def handle_info(:processing, %__MODULE__{processing?: false} = state) do
    # Process messages and reset the processing interval
    state =
      state
      |> process_messages()
      |> refresh_timer()

    {:noreply, state}
  end

  # Process task completed successfully
  def handle_info(
        {ref, :processing_completed},
        %__MODULE__{runner_task: %Task{ref: ref}} = state
      ) do
    # We don't care about the DOWN message now, so let's demonitor and flush it
    Process.demonitor(ref, [:flush])

    # Make sure to complete the processing properly
    {:noreply, complete_processing(state)}
  end

  # Process task failed
  def handle_info(
        {:DOWN, ref, :process, _from, reason},
        %__MODULE__{runner_task: %Task{ref: ref}, buffer: buffer, partition: partition} =
          state
      ) do
    # Emit processing task failed event
    :telemetry.execute(
      @telemetry_prefix ++ [:processing_failed],
      %{system_time: System.system_time()},
      %{buffer: buffer, partition: partition, reason: reason}
    )

    {:noreply, complete_processing(state)}
  end

  @impl true
  def terminate(
        reason,
        %__MODULE__{
          buffer: buffer,
          partition: partition,
          start_time: start_time,
          module: module,
          processing_batch_size: batch_size,
          processor: processor
        }
      ) do
    # Process messages before dying
    # `process_batch` is used to perform a blocking process
    partition
    |> current_table()
    |> process_batch(module, batch_size, processor)
  after
    # Delete the metadata entry so observers don't hold a stale tid
    # after the GenServer (and its ETS tables) is gone.
    partition
    |> current_table_key()
    |> Metadata.delete()

    # Emit stop event
    :telemetry.execute(
      @telemetry_prefix ++ [:stop],
      %{duration: System.monotonic_time() - start_time},
      %{buffer: buffer, partition: partition, reason: reason}
    )
  end

  ## Private functions

  # The processing is performed asynchronously to better handle the
  # read-and-write concurrency.
  defp process_messages(
         %__MODULE__{
           buffer: buffer,
           partition: partition,
           module: module,
           task_supervisor_name: task_supervisor_name,
           processor: processor,
           processing_timeout: processing_timeout,
           processing_batch_size: batch_size
         } = state
       ) do
    # Get the current writing ETS table.
    current_table = current_table(partition)

    # Get the current table size
    size = :ets.info(current_table, :size)

    # Check if the current table has data to process
    if size > 0 do
      # Allocate a fresh ETS table for incoming writes and swap the
      # "current table" pointer over to it. The previous table is then
      # isolated for processing.
      :ok =
        partition
        |> new_processing_table(module)
        |> then(&put_current_table(partition, &1))

      # Get the current process so the task can send the result back to it
      self = self()

      # Spawn a separate task to run the processing on the previous table
      task =
        Task.Supervisor.async_nolink(
          task_supervisor_name,
          fn -> send_messages(self, buffer, partition, size, module, batch_size, processor) end,
          shutdown: processing_timeout
        )

      # Give away the previous current table to the processing task to isolate
      # the process operation. In batch mode the task deletes the table after
      # draining it. In :table mode the processor takes ownership: when the
      # task exits, ETS auto-deletes the table unless the processor has
      # already transferred it elsewhere via `:ets.give_away/3`.
      true = :ets.give_away(current_table, task.pid, :process)

      # Update the state acknowledging the process is in the "processing" state.
      %{state | processing?: true, runner_task: task}
    else
      # Nothing to do if the table is empty
      state
    end
  end

  # Function for handling the processing asynchronously
  defp send_messages(from, buffer, partition, size, module, batch_size, processor) do
    # Trap exits for the `:shutdown` timeout to have an effect
    # See `Task.Supervisor.async_nolink/3` for more info
    Process.flag(:trap_exit, true)

    # Telemetry metadata for the span
    metadata = %{
      buffer: buffer,
      partition: partition
    }

    # Emit a Telemetry span to keep track of the processing duration
    :telemetry.span(@telemetry_prefix ++ [:processing], metadata, fn ->
      # Receive the table transfer message
      receive do
        {:"ETS-TRANSFER", table, ^from, :process} ->
          # Process the table data
          :ok = process_batch(table, module, batch_size, processor)

          # In batch mode we own the table and delete it after draining.
          # In :table mode the processor took ownership: leave the table
          # alone (the task's death will auto-delete it unless the
          # processor handed it off elsewhere).
          if batch_size != :table do
            true = :ets.delete(table)
          end

          # Acknowledge the process is completed
          {:processing_completed, %{size: size}, metadata}
      end
    end)
  end

  # Complete the processing: just clear the in-flight bookkeeping. The
  # next swap will allocate its own fresh table — there's no name to
  # reclaim here.
  defp complete_processing(state) do
    %{state | processing?: false, runner_task: nil}
  end

  # Table mode: pass the ETS table directly to the processor
  defp process_batch(table, _module, :table, processor) do
    invoke_processor(processor, table)

    :ok
  end

  # Batch mode: read from the ETS table in batches via the impl's match spec
  defp process_batch(table, module, batch_size, processor) do
    table
    |> :ets.select(module.ets_match_spec(), batch_size)
    |> process_batch(processor)
  end

  # We're finished!
  defp process_batch(:"$end_of_table", _processor) do
    :ok
  end

  # We're continuing!
  defp process_batch({results, continuation}, processor) do
    # Invoke the processor function
    invoke_processor(processor, results)

    # Continue processing the next batch
    continuation
    |> :ets.select()
    |> process_batch(processor)
  end

  # MFA processor
  defp invoke_processor({m, f, a}, results) do
    apply(m, f, [results | a])
  end

  # Function processor
  defp invoke_processor(fun, results) when is_function(fun, 1) do
    fun.(results)
  end

  defp refresh_timer(%__MODULE__{timer_ref: timer_ref, processing_interval: interval} = state) do
    if timer_ref, do: Process.cancel_timer(timer_ref)

    timer_ref = Process.send_after(self(), :processing, interval)

    %{state | timer_ref: timer_ref}
  end

  # `partition` is the debug hint shown by ETS introspection — no atoms
  # are allocated per table because the table is unnamed (`:named_table`
  # is intentionally NOT in `ets_table_opts/0`). The returned tid is the
  # only handle to the table.
  defp new_processing_table(partition, module) do
    :ets.new(partition, module.ets_table_opts())
  end

  @compile inline: [current_table_key: 1]
  defp current_table_key(partition) do
    {__MODULE__, :current_table, partition}
  end

  defp put_current_table(partition, table) do
    partition
    |> current_table_key()
    |> Metadata.put(table)
  end
end
