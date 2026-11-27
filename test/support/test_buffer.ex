defmodule Tidefall.TestBuffer do
  @moduledoc false

  # Starts, ticks and stops buffers for property tests, with a
  # processor that sends each batch to the test process. There's no
  # public API to trigger a tick and wait for it, so this is the one
  # place tests touch partition internals: the registry, the
  # `:processing` message and the `processing?` field.
  #
  # ExUnit isn't in the dialyzer PLT, so this module raises instead
  # of calling `assert_receive`.

  alias Tidefall.Buffer

  @doc """
  Starts `module` as `name` with `opts`, a 1 h processing interval
  so no tick fires on its own, and a processor that sends
  `{:batch, batch}` to the caller.

  A failing case leaves its buffer up and the shrinker reruns the
  body, so a buffer still running under `name` is stopped first.
  """
  @spec start!(module(), atom(), keyword()) :: :ok
  def start!(module, name, opts) do
    if registered?(name) do
      stop!(module, name)
      flush_batches()
    end

    test = self()

    {:ok, _} =
      [
        name: name,
        processing_interval: :timer.hours(1),
        processor: &send(test, {:batch, &1})
      ]
      |> Keyword.merge(opts)
      |> module.start_link()

    :ok
  end

  @doc """
  Stops the buffer, which drains every partition synchronously, and
  waits until its partitions leave the registry.
  """
  @spec stop!(module(), atom()) :: :ok
  def stop!(module, name) do
    :ok = module.stop(name)

    # Registry cleanup is async; a restart before it ends finds the
    # old partitions.
    await_unregistered(name)
  end

  @doc """
  Runs one tick on the buffer's only partition and waits for it to
  end, processor calls included.
  """
  @spec tick!(atom()) :: :ok
  def tick!(buffer) do
    [{partition, _}] = Registry.lookup(Tidefall.Registry, buffer)

    # An empty tick emits no event, so there's nothing to wait for.
    if Buffer.buffer_size(buffer) > 0 do
      with_tick_handler(buffer, fn ->
        send(partition, :processing)
        await_tick_done(buffer)
      end)
    else
      send(partition, :processing)
    end

    # The task emits its event before the partition handles its
    # completion, and a tick that arrives in between is skipped.
    await_idle(partition)
  end

  @doc """
  Every batch in the mailbox, concatenated. See `batches/0`.
  """
  @spec flush_batches() :: list()
  def flush_batches do
    Enum.concat(batches())
  end

  @doc """
  Every batch in the mailbox, one list per batch. `tick!/1` and
  `stop!/2` wait for their batches, so after either one they're all
  here.
  """
  @spec batches() :: [list()]
  def batches do
    receive do
      {:batch, batch} -> [batch | batches()]
    after
      0 -> []
    end
  end

  @doc false
  def handle_event(_event, _measurements, %{buffer: buffer}, pid) do
    send(pid, {:tick_done, buffer})
  end

  ## Private

  defp with_tick_handler(buffer, fun) do
    id = {__MODULE__, buffer}

    :ok =
      :telemetry.attach(
        id,
        [:tidefall, :partition, :processing, :stop],
        &__MODULE__.handle_event/4,
        self()
      )

    try do
      fun.()
    after
      :telemetry.detach(id)
    end
  end

  # The task sends its batches and then the event, so once the event
  # is here, so is every batch.
  defp await_tick_done(buffer) do
    receive do
      {:tick_done, ^buffer} -> :ok
    after
      :timer.seconds(1) -> raise "the tick on #{inspect(buffer)} didn't finish within 1s"
    end
  end

  defp await_idle(partition) do
    if :sys.get_state(partition).processing? do
      await_idle(partition)
    else
      :ok
    end
  end

  defp await_unregistered(name) do
    if registered?(name) do
      await_unregistered(name)
    else
      :ok
    end
  end

  defp registered?(name) do
    Registry.lookup(Tidefall.Registry, name) != []
  end
end
