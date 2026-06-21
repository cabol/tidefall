defmodule Tidefall.Queue do
  @moduledoc """
  ETS-based queue buffer for high-throughput data processing.

  `Tidefall.Queue` buffers arbitrary data in insertion order and
  periodically processes it using a configurable processor function.
  The queue is **drain-only**: there is no `pop`/`dequeue`. Producers
  `push/3` items, and the engine drains the buffer to the processor on
  each processing tick.
  It implements partitioning to reduce lock contention during high-throughput
  writes, and uses double-buffering to ensure zero-downtime processing.

  ## Data Flow

  ```asciidoc
  push(buffer, items)
         |
         v
  +-------------------+
  | Partition Routing |
  | phash2(item, N)   |
  +-------------------+
     |         |         |
     v         v         v
  +-------+ +-------+ +-------+     ETS :ordered_set
  | P 0   | | P 1   | | P N-1 |     Key: {sort_key, ref}
  +-------+ +-------+ +-------+     Val: item
     |         |         |
     v         v         v
  +--------------------------------------+
  | processor(batch)                     |
  | batch = [val1, val2, ...]            |
  +--------------------------------------+
  ```

  Items are routed to partitions via `phash2`, stored in
  `:ordered_set` ETS tables keyed by `{sort_key, ref}`, and
  periodically flushed to the processor in batches. `sort_key`
  is `System.monotonic_time()` by default — giving insertion-time
  order — or the term produced by the `:sort_key` runtime option;
  `ref` keeps every key unique so no item is ever overwritten.
  Ordering is **per partition** (see `:sort_key` under runtime options).

  ## Start options

  #{Tidefall.Buffer.Options.start_options_docs()}

  ## Runtime options

  #{Tidefall.Queue.Options.runtime_options_docs()}

  ## Examples

  ### Standalone Usage

      # Start a queue buffer with a custom processor
      iex> {:ok, _sup_pid} =
      ...>   Tidefall.Queue.start_link(
      ...>     name: :my_buffer,
      ...>     processor: fn batch -> IO.inspect(batch) end
      ...>   )

      # Push a single item into the buffer
      iex> Tidefall.Queue.push(:my_buffer, "item1")
      :ok

      # Push a batch of items
      iex> Tidefall.Queue.push(:my_buffer, ["item2", "item3"])
      :ok

      # Check buffer size
      iex> Tidefall.Queue.size(:my_buffer)
      3

      # Stop the buffer gracefully (processes remaining items)
      iex> Tidefall.Queue.stop(:my_buffer)
      :ok

  ### Adding to a Supervision Tree

      children = [
        {Tidefall.Queue,
         name: :my_buffer,
         processor: &MyApp.EventProcessor.process_batch/1}
      ]

      Supervisor.start_link(children, strategy: :one_for_one)

  ## Defining a buffer module

  For the recommended **module-based** pattern — `use Tidefall.Queue`,
  where the module name becomes the default instance and start options
  layer across compile-time `use` opts, the application environment, and
  explicit opts — see the
  [Module-based buffers](`m:Tidefall#module-module-based-buffers-recommended`)
  section of `Tidefall`.

  ## Processor

  The processor function receives a list of values
  (the items pushed to the buffer):

      fn batch ->
        # batch is [value1, value2, ...]
        Enum.each(batch, fn value -> process(value) end)
      end

  See [The processor](`m:Tidefall#module-the-processor`) for when it runs,
  batching, failure isolation, and shutdown-drain behavior.

  """

  @behaviour Tidefall.Buffer

  import Record, only: [defrecordp: 2]

  alias Tidefall.Buffer
  alias Tidefall.Buffer.{Definition, Partition}
  alias Tidefall.Queue.Options

  # Queue-specific key record.
  #
  # `sort_key` is the primary ordering term — `System.monotonic_time()`
  # by default (insertion order), or whatever the `:sort_key` runtime
  # option resolves to. `ref` is always retained as the uniqueness
  # tiebreaker: since `make_ref()` is unique, distinct items never
  # collide in the `:ordered_set`, so nothing is overwritten even when
  # two items share the same `sort_key`.
  defrecordp(:key, sort_key: nil, ref: nil)

  # Entry record stored in ETS. Queue only needs key/value; the
  # match spec returns just the value to the processor.
  defrecordp(:entry, key: nil, value: nil)

  @typedoc "Any term that will be buffered and processed"
  @type item() :: any()

  @typedoc "Proxy type for a buffer"
  @type buffer() :: Tidefall.Buffer.buffer()

  ## Definition module

  @doc false
  defmacro __using__(opts) do
    # Public operations delegated to the definition module. Each entry is
    # `{name, leading_params, min_optional, max_optional}` — `leading_params`
    # counts the required non-buffer/non-opts params, the optional window
    # drives the distinct nameless arities. See `Tidefall.Buffer.Definition`.
    ops = [
      {:push, 1, 0, 1},
      {:size, 0, 0, 0},
      {:update_options, 1, 0, 0},
      {:stop, 0, 0, 2}
    ]

    Definition.define(__MODULE__, ops, opts)
  end

  ## API

  @doc """
  Starts a new queue buffer.

  ## Options

  See [start options](`m:Tidefall.Queue#module-start-options`).

  ## Examples

      Tidefall.Queue.start_link(
        name: :my_queue_buffer,
        processor: &MyApp.Sink.process/1
      )

  """
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts \\ []) do
    opts
    |> Keyword.put(:module, __MODULE__)
    |> Buffer.start_link()
  end

  @doc """
  Stops a queue buffer gracefully.

  ## Examples

      Tidefall.Queue.stop(:my_queue_buffer)

  """
  @spec stop(buffer() | pid(), reason :: any(), timeout()) :: :ok
  defdelegate stop(buffer, reason \\ :normal, timeout \\ :infinity), to: Buffer

  @doc """
  Returns the queue buffer child spec.
  """
  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    %{
      id: opts[:name] || __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      type: :supervisor
    }
  end

  @doc """
  Pushes an item or a batch of items into the buffer.

  ## Parameters

    * `buffer` - The buffer name (atom).
    * `item_or_batch` - A single item or a list of items to push.
    * `opts` - Optional runtime options.

  ## Options

  See [runtime options](`m:Tidefall.Queue#module-runtime-options`).

  ## Examples

      # Simple push with default routing
      push(:my_buffer, "item1")
      push(:my_buffer, ["item2", "item3"])

      # Custom partition routing using function
      push(:my_buffer, user_event, partition_key: &(&1.user_id))

      # Custom partition routing using MFA tuple (item prepended to args)
      push(:my_buffer, event, partition_key: {MyApp.Router, :get_partition, []})

      # Custom partition routing with fixed key (all items to same partition)
      push(:my_buffer, log_entry, partition_key: :logs)

      # Custom ordering: drain by a value-derived sort key (per partition)
      push(:my_buffer, event, sort_key: & &1.priority)

  """
  @spec push(buffer(), item() | [item()], keyword()) :: :ok
  def push(buffer, item_or_batch, opts \\ [])

  def push(buffer, batch, opts) when is_list(batch) do
    opts = Options.validate_runtime_options!(opts)
    partition_key = Keyword.fetch!(opts, :partition_key)
    sort_key = Keyword.get(opts, :sort_key)

    batch
    |> Enum.group_by(&Buffer.get_partition(buffer, partition_key, &1))
    |> Enum.each(fn {partition, items} ->
      partition
      |> Partition.current_table()
      |> :ets.insert(Enum.map(items, &new_entry(build_key(sort_key, &1), &1)))
    end)
  end

  def push(buffer, item, opts) do
    push(buffer, [item], opts)
  end

  @doc """
  Returns the queue size (total number of items across all partitions).

  ## Examples

      size(:my_buffer)

  """
  @spec size(buffer()) :: non_neg_integer()
  defdelegate size(buffer), to: Buffer, as: :buffer_size

  @doc """
  Updates the options for the queue buffer.

  ## Options

  Updatable options: `:processing_interval`, `:processing_timeout`,
  `:processing_batch_size`, `:drain_threshold`, `:drain_check_interval`. See
  [start options](`m:Tidefall.Queue#module-start-options`) for each option's
  semantics.

  ## Examples

      # Update the processing interval to 100ms
      update_options(:my_buffer, processing_interval: 100)

  """
  @spec update_options(buffer(), keyword()) :: :ok
  defdelegate update_options(buffer, opts), to: Buffer

  ## Callbacks

  @impl Tidefall.Buffer
  def ets_table_opts do
    [
      :ordered_set,
      :public,
      keypos: entry(:key) + 1,
      write_concurrency: true,
      decentralized_counters: true
    ]
  end

  @impl Tidefall.Buffer
  def ets_match_spec do
    [
      {
        entry(key: :"$1", value: :"$2"),
        [true],
        [:"$2"]
      }
    ]
  end

  ## Private functions

  # Iniline common instructions
  @compile inline: [new_entry: 2]

  # Default (no `:sort_key`): order by insertion time.
  defp build_key(nil, _item) do
    key(sort_key: System.monotonic_time(), ref: make_ref())
  end

  # Arity-1: derive the sort term from the item.
  defp build_key(fun, item) when is_function(fun, 1) do
    key(sort_key: fun.(item), ref: make_ref())
  end

  # Arity-0: generate the sort term at push time.
  defp build_key(fun, _item) when is_function(fun, 0) do
    key(sort_key: fun.(), ref: make_ref())
  end

  defp new_entry(key, value), do: entry(key: key, value: value)
end
