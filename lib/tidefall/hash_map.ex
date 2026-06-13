defmodule Tidefall.HashMap do
  @moduledoc """
  Last-write-wins key/value buffer where same-key writes coalesce
  between processing ticks.

  `Tidefall.HashMap` buffers key-value entries in an ETS `:set` — a hash
  table, hence the name — so that writes to the same key overwrite each
  other and only the latest value survives to the next processing tick.
  It periodically processes buffered entries using a configurable processor
  function. Like `Queue`, it implements partitioning to reduce lock
  contention during high-throughput writes, and uses double-buffering to
  ensure zero-downtime processing.

  It also supports versioned conditional updates via `put_newer/4` and
  `put_all_newer/3`, which use "newer version wins" semantics — an entry is
  only written if the key doesn't exist or the new version is greater than
  the existing one.

  ## Data Flow

  ```asciidoc
  put(buffer, key, value)          put_newer(buffer, key, value, opts)
         |                                    |
         v                                    v
  +--------------------+            +--------------------+
  | Partition Routing  |            | Partition Routing  |
  | phash2(key, N)     |            | phash2(key, N)     |
  +--------------------+            +--------------------+
         |                                    |
         v                                    v
  +------------------+              +---------------------------+
  | :ets.insert      |              | 1. :ets.insert_new        |
  | (last-write-wins)|              |    Key new? -> inserted   |
  +------------------+              | 2. :ets.select_replace    |
                    \\              |    new_ver > old_ver?     |
                     \\             |    Yes -> updated         |
                      \\            |    No  -> skipped         |
                       \\           +---------------------------+
                        \\            /
                         v           v
              +-------------------------------------+
              | ETS :set                            |
              | (key, raw_key, value, version,      |
              |  updates)                           |
              +-------------------------------------+
                            |
                            v
              +--------------------------------------+
              | processor(batch)                     |
              | batch = [%Entry{key, value, version, |
              |                 updates}, ...]       |
              +--------------------------------------+
  ```

  Entries are routed to partitions via `phash2(key)` and stored in
  `:set` ETS tables. Regular `put/4` uses simple `ets:insert`
  (last-write-wins). Versioned `put_newer/4` uses a two-step
  atomic approach: `ets:insert_new` for new keys, then
  `ets:select_replace` for conditional "newer version wins"
  updates.

  ### Complex keys and `:key_hasher`

  ETS match specs (used by `put_newer/4`/`put_all_newer/3`)
  cannot equality-compare arbitrary terms in the match head — in
  particular, **maps anywhere in the key** fall back to subset
  semantics and may match the wrong row. To support complex
  keys, pass `:key_hasher` on every operation that touches a
  given key (see the per-function docs for the option).

  ## Start options

  #{Tidefall.Buffer.Options.start_options_docs()}

  ## Runtime options

  #{Tidefall.HashMap.Options.runtime_options_docs()}

  Additional options accepted by `put_newer/4`:

  #{Tidefall.HashMap.Options.put_newer_options_docs()}

  ## Examples

  ### Standalone Usage

      # Start a HashMap buffer with a custom processor
      iex> {:ok, _sup_pid} =
      ...>   Tidefall.HashMap.start_link(
      ...>     name: :my_hash_map_buffer,
      ...>     processor: fn batch -> IO.inspect(batch) end
      ...>   )

      # Put a single entry
      iex> Tidefall.HashMap.put(:my_hash_map_buffer, :key1, "value1")
      :ok

      # Put multiple entries at once
      iex> Tidefall.HashMap.put_all(:my_hash_map_buffer, %{key2: "value2", key3: "value3"})
      :ok

      # Delete an entry
      iex> Tidefall.HashMap.delete(:my_hash_map_buffer, :key1)
      :ok

      # Versioned put (newer version wins)
      iex> Tidefall.HashMap.put_newer(:my_hash_map_buffer, :key4, "v1", version: 100)
      :ok
      iex> Tidefall.HashMap.put_newer(:my_hash_map_buffer, :key4, "v2", version: 200)
      :ok
      iex> Tidefall.HashMap.get(:my_hash_map_buffer, :key4)
      "v2"

      # Check buffer size
      iex> Tidefall.HashMap.size(:my_hash_map_buffer)
      3

      # Stop the buffer gracefully (processes remaining items)
      iex> Tidefall.HashMap.stop(:my_hash_map_buffer)
      :ok

  ### Adding to a Supervision Tree

      children = [
        {Tidefall.HashMap,
         name: :my_hash_map_buffer,
         processor: &MyApp.EventProcessor.process_batch/1}
      ]

      Supervisor.start_link(children, strategy: :one_for_one)

  ## Defining a buffer module

  Instead of referring to a buffer by a runtime name, you can define a
  dedicated module with `use Tidefall.HashMap`. The module name becomes
  the default instance name, and start options can be layered from
  compile-time `use` opts, the application environment (`:otp_app` is
  required), and explicit `start_link`/child-spec opts (in that order of
  increasing precedence):

      defmodule MyApp.StateMap do
        use Tidefall.HashMap, otp_app: :my_app
      end

      # config/runtime.exs (optional — requires `otp_app:` above)
      config :my_app, MyApp.StateMap,
        processor: &MyApp.Sink.process/1,
        partitions: 4

      # supervision
      children = [{MyApp.StateMap, processing_interval: 5_000}]

      # calls on the default instance (named after the module)
      MyApp.StateMap.put(key, value)
      MyApp.StateMap.put_newer(key, value, version: v)
      MyApp.StateMap.get(key)

  The generated functions come in distinct arities: the nameless
  variants operate on the default instance (the module name), while a
  single full-arity variant takes the instance name as its first
  argument. To address a **dynamically started instance** of the same
  definition, use that full-arity form with all arguments explicit
  (including the trailing options):

      MyApp.StateMap.start_link(name: :tenant_a)
      MyApp.StateMap.put(:tenant_a, key, value, [])

  ## Processor

  The processor function receives a list of `t:Tidefall.HashMap.Entry.t/0`
  structs, where `:version` is the entry version (set via `put_newer/4` and
  `put_all_newer/3`; `0` for regular `put/4` entries), and `:updates` is the
  number of times an existing key was replaced by a newer version (only
  tracked for versioned updates; regular `put/4` entries always have
  `:updates` set to `0`):

      fn batch ->
        Enum.each(batch, fn %Tidefall.HashMap.Entry{key: k, value: v} ->
          process(k, v)
        end)
      end

  When `:key_hasher` is used, the entry's `:key` is the **original**
  (pre-hash) key — the hash is purely an internal ETS lookup detail.

  """

  @behaviour Tidefall.Buffer

  import Record, only: [defrecordp: 2]

  alias Tidefall.Buffer
  alias Tidefall.Buffer.{Definition, Partition}
  alias Tidefall.HashMap.{Entry, Options}

  # Entry record stored in ETS. `:key` is the ETS lookup key (the original
  # key when `:key_hasher` is not used, the hash when it is). `:raw_key`
  # holds the original key when hashing is in effect, and is `nil`
  # otherwise; the match spec uses it to emit the user-facing original
  # key on the processor batch.
  defrecordp(:entry, key: nil, raw_key: nil, value: nil, version: 0, updates: 0)

  @typedoc "Proxy type for a buffer"
  @type buffer() :: Tidefall.Buffer.buffer()

  @typedoc """
  Version stamp used to resolve conflicts in `put_newer/4` and
  `put_all_newer/3`. Restricted to integers, atoms, and binaries —
  see the `:version` option for semantics and the caveat about
  mixing types.
  """
  @type version() :: integer() | atom() | binary()

  @typedoc """
  A versioned key-value entry — the tuple shape accepted by
  `put_all_newer/3`. (Named `kv_entry` to avoid clashing with the
  private `entry` record.)
  """
  @type kv_entry() :: {key :: any(), value :: any(), version :: version()}

  @typedoc """
  Key-hashing option accepted by every HashMap operation:

    * `nil` (default) — no hashing; the user's key is stored and
      looked up as-is.
    * `true` — hash the key with `:erlang.phash2/1` before
      storage/lookup. Fast, **but 28-bit and so collision-prone**;
      use only when collisions are acceptable.
    * A function of arity 1 — applied to the user's key to produce
      the storage/lookup key. Use a cryptographic hash
      (e.g. `&:crypto.hash(:sha256, :erlang.term_to_binary(&1))`)
      if you need collision resistance.

  > #### Use consistently {: .warning}
  >
  > If you write a key with a given `:key_hasher`, you **must**
  > pass the same `:key_hasher` to every subsequent `get/4`,
  > `delete/3`, `put_newer/4`, and `put_all_newer/3` call for
  > that key. Otherwise the lookup will compute a different
  > storage key and miss the entry. Mixing hashed and non-hashed
  > writes for the same logical key produces two distinct entries
  > under the hood — that's a user error, not a library bug.
  """
  @type key_hasher() :: true | (any() -> any())

  ## Definition module

  @doc false
  defmacro __using__(opts) do
    # Public operations delegated to the definition module. Each entry is
    # `{name, leading_params, min_optional, max_optional}` — `leading_params`
    # counts the required non-buffer/non-opts params, the optional window
    # drives the distinct nameless arities. See `Tidefall.Buffer.Definition`.
    ops = [
      {:put, 2, 0, 1},
      {:put_all, 1, 0, 1},
      {:put_newer, 2, 0, 1},
      {:put_all_newer, 1, 0, 1},
      {:get, 1, 0, 2},
      {:delete, 1, 0, 1},
      {:size, 0, 0, 0},
      {:update_options, 1, 0, 0},
      {:stop, 0, 0, 2}
    ]

    Definition.define(__MODULE__, ops, opts)
  end

  ## API

  @doc """
  Starts a new HashMap buffer.

  ## Options

  See [start options](`m:Tidefall.HashMap#module-start-options`).

  ## Examples

      Tidefall.HashMap.start_link(name: :my_hash_map_buffer)

  """
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts \\ []) do
    opts
    |> Keyword.put(:module, __MODULE__)
    |> Buffer.start_link()
  end

  @doc """
  Stops a HashMap buffer gracefully.

  ## Examples

      Tidefall.HashMap.stop(:my_hash_map_buffer)

  """
  @spec stop(buffer() | pid(), reason :: any(), timeout()) :: :ok
  defdelegate stop(buffer, reason \\ :normal, timeout \\ :infinity), to: Buffer

  @doc """
  Returns the HashMap buffer child spec.
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
  Puts a single key-value entry into the buffer.

  ## Parameters

    * `buffer` - The buffer name (atom).
    * `key` - The key for the entry.
    * `value` - The value for the entry.
    * `opts` - Optional runtime options.

  ## Options

  See [runtime options](`m:Tidefall.HashMap#module-runtime-options`).

  ## Examples

      # Simple put
      put(:my_buffer, :key1, "val1")

      # With custom partition routing
      put(:my_buffer, :key1, "val1", partition_key: fn {k, _v} -> k end)

      # With key hashing (key is a map — store under a hash; processor
      # still receives the original map as the entry's :key)
      put(:my_buffer, %{tenant: "acme", id: 42}, "val1", key_hasher: true)

  """
  @spec put(buffer(), any(), any(), keyword()) :: :ok
  def put(buffer, key, value, opts \\ []) do
    put_all(buffer, [{key, value}], opts)
  end

  @doc """
  Puts multiple key-value entries into the buffer.

  Accepts either a map or a list of `{key, value}` tuples.

  ## Parameters

    * `buffer` - The buffer name (atom).
    * `entries` - A map or list of `{key, value}` tuples.
    * `opts` - Optional runtime options.

  ## Options

  See [runtime options](`m:Tidefall.HashMap#module-runtime-options`).

  ## Examples

      # Using a map
      put_all(:my_buffer, %{key1: "val1", key2: "val2"})

      # Using a list of tuples
      put_all(:my_buffer, [{:key1, "val1"}, {:key2, "val2"}])

      # With custom partition routing
      put_all(:my_buffer, %{key1: "val1"}, partition_key: fn {k, _v} -> k end)

  """
  @spec put_all(buffer(), map() | [{any(), any()}], keyword()) :: :ok
  def put_all(buffer, entries, opts \\ []) when is_map(entries) or is_list(entries) do
    opts = Options.validate_runtime_options!(opts)
    partition_key = Keyword.fetch!(opts, :partition_key)
    key_hasher = Keyword.get(opts, :key_hasher)

    entries
    |> Enum.group_by(fn {key, _value} ->
      # Route by the ORIGINAL key — the hash is purely a storage detail
      Buffer.get_partition(buffer, partition_key, key)
    end)
    |> Enum.each(fn {partition, kv_pairs} ->
      entries =
        Enum.map(kv_pairs, fn {key, value} ->
          {ets_key, raw_key} = resolve_keys(key, key_hasher)

          new_entry(ets_key, raw_key, value)
        end)

      true =
        partition
        |> Partition.current_table()
        |> :ets.insert(entries)
    end)
  end

  @doc """
  Puts a single versioned key-value entry into the buffer.

  Uses "newer version wins" semantics: the entry is only written if:
  - The key doesn't exist, or
  - The new version is greater than the existing version

  Useful for scenarios where you want to ensure only the latest
  version of data is stored, such as event sourcing or state
  synchronization.

  ## Parameters

    * `buffer` - The buffer name (atom).
    * `key` - The key for the entry.
    * `value` - The value for the entry.
    * `opts` - Optional runtime options.

  ## Options

  See [runtime options](`m:Tidefall.HashMap#module-runtime-options`).

  ## Examples

      # Default version (positive monotonic integer)
      put_newer(:my_buffer, :user_123, %{name: "Alice"})

      # Explicit version — e.g. a sequence number
      put_newer(:my_buffer, :counter, 42, version: 5)

      # Complex key with hashing (must use :key_hasher on get/delete too)
      put_newer(:my_buffer, %{tenant: "acme", id: 42}, "val", key_hasher: true)

  """
  @spec put_newer(buffer(), any(), any(), keyword()) :: :ok
  def put_newer(buffer, key, value, opts \\ []) do
    opts =
      opts
      |> Keyword.put_new_lazy(:version, fn ->
        :erlang.unique_integer([:monotonic, :positive])
      end)
      |> Options.validate_put_newer_options!()

    {version, opts} = Keyword.pop!(opts, :version)

    put_all_newer(buffer, [{key, value, version}], opts)
  end

  @doc """
  Puts multiple versioned key-value entries into the buffer.

  Uses "newer version wins" semantics for each entry.

  ## Parameters

    * `buffer` - The buffer name (atom).
    * `entries` - A list of `{key, value, version}` tuples.
    * `opts` - Optional runtime options.

  ## Options

  See [runtime options](`m:Tidefall.HashMap#module-runtime-options`).

  ## Examples

      entries = [
        {:user_1, %{name: "Alice"}, 100},
        {:user_2, %{name: "Bob"}, 200}
      ]
      put_all_newer(:my_buffer, entries)

  """
  @spec put_all_newer(buffer(), [kv_entry()], keyword()) :: :ok
  def put_all_newer(buffer, entries, opts \\ []) when is_list(entries) do
    validated_entries = Enum.map(entries, &validate_versioned_entry!/1)
    opts = Options.validate_runtime_options!(opts)
    partition_key = Keyword.fetch!(opts, :partition_key)
    key_hasher = Keyword.get(opts, :key_hasher)

    validated_entries
    |> Enum.group_by(fn {key, _value, _version} ->
      # Route by the ORIGINAL key — the hash is purely a storage detail
      Buffer.get_partition(buffer, partition_key, key)
    end)
    |> Enum.each(fn {partition, kv_entries} ->
      records =
        Enum.map(kv_entries, fn {k, v, version} ->
          {ets_key, raw_key} = resolve_keys(k, key_hasher)

          new_entry(ets_key, raw_key, v, version)
        end)

      do_put_newer(partition, records)
    end)
  end

  @doc """
  Gets the value for the given `key` from the buffer's current write table.

  Returns `default` if the key is not found.

  Note: This reads from the current write table only. Entries already handed
  off for processing will not be visible.

  ## Parameters

    * `buffer` - The buffer name (atom).
    * `key` - The key to look up.
    * `default` - The default value if key is not found (defaults to `nil`).
    * `opts` - Optional runtime options.

  ## Options

  See [runtime options](`m:Tidefall.HashMap#module-runtime-options`).

  ## Examples

      # Simple get
      get(:my_buffer, :key1)

      # With custom partition routing
      get(:my_buffer, :key1, partition_key: fn {k, _v} -> k end)

  """
  @spec get(buffer(), any(), any(), keyword()) :: any()
  def get(buffer, key, default \\ nil, opts \\ []) do
    opts = Options.validate_runtime_options!(opts)
    partition_key = Keyword.fetch!(opts, :partition_key)
    key_hasher = Keyword.get(opts, :key_hasher)

    {ets_key, _raw_key} = resolve_keys(key, key_hasher)

    buffer
    |> Buffer.get_partition(partition_key, key)
    |> Partition.current_table()
    |> :ets.lookup(ets_key)
    |> case do
      [entry(value: value)] -> value
      [] -> default
    end
  end

  @doc """
  Deletes a key from the buffer's current write table.

  Note: If the entry has already been handed off for processing (via
  double-buffering), this delete will not affect the in-flight batch.

  ## Parameters

    * `buffer` - The buffer name (atom).
    * `key` - The key to delete.
    * `opts` - Optional runtime options.

  ## Options

  See [runtime options](`m:Tidefall.HashMap#module-runtime-options`).

  ## Examples

      # Simple delete
      delete(:my_buffer, :key1)

      # With custom partition routing
      delete(:my_buffer, :key1, partition_key: fn {k, _v} -> k end)

  """
  @spec delete(buffer(), any(), keyword()) :: :ok
  def delete(buffer, key, opts \\ []) do
    opts = Options.validate_runtime_options!(opts)
    partition_key = Keyword.fetch!(opts, :partition_key)
    key_hasher = Keyword.get(opts, :key_hasher)
    partition = Buffer.get_partition(buffer, partition_key, key)

    {ets_key, _raw_key} = resolve_keys(key, key_hasher)

    true =
      partition
      |> Partition.current_table()
      |> :ets.delete(ets_key)

    :ok
  end

  @doc """
  Returns the HashMap buffer size (total number of entries across all partitions).

  ## Examples

      size(:my_buffer)

  """
  @spec size(buffer()) :: non_neg_integer()
  defdelegate size(buffer), to: Buffer, as: :buffer_size

  @doc """
  Updates the options for the HashMap buffer.

  ## Options

  Updatable options: `:processing_interval`, `:processing_timeout`,
  `:processing_batch_size`. See [start options](`m:Tidefall.HashMap#module-start-options`)
  for each option's semantics.

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
      :set,
      :public,
      keypos: entry(:key) + 1,
      write_concurrency: true,
      decentralized_counters: true
    ]
  end

  @impl Tidefall.Buffer
  def ets_match_spec do
    # Two clauses so the processor always sees the user's ORIGINAL key
    # in `%Entry{key: ...}`, regardless of whether `:key_hasher` was used
    # on write. Non-hashed entries have `:raw_key` == nil and the ETS key
    # IS the original; hashed entries have `:raw_key` set to the original
    # and the ETS key is the hash (an internal storage detail).
    [
      # Non-hashed entries
      {
        entry(key: :"$1", raw_key: nil, value: :"$2", version: :"$3", updates: :"$4"),
        [true],
        [%Entry{key: :"$1", value: :"$2", version: :"$3", updates: :"$4"}]
      },
      # Hashed entries — use raw_key as the user-facing key
      {
        entry(key: :_, raw_key: :"$1", value: :"$2", version: :"$3", updates: :"$4"),
        [{:"/=", :"$1", nil}],
        [%Entry{key: :"$1", value: :"$2", version: :"$3", updates: :"$4"}]
      }
    ]
  end

  ## Private functions

  # Iniline common instructions
  @compile [inline: [new_entry: 3, new_entry: 4]]

  defp new_entry(ets_key, raw_key, value) do
    entry(key: ets_key, raw_key: raw_key, value: value)
  end

  defp new_entry(ets_key, raw_key, value, version)
       when is_integer(version)
       when is_atom(version)
       when is_binary(version) do
    entry(key: ets_key, raw_key: raw_key, value: value, version: version)
  end

  # Resolves the `{ets_key, raw_key}` pair for storage/lookup.
  # `raw_key` is `nil` for non-hashed entries — distinguishing them
  # from hashed entries in the match spec.
  defp resolve_keys(key, nil), do: {key, nil}
  defp resolve_keys(key, true), do: {:erlang.phash2(key), key}
  defp resolve_keys(key, hasher) when is_function(hasher, 1), do: {hasher.(key), key}

  defp validate_versioned_entry!({_key, _value, version} = entry)
       when is_integer(version)
       when is_atom(version)
       when is_binary(version) do
    entry
  end

  defp validate_versioned_entry!(other) do
    raise ArgumentError, "invalid entry: #{inspect(other)}"
  end

  defp do_put_newer(partition, entries) do
    table = Partition.current_table(partition)

    Enum.each(entries, fn entry(
                            key: ets_key,
                            raw_key: raw_key,
                            value: value,
                            version: version
                          ) = entry ->
      # Try to insert as a new entry first
      case :ets.insert_new(table, entry) do
        true ->
          # Entry was new, we're done
          :ok

        false ->
          # Entry exists; try a conditional update via select_replace
          match_spec = replace_match_spec(ets_key, raw_key, value, version)

          :ets.select_replace(table, match_spec)
      end
    end)
  end

  defp replace_match_spec(ets_key, raw_key, value, version) do
    # Performance note: The key in the match head is a literal (bound value),
    # not a pattern variable. This allows ETS to use its hash index for O(1)
    # lookup rather than scanning the entire table.
    #
    # In match spec bodies, bare tuples are interpreted as operations/function
    # calls, NOT as literal data. We wrap ets_key, raw_key, and value with
    # ms_literal/1 so tuples use the {{...}} constructor form and maps use
    # {:const, map} that ETS understands. Version is restricted to
    # integer/atom/binary (match-spec literals; no wrapping needed).
    [
      {
        # Match by the ETS lookup key (literal — O(1) hash lookup)
        entry(
          key: ets_key,
          raw_key: :_,
          value: :_,
          version: :"$1",
          updates: :"$2"
        ),
        # Guard (update only if): new_version > existing_version
        [{:>, version, :"$1"}],
        # Result: the new entry with incremented updates counter
        [
          {entry(
             key: ms_literal(ets_key),
             raw_key: ms_literal(raw_key),
             value: ms_literal(value),
             version: version,
             updates: {:+, :"$2", 1}
           )}
        ]
      }
    ]
  end

  # Wraps a term so it is safe to use as a literal in a match spec body.
  # In match spec bodies, bare tuples are interpreted as operations — not
  # data. The {{...}} form tells ETS to construct a tuple from its elements.
  # Maps use {:const, map} to be treated as opaque literals.
  defp ms_literal(value) when is_tuple(value) do
    value
    |> Tuple.to_list()
    |> Enum.map(&ms_literal/1)
    |> List.to_tuple()
    |> then(&{&1})
  end

  defp ms_literal(value) when is_list(value) do
    Enum.map(value, &ms_literal/1)
  end

  defp ms_literal(value) when is_map(value) do
    {:const, value}
  end

  defp ms_literal(value) do
    value
  end
end
