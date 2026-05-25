# Tidefall Project-Specific Usage Rules

This document covers the domain-specific patterns, primitives, and
pitfalls for working in the Tidefall codebase. The `API Surface`
section below is the load-bearing primitive list — read it before
writing any buffer module.

## API Surface

Three surfaces matter when working in this codebase:

1. **`Tidefall`** — top-level public helpers (start/stop/lookup/size/options).
2. **`Tidefall.Buffer`** — the behaviour every impl implements (two callbacks).
3. **`Tidefall.Buffer.Partition`** — the engine's small public API impls call into to reach the active write table.

Impl modules (`Tidefall.Queue`, `Tidefall.CoalescedMap`, …) own their own
entry-record layout, all `:ets.*` calls (insert, lookup, delete,
insert_new, select_replace), and any impl-specific helpers like
CoalescedMap's `replace_match_spec/3` for `put_newer`. The partition stays
ETS-agnostic — it allocates tables, swaps them on the processing
tick, and drains them via the impl's `ets_match_spec/0`.

### `Tidefall` (top-level)

```elixir
# Start a buffer. The caller must pass `:module` in opts;
# implementations (Queue/CoalescedMap/...) inject it before delegating.
@spec start_link(opts :: keyword()) :: Supervisor.on_start()
Tidefall.Buffer.start_link(opts)

# Stop a buffer gracefully. Drains any in-flight entries
# synchronously via the partition's `terminate/2`.
@spec stop(buffer :: atom() | pid(), reason :: any(), timeout()) :: :ok
Tidefall.Buffer.stop(buffer, reason \\ :normal, timeout \\ :infinity)

# Locate the partition (an atom) responsible for a routing key.
# Used by buffer modules to route writes. The `partition_key`
# arg may be nil, a fun/1, an MFA, or a static term (see
# Runtime Options).
@spec get_partition(buffer :: atom(), partition_key :: any(), object :: any()) :: atom()
Tidefall.Buffer.get_partition(buffer, partition_key, object)

# Total entries across all partitions of a buffer.
@spec buffer_size(buffer :: atom()) :: non_neg_integer()
Tidefall.Buffer.buffer_size(buffer)

# Mutate updatable options (processing_interval,
# processing_timeout, processing_batch_size) at runtime.
@spec update_options(buffer :: atom(), keyword()) :: :ok
Tidefall.Buffer.update_options(buffer, opts)
```

### `Tidefall.Buffer` (behaviour)

Every buffer module implements two callbacks:

```elixir
# Options list passed verbatim to :ets.new/2 when the partition
# allocates one of its two backing tables. Must include the ETS
# table type, :keypos, and any concurrency knobs the impl wants.
@callback ets_table_opts() :: [atom() | {atom(), any()}]

# Match spec used by the processing task when it drains the
# swapped table via :ets.select/3. Determines the shape of each
# element handed to the processor.
@callback ets_match_spec() :: :ets.match_spec()
```

The impl owns the rest — entry record, all data-shape `:ets.*`
calls, any specialized match specs. The partition only knows
how to allocate, swap, drain, and tear down.

### `Tidefall.Buffer.Partition`

```elixir
@spec start_link(opts :: keyword()) :: GenServer.on_start()
Tidefall.Buffer.Partition.start_link(opts)

# The only sanctioned access to the active write table. Impls
# call this and then issue their own :ets.* operations.
@spec current_table(partition :: atom()) :: atom()
Tidefall.Buffer.Partition.current_table(partition)

# Total entries currently in the partition's write table (does
# NOT include entries in flight in the processing task).
@spec buffer_size(partition :: atom()) :: non_neg_integer()
Tidefall.Buffer.Partition.buffer_size(partition)

# Mutate updatable options (processing_interval,
# processing_timeout, processing_batch_size).
@spec update_options(GenServer.server(), keyword()) :: :ok
Tidefall.Buffer.Partition.update_options(server, opts)
```

> **Do not cache `current_table/1`'s return value.** The pointer
> swaps on every processing tick; holding it across calls is a
> correctness bug. Resolve it fresh inside each operation.

Notes:

- `partition` is the atom name of a specific partition, obtained
  via `Tidefall.Buffer.get_partition(buffer, partition_key, routing_key)`.
- `buffer_size/1` reads from the *write* table only — it doesn't
  count entries in flight in a handed-off table.

### `Tidefall.Metadata` (shared metadata)

`Tidefall.Metadata` is `@moduledoc false` — internal-only, no
hexdocs surface. It's a `GenServer` owning a single
`:named_table, :public, :set` ETS table, used to share
low-cardinality state across the buffer subsystem. Today it
backs the partition current-table pointer (key
`{Tidefall.Buffer.Partition, :current_table, <partition>}`), which
previously lived in `:persistent_term`.

It exists because `:persistent_term.put/2` duplicates the whole
persistent-term store on every write. With many buffers ×
many partitions × every processing tick, that copy adds up
quickly. ETS writes are `O(1)` and contention-friendly with
`write_concurrency: true`.

```elixir
# Default-named instance is started by `Tidefall.Application`
# and registered as `Tidefall.Metadata`.
Tidefall.Metadata.put(:my_key, "value")     # → :ok
Tidefall.Metadata.get(:my_key)              # → "value"
Tidefall.Metadata.delete(:my_key)           # → :ok
Tidefall.Metadata.get(:my_key)              # raises RuntimeError

# Custom-named instance (for tests):
{:ok, _pid} = Tidefall.Metadata.start_link(name: :test_meta)
Tidefall.Metadata.put(:test_meta, :k, :v)
Tidefall.Metadata.get(:test_meta, :k)       # → :v
```

Notes:

- The first argument to `put/get/delete` defaults to
  `Tidefall.Metadata`. Production callers omit it; tests pass an
  explicit table to isolate state.
- `get/2` raises a `RuntimeError` when the key has no entry.
  When the named table doesn't exist at all (server not
  running, wrong name), `:ets.lookup/2` raises its native
  `ArgumentError` — we let it propagate.
- Writes go straight to ETS (`:public` table); no `GenServer.call`
  round-trip, no process bottleneck. The GenServer is only the
  table owner — when it dies, the table dies with it.

## Adding a New Buffer Type

Buffer types are thin shims on top of `Tidefall.Buffer.Partition`.
The example below is a **Queue-shaped template** (insertion-
ordered via a monotonic unique key) using the placeholder name
`MyApp.MyBuffer`.

### Before you copy the skeleton

> **Read these first** — they're easy to miss and silently wrong
> if you copy the skeleton as-is for a non-Queue buffer.

- **Entry record**: the impl owns its `defrecordp :entry, ...`.
  Pick the fields you need. Queue uses 2 (`key, value`); CoalescedMap
  uses 5 (`key, raw_key, value, version, updates`) to support
  `put_newer` semantics.
- **`ets_table_opts/0`**: returns the full list passed to
  `:ets.new/2`. Include the type, `:keypos` (use `entry(:key) + 1`
  for the canonical layout), and any concurrency knobs.
- **`ets_match_spec/0`**: returns the spec used to drain the
  swapped table. Determines the per-entry shape your processor
  receives. Independent decision; no cross-impl coupling.
- **Key strategy** (see the dedicated subsection after the
  skeleton): the example uses `unique_key/0` for insertion-order
  semantics. CoalescedMap-style needs the caller's key; Set-style needs
  the item-as-key.
- **Public function names**: see **Buffer Module API
  Conventions** below.

```elixir
defmodule MyApp.MyBuffer do
  @moduledoc "..."

  @behaviour Tidefall.Buffer

  import Record, only: [defrecordp: 2]

  alias Tidefall.Buffer.{Options, Partition}

  # Your impl owns its entry-record shape.
  defrecordp(:entry, key: nil, value: nil)

  @type buffer() :: atom()
  @type item() :: any()

  ## API

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts \\ []) do
    opts
    |> Keyword.put(:module, __MODULE__)
    |> Tidefall.Buffer.start_link()
  end

  @spec stop(buffer() | pid(), reason :: any(), timeout()) :: :ok
  defdelegate stop(buffer, reason \\ :normal, timeout \\ :infinity),
    to: Tidefall.Buffer

  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    %{
      id: opts[:name] || __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      type: :supervisor
    }
  end

  # Domain write/read functions — adapt to your semantics.
  # Resolve the partition, then the current write table, then
  # call :ets directly. Do not cache the table atom across calls.
  @spec push(buffer(), item(), keyword()) :: :ok
  def push(buffer, item, opts \\ []) do
    opts = Options.validate_runtime_options!(opts)
    partition_key = Keyword.fetch!(opts, :partition_key)
    partition = Tidefall.Buffer.get_partition(buffer, partition_key, item)

    true =
      partition
      |> Partition.current_table()
      |> :ets.insert(new_entry(unique_key(), item))

    :ok
  end

  @spec size(buffer()) :: non_neg_integer()
  defdelegate size(buffer), to: Tidefall.Buffer, as: :buffer_size

  @spec update_options(buffer(), keyword()) :: :ok
  defdelegate update_options(buffer, opts), to: Tidefall.Buffer

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
    [{entry(key: :"$1", value: :"$2"), [true], [:"$2"]}]
  end

  ## Private

  defp new_entry(key, value), do: entry(key: key, value: value)

  defp unique_key, do: {System.monotonic_time(), make_ref()}
end
```

### Required steps

1. Define your `entry` record with `defrecordp`. Shape is your
   choice — Queue uses 2 fields, CoalescedMap uses 4 (with
   `version`/`updates` for `put_newer`).
2. Implement `@behaviour Tidefall.Buffer` — return the ETS
   options from `ets_table_opts/0` and the processor batch shape
   from `ets_match_spec/0`. Both are independent decisions you
   own; no cross-impl coupling.
3. Implement `start_link/1` (set `:module` then delegate to
   `Tidefall.Buffer.start_link/1`), `stop/3`, and `child_spec/1`
   exactly as in the skeleton.
4. Implement your domain write/read functions:
   - Resolve the partition via `Tidefall.Buffer.get_partition/3`.
   - Resolve the table via `Partition.current_table/1`.
   - Call `:ets.*` directly with your entry-record shape.
5. `defdelegate` shared ops (`size` → `buffer_size`,
   `update_options`, `stop`) to `Tidefall`.
6. Add tests mirroring `test/tidefall/queue_test.exs` or
   `test/tidefall/map_test.exs` — same describe blocks, same
   telemetry assertions, parameterized on your buffer module.

### Key strategy

`Partition.new_entry/3`'s first argument is the entry's key — and
its **uniqueness semantics drive what your buffer can do**.
Choose deliberately:

- **Insertion-ordered, every write distinct** (Queue-style):
  use a monotonic + ref key, e.g.
  `{System.monotonic_time(), make_ref()}`. The example above
  uses this pattern via `unique_key/0`. Pairs with
  `ets_type: :ordered_set`.
- **Last-write-wins by user key** (CoalescedMap-style): use the
  caller-supplied key. Each new write with the same key
  overwrites the previous value. Pairs with `ets_type: :set`.
- **Dedup by item** (Set-style): use the item itself as both
  key and value (`Partition.new_entry(item, item)`). Duplicates
  collapse naturally. Pairs with `ets_type: :set`.
- **Multi-value per key** (Bag-style): pair the caller's key
  with a per-write `make_ref()` so the same key can coexist
  multiple times. Pairs with `ets_type: :bag` or
  `:duplicate_bag`.

Picking the wrong key strategy is a silent bug — the buffer will
"work" but with the wrong semantics. The skeleton above is
Queue-style; **change the key when copying it**.

### Buffer Module API Conventions

Buffer modules follow shared conventions for their public API.
Use existing buffers (`Tidefall.Queue`, `Tidefall.CoalescedMap`) as the
canonical reference; the patterns below are what users expect.

**Mandatory (every buffer module must expose):**

| Function | Purpose |
|---|---|
| `start_link/1` | Inject `:module`, delegate to `Tidefall.Buffer.start_link/1`. |
| `stop/3` | `defdelegate` to `Tidefall.Buffer.stop/3`. |
| `child_spec/1` | Standard `:supervisor` child spec (see skeleton). |
| `size/1` | `defdelegate` to `Tidefall.Buffer.buffer_size/1`. |
| `update_options/2` | `defdelegate` to `Tidefall.Buffer.update_options/2`. |

**Domain functions (name to match semantics):**

| Pattern | Example | Notes |
|---|---|---|
| Single-write | `Queue.push/3`, `CoalescedMap.put/4` | Always accepts a trailing `opts` for `:partition_key`. |
| Bulk-write | `CoalescedMap.put_all/3`, `Queue.push/3` (list-or-scalar) | Either `_all` suffix, or a single-function that accepts a list. Pick one and stick to it. |
| Conditional | `CoalescedMap.put_newer/5`, `CoalescedMap.put_all_newer/3` | Versioned writes when there's a notion of "newer wins." |
| Read | `CoalescedMap.get/3`, future `Set.member?/3` | Use `?` suffix for booleans; `_/2` for value-returning reads. |
| Delete | `CoalescedMap.delete/3` | Trailing `opts` for `:partition_key`. |

**Naming guidance:**

- Match Elixir stdlib idioms when in doubt: `MapSet.put` →
  `add`, `List.member?` → `member?`, `CoalescedMap.get` → `get`, etc.
- Don't reuse a name across buffer types if the semantics differ
  (e.g. `put` means last-write-wins in CoalescedMap; using it in Set
  would be misleading).
- Always accept a trailing `opts :: keyword()` — even if empty
  today, it's where `:partition_key` and future runtime options
  go.

## Processor Conventions

### Contract

A processor is a function (or MFA tuple) invoked by the partition
on every processing tick with the accumulated batch. Key points:

- **Return value.** The return value is discarded — the
  processor runs for side effects. In `:table` mode the
  processor takes ownership of the handed-off ETS table: the
  buffer does not delete it, but the processing task does own
  it, so it is auto-deleted when the task exits unless the
  processor hands it off elsewhere via `:ets.give_away/3`. See
  `:table` Mode below.
- **Process model.** The processor runs in a `Task.Supervisor`
  task spawned via `Task.Supervisor.async_nolink/3`, not in the
  partition process. It is unlinked from the partition.
- **Exceptions.** If the processor raises, the task crashes but
  the partition is not affected. Telemetry emits
  `[:tidefall, :partition, :processing, :exception]`
  (with `kind`, `reason`, `stacktrace`) and then
  `[:tidefall, :partition, :processing_failed]` (with `reason`).
  The partition recreates the swapped table and continues
  normally on the next tick. **The buffered batch in the swapped
  table is lost** — the processor must be responsible for any
  durability guarantees (retry, dead-letter, etc.).
- **Timeout.** If the processor doesn't return within
  `processing_timeout`, the task is forcefully shut down. The
  same "batch is lost" caveat applies.
- **Backpressure.** If a tick fires while a previous processing
  task is still running, the next tick is postponed (the
  partition tracks `processing?: true`). Writes continue
  uninterrupted on the active table.

### Queue Processor

The processor receives a flat list of values:

```elixir
fn batch ->
  # batch is [value1, value2, ...]
  Enum.each(batch, &process/1)
end
```

### CoalescedMap Processor

The processor receives a list of `t:Tidefall.CoalescedMap.Entry.t/0`
structs:

```elixir
fn batch ->
  # batch is [%Tidefall.CoalescedMap.Entry{key, value, version, updates}, ...]
  Enum.each(batch, fn %Tidefall.CoalescedMap.Entry{key: k, value: v} ->
    process(k, v)
  end)
end
```

- `:version` — set via `put_newer/4` / `put_all_newer/3`;
  `0` for regular `put/4` entries.
- `:updates` — number of times an existing key was conditionally
  updated; `0` for regular `put/4` entries.
- `:key` — always the **original** key, even when `:key_hasher`
  was used (the hash is an internal storage detail).

### `:table` Mode

When `processing_batch_size: :table` is set, the processor
receives the ETS **table identifier** (a tid) instead of a batch
of entries:

```elixir
fn table ->
  # full control over how to read the table
  :ets.foldl(..., :start_acc, table)
end
```

- The **processor takes ownership** of the table. The buffer
  itself does NOT delete it.
- The table is owned by the short-lived `Task.Supervisor` task
  running the processor. When that task exits (after the
  processor returns), ETS auto-deletes the table unless the
  processor has already transferred it elsewhere.
- To **keep the table** for later async processing, call
  `:ets.give_away(table, other_pid, gift)` before returning. The
  receiving process becomes the owner; the table survives the
  task exit.

### MFA Processors

The processor also accepts `{Module, Function, Args}` tuples. The
batch (or table name) is **prepended** to the arguments:

```elixir
processor: {MyModule, :process, [extra_arg]}
# calls MyModule.process(batch, extra_arg)
```

## Options and Validation

- All options are validated via `NimbleOptions` schemas in
  `Tidefall.Buffer.Options`.
- **Start options**: `:name`, `:processor`, `:partitions`,
  `:processing_interval`, `:processing_timeout`,
  `:processing_batch_size`, `:module` (auto-set by buffer
  implementations).
- **Runtime options** (passed to write functions): `:partition_key`.
- **Updatable options** (via `Tidefall.Buffer.update_options/2`):
  `:processing_interval`, `:processing_timeout`,
  `:processing_batch_size`.
- Validate as early as possible. The schemas raise
  `NimbleOptions.ValidationError` at startup or update time.
- Options documented via `NimbleOptions` should be inserted into
  module docs using helper functions —
  `#{Tidefall.Buffer.Options.start_options_docs()}`,
  `#{Tidefall.Buffer.Options.runtime_options_docs()}`,
  `#{Tidefall.Buffer.Options.updatable_options_docs()}`. Do not duplicate
  the docs inline.

### Adding a New Option

Options live in `lib/tidefall/buffer/options.ex` in one of four schemas.
The schema names below are the actual module-attribute / local
keyword-list variable names in that file — find them and add
your entry to the matching list.

| Schema | Lifecycle | When it applies |
|---|---|---|
| `start_opts` | Compile-time | Passed to `start_link/1`. |
| `runtime_opts` | Per-call | Passed to write functions (e.g. `push/3`). |
| `updatable_opts` | Live | Mutable via `Tidefall.Buffer.update_options/2`; auto-merged into `start_opts`. |
| `auto_opts` | Internal | Set by buffer implementations (e.g. `:module`). |

To add an option:

1. Add an entry (with `:type`, `:required`, `:default`, `:doc`) to
   the relevant keyword list in `Tidefall.Buffer.Options`.
2. If it's both startup-time and updatable, add it to
   `updatable_opts` only — `start_opts` includes
   `updatable_opts` via concatenation in `Tidefall.Buffer.Options`,
   roughly:
   ```elixir
   start_opts = [...] ++ updatable_opts
   ```
   Adding the same key to both lists raises a NimbleOptions
   duplicate-key error at startup.
3. Consume it from the relevant module via
   `Keyword.fetch!(opts, :your_option)` or via the
   `Tidefall.Buffer.Partition` state struct (don't forget the
   `defstruct` field).
4. Docs flow automatically. `Tidefall.Buffer.Options.start_options_docs/0`
   / `runtime_options_docs/0` / `updatable_options_docs/0`
   render the schema (including your new entry's `:doc` text)
   and are interpolated into the consuming module's
   `@moduledoc` (see `Tidefall.Queue` / `Tidefall.CoalescedMap` for the
   pattern). **Do not paste the option's `:doc` text inline** —
   let the helpers inject it, otherwise the docs drift.
   Modules and functions themselves still need their own
   `@moduledoc` / `@doc` / `@spec` per `architecture.md`
   Non-Negotiable #3 — this rule is only about the option's
   description text, not about documentation in general.

## ETS Match Spec Safety

When building ETS match specs (especially for `select_replace`):

- **Bare tuples in match-spec bodies are operations, not literal
  data.** To use a tuple as a literal value, wrap it with the
  `ms_literal/1` helper (lives in `Tidefall.CoalescedMap` today as it's
  only needed by `put_newer`'s conditional update; copy or move
  it as needed when adding new conditional-update buffers) — it
  emits the `{{...}}` constructor form (and `{:const, map}` for
  maps) that ETS expects.
- **Use literal (bound) keys in match heads** for O(1) lookup
  instead of pattern variables, which cause full-table scans.
- Maps with embedded tuples have known limitations in ETS
  `select_replace`. If you hit one, fall back to two-step
  `insert_new` + retry instead of forcing a single match spec.

## Telemetry

All telemetry events use the prefix `[:tidefall, :partition]`.
Event names and shapes are public API; changing them requires a
major version bump (see `architecture.md` Non-Negotiable #2).

> **Mirror.** This table is mirrored in the `Tidefall` module
> `@moduledoc` (the user-facing source rendered to hexdocs). The
> `@moduledoc` is the canonical home — when adding a new event,
> emit it from `Tidefall.Buffer.Partition`, update the `@moduledoc`,
> then update this table. Drift between the two is a bug.

| Event | When | Measurement | Metadata |
|-------|------|-------------|----------|
| `[:tidefall, :partition, :start]` | Partition starts | `system_time` | `buffer`, `partition` |
| `[:tidefall, :partition, :stop]` | Partition terminates | `duration` | `buffer`, `partition`, `reason` |
| `[:tidefall, :partition, :processing, :start]` | Batch processing begins | `system_time` | `buffer`, `partition` |
| `[:tidefall, :partition, :processing, :stop]` | Batch processing completes | `duration`, `size` | `buffer`, `partition` |
| `[:tidefall, :partition, :processing, :exception]` | Exception during processing | `duration` | `buffer`, `partition`, `kind`, `reason`, `stacktrace` |
| `[:tidefall, :partition, :processing_failed]` | Processing task `:DOWN` (timeout or crash) | `system_time` | `buffer`, `partition`, `reason` |

The `:processing, :start` / `:stop` / `:exception` triplet is
driven by `:telemetry.span/3` in the processing task.
`:processing_failed` fires when the task goes `:DOWN` without
producing an exception span (e.g. forced shutdown on timeout).

## Testing Patterns

- Cover both `Queue` and `CoalescedMap` for shared concerns; cover each one
  separately for type-specific behaviour.
- Use short `processing_interval` (e.g., 100 ms) in tests to
  keep the suite fast.
- For deterministic assertions, use a processor that sends
  messages to the test process:
  `fn batch -> send(pid, {:batch, batch}) end`, then
  `assert_receive {:batch, _}`.
- Cover the edge cases: empty buffers, single-partition setups,
  concurrent writes during processing, graceful shutdown
  processing on `terminate/2`.
- For versioned updates (`put_newer`), test ordering guarantees
  explicitly: newer overwrites older, older is ignored, equal is
  ignored.
- Test with tuple and list keys/values to exercise the
  `ms_literal/1` path.
- Test the `:table` processor mode separately from batch mode —
  the table-ownership lifecycle is different.

## Common Pitfalls to Avoid

These are actionable "do NOT" warnings for code review and
implementation. The architectural reasoning behind them lives in
`architecture.md` (Key Design Decisions and Non-Negotiables);
this list intentionally restates them in their imperative form.

- **Do NOT** resolve a partition's active table by any path
  other than `Tidefall.Buffer.Partition.current_table/1`. That's
  the single sanctioned lookup; the pointer swaps on every
  processing tick, so caching it across calls (in a struct,
  another process, a long pipeline) is a correctness bug.
  (Architecture: Non-Negotiable #1.)
- **Do NOT** use bare tuples in ETS match-spec bodies without
  wrapping them via `ms_literal/1`.
- **Do NOT** delete an ETS table outside the processing task. The
  task owns the handed-off table and deletes it when done.
- **Do NOT** route the partition current-table pointer through
  anything other than `Tidefall.Metadata`. The pointer is the
  hottest read in the system; redirecting it through a
  `GenServer.call` or duplicating it into `:persistent_term`
  reintroduces either a single-process bottleneck or the
  copy-on-write cost we left behind.
  (Architecture: Key Design Decision — `Tidefall.Metadata` for the
  current-table pointer.)
- **Do NOT** rely on the `:processing?` flag from outside the
  partition. The partition manages it; producers don't observe it.
- **Do NOT** assume the processor runs in the partition process.
  It runs in a `Task.Supervisor` task and is unlinked from the
  partition. (See "Processor Conventions → Contract" above.)
- **Do NOT** forget to handle the long-tail case in
  `terminate/2`: any leftover entries in the current table must
  be drained synchronously before the partition dies. The
  partition's `terminate/2` calls `process_batch/4` directly
  (synchronous, no task hand-off); preserve that pattern when
  extending shutdown behaviour.
