# Tidefall Project-Specific Usage Rules

Code-level patterns for working in this codebase. Read this
before writing or modifying a buffer impl, processor, option,
match spec, telemetry event, or their tests. Architectural
rationale and non-negotiables live in `architecture.md`; the
canonical API reference is hexdocs (`@moduledoc`/`@doc` in the
code) — this file does not duplicate typespecs.

## API Surface

Three surfaces matter:

1. **`Tidefall.Buffer`** — buffer-operations API
   (`start_link/1`, `stop/3`, `get_partition/3`,
   `buffer_size/1`, `update_options/2`) and the behaviour every
   impl implements: `ets_table_opts/0` (the list passed verbatim
   to `:ets.new/2` — must include the table type, `:keypos`, and
   concurrency knobs) and `ets_match_spec/0` (drains the swapped
   table; determines the per-entry shape the processor receives).
2. **`Tidefall.Buffer.Partition`** — the engine's public API:
   `start_link/1`, `current_table/1`, `buffer_size/1`,
   `update_options/2`. `current_table/1` is the only sanctioned
   table access (Non-Negotiable #1: resolve it fresh inside each
   operation, never cache it). `buffer_size/1` counts the *write*
   table only — not entries in flight in a handed-off table.
3. **Impl modules** (`Tidefall.Queue`, `Tidefall.HashMap`,
   …) — own their entry-record layout, all `:ets.*` calls, and
   impl-specific helpers (e.g. HashMap's
   `replace_match_spec/4` for `put_newer`).

### `Tidefall.Metadata`

Internal (`@moduledoc false`) GenServer owning a single
`:named_table, :public, :set` ETS table for low-cardinality
shared state — today, the partition current-table pointer. The
GenServer is only the table owner (the table dies with it);
reads/writes go straight to ETS, no call round-trip. `get/2`
raises `RuntimeError` on a missing key; a missing *table*
propagates ETS's native `ArgumentError`. The first argument
defaults to `Tidefall.Metadata`; tests pass an explicit name
(`start_link(name: :test_meta)`) to isolate state.

## Adding a New Buffer Type

Buffer types are thin shims on top of
`Tidefall.Buffer.Partition`. The skeleton below is
**Queue-shaped** (insertion-ordered via a monotonic unique key) —
change the entry record and key strategy when copying it for
other semantics.

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
  # call :ets directly. Do not cache the table across calls.
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

Required steps beyond the skeleton:

1. Define your `entry` record with `defrecordp` — the shape is
   yours. Queue uses 2 fields (`key, value`); HashMap uses 5
   (`key, raw_key, value, version, updates`) to support
   `put_newer` (`raw_key` stores the internal key when a
   `:key_hasher` is in play; the user-facing `Entry` struct
   exposes 4 of them — everything but `raw_key`).
2. `ets_table_opts/0` and `ets_match_spec/0` are independent
   decisions you own; no cross-impl coupling.
3. The skeleton's `push/3` is single-item; for list/batch
   writes, group items per partition before inserting — see
   `Tidefall.Queue.push/3` for the pattern.
4. Add tests mirroring `test/tidefall/queue_test.exs` /
   `test/tidefall/hash_map_test.exs` — same describe blocks, same
   telemetry assertions, parameterized on your buffer module.

### Key strategy

Each impl builds entries with its own private `new_entry/…`
helper (see the skeleton); the record's key field — and its
**uniqueness semantics** — drive what your buffer can do. Picking
the wrong strategy is a silent bug: the buffer "works" with the
wrong semantics.

- **Insertion-ordered, every write distinct** (Queue-style):
  monotonic + ref key, `{System.monotonic_time(), make_ref()}`.
  Pairs with `:ordered_set`.
- **Last-write-wins by user key** (HashMap-style): the
  caller-supplied key; same key overwrites. Pairs with `:set`.
- **Dedup by item** (Set-style): the item as both key and value;
  duplicates collapse. Pairs with `:set`. Match spec and
  processor batch shape are identical to Queue (a flat list of
  items).
- **Multi-value per key** (Bag-style): caller's key + per-write
  `make_ref()`. Pairs with `:bag` / `:duplicate_bag`.

### API conventions

Every buffer module exposes `start_link/1` (inject `:module`,
delegate), `stop/3`, `child_spec/1`, `size/1` (→ `buffer_size`),
and `update_options/2` — exactly as in the skeleton. Domain
functions are named to match semantics (don't reuse a name across
buffer types if semantics differ — `put` means last-write-wins in
HashMap), and always accept a trailing `opts :: keyword()` —
that's where `:partition_key` and future runtime options go.

## Definition Modules (`use`)

Each buffer type ships its own `__using__/1` (there is no single
`use Tidefall.Buffer` facade parameterised by buffer type). `use
Tidefall.Queue` / `use Tidefall.HashMap` generate a definition
module whose name is the default instance name. Shared codegen
lives in the internal `@moduledoc false` module
`Tidefall.Buffer.Definition` (documented here, not in hexdocs).

What the buffer type's `__using__/1` passes to
`Tidefall.Buffer.Definition.define/3`: its own module, a list of
op specs `{name, leading_params, min_optional, max_optional}`
derived from the **real buffer-type signatures** (not invented),
and the raw compile-time `use` opts. Add a tuple for every public
op you want callable on the definition module.

### Distinct-arity scheme — never default the leading name

For a buffer-type op `f(buffer, a, b, opts \\ [])` the generator emits
**nameless** variants that pre-bind `__MODULE__` and mirror the
buffer type's optional params (`f(a, b)`, `f(a, b, opts)`), plus **one
nameful** variant at the FULL arity with every param explicit and
opts **required** (`f(name, a, b, opts)`).

Do **not** "simplify" this into a single
`def f(name \\ __MODULE__, ..., opts \\ [])`. A defaulted leading
name plus a defaulted trailing opts silently misroutes: e.g.
`put(:k, "v", partition_key: 1)` binds `:k` as the *name* and
shifts everything right. Distinct arities make every call
unambiguous at compile time. This was empirically shown to break;
it is locked (see `architecture.md`). Generate distinct variable
names per param so a clause never binds two args to the same name.

### Config precedence

Resolved in `Tidefall.Buffer.Definition.resolve_opts/2` at
**runtime** inside `start_link`/`child_spec`, lowest → highest:

1. compile-time `use` opts (injected via `def __compile_opts__`,
   **not** a module attribute — captures like
   `processor: &Foo.run/1` must compile naturally, and
   `Macro.escape/1` would break them);
2. `Application.get_env(otp_app, __MODULE__, [])` — `:otp_app` is
   **required** in the `use` opts (`resolve_opts/2` calls
   `Keyword.pop!/2` and raises `KeyError` without it);
3. explicit `start_link`/child-spec opts.

Then `Keyword.put_new(:name, __MODULE__)` so an explicit name
always wins over the module-name default. The merged list flows
into the existing `Tidefall.Buffer.Options` validation unchanged —
do not touch that module for this.

`child_spec/1` derives `id` from the resolved name
(`opts[:name] || __MODULE__`) with `type: :supervisor`, so two
instances of one definition coexist in one tree; `child_spec: 1`
and `start_link: 1` are `defoverridable`. Generated functions are
all `@doc false`; document the pattern in each buffer type's
`@moduledoc`.

## Processor Contract

A processor is a function (or MFA) invoked on every processing
tick with the accumulated batch:

- **Return value is discarded** — the processor runs for side
  effects.
- **Process model**: runs in a `Task.Supervisor` task via
  `async_nolink/3` — NOT in the partition process, and unlinked
  from it.
- **Exceptions**: the task crashes; the partition is unaffected
  and continues on the next tick. **The buffered batch is lost**
  — the processor owns any durability guarantees (retry,
  dead-letter, …). Never add retry or re-insertion to the engine
  itself: at-most-once is the intended contract
  (`architecture.md` → "At-most-once delivery, no retries").
- **Timeout**: past `processing_timeout` the task is forcefully
  shut down; same batch-is-lost caveat.
- **Backpressure**: if a tick fires while processing is still
  running, the tick is postponed. Writes continue uninterrupted.
  The `:processing?` flag that tracks this is partition-internal
  — never rely on it from outside.

Batch shapes: Queue processors receive a flat list of values.
HashMap processors receive `Tidefall.HashMap.Entry`
structs — `:version` and `:updates` are `0` for plain `put/4`
entries, and `:key` is always the **original** key even when
`:key_hasher` was used (the hash is internal storage detail).

MFA processors: the batch (or table) is **prepended** to the
args — `processor: {MyModule, :process, [extra]}` calls
`MyModule.process(batch, extra)`.

### `:table` mode

With `processing_batch_size: :table` the processor receives the
ETS **tid** instead of a batch and **takes ownership**:

- The buffer does NOT delete the table. It is owned by the
  short-lived task; when the task exits, ETS auto-deletes it.
- To keep the table for later async processing, call
  `:ets.give_away(table, other_pid, gift)` before returning —
  the table survives the task exit. Never delete a handed-off
  table from outside the owning process.
- Test `:table` mode separately from batch mode — the ownership
  lifecycle is different.

## Options

All options are validated via `NimbleOptions` schemas in
`Tidefall.Buffer.Options`, raising `NimbleOptions.ValidationError`
at startup or update time. Four schemas: `start_opts` (passed to
`start_link/1`), `runtime_opts` (per write call, e.g.
`:partition_key`), `update_opts` (live via
`update_options/2`), `auto_opts` (internal, e.g. `:module`).

To add an option:

1. Add the entry (`:type`, `:required`, `:default`, `:doc`) to
   the matching keyword list in `Tidefall.Buffer.Options`.
2. If it's both startup-time and updatable, add it to
   `update_opts` **only** — `start_opts` already concatenates
   `update_opts`; adding the key to both lists raises a
   NimbleOptions duplicate-key error at startup.
3. Consume it via `Keyword.fetch!/2` or the
   `Tidefall.Buffer.Partition` state struct (add the `defstruct`
   field).
4. **Do not paste the option's `:doc` text inline** in
   moduledocs — the `Tidefall.Buffer.Options.*_options_docs/0`
   helpers render the schema into `@moduledoc` (see
   `Tidefall.Queue` for the pattern); inlining makes docs drift.

## ETS Match Spec Safety

When building match specs (especially for `select_replace`):

- **Bare tuples in match-spec bodies are operations, not literal
  data.** Wrap literal tuples with the `ms_literal/1` helper — it
  emits the `{{...}}` constructor form (and `{:const, map}` for
  maps) that ETS expects. It is currently a private helper in
  `Tidefall.HashMap` (only `put_newer` needs it); lift it to
  a shared internal module if a second buffer needs conditional
  updates.
- **Use literal (bound) keys in match heads** for O(1) lookup;
  pattern variables cause full-table scans.
- Maps with embedded tuples have known limitations in
  `select_replace`. If you hit one, fall back to two-step
  `insert_new` + retry instead of forcing a single match spec.

## Telemetry

All events use the prefix `[:tidefall, :partition]`. Event names
and shapes are public API (Non-Negotiable #2).

> **Canonical home**: the `Tidefall` `@moduledoc` (rendered to
> hexdocs). When adding an event: emit it from
> `Tidefall.Buffer.Partition`, update the `@moduledoc`, then this
> table. Drift between the two is a bug.

| Event | When | Measurement | Metadata |
|-------|------|-------------|----------|
| `[:tidefall, :partition, :start]` | Partition starts | `system_time` | `buffer`, `partition` |
| `[:tidefall, :partition, :stop]` | Partition terminates | `duration` | `buffer`, `partition`, `reason` |
| `[:tidefall, :partition, :processing, :start]` | Batch processing begins | `system_time` | `buffer`, `partition` |
| `[:tidefall, :partition, :processing, :stop]` | Batch processing completes | `duration`, `size` | `buffer`, `partition` |
| `[:tidefall, :partition, :processing, :exception]` | Exception during processing | `duration` | `buffer`, `partition`, `kind`, `reason`, `stacktrace` |
| `[:tidefall, :partition, :processing_failed]` | Processing task `:DOWN` | `system_time` | `buffer`, `partition`, `reason` |

The `:processing` triplet is driven by `:telemetry.span/3` inside
the processing task. **`:processing_failed` and
`:processing, :exception` are distinct paths**: a processor that
raises produces an `:exception` event (from the span) and then
`:processing_failed` (from the partition's `:DOWN` handler); a
**timeout** produces `:processing_failed` only — there is no
exception span on forced shutdown. Alerting must watch both.

Also note: an empty tick emits **no** events at all (the
empty-table guard skips the swap and the span entirely).

## Testing Patterns

- Parameterize shared-concern tests across `Queue` and
  `HashMap`; cover type-specific behaviour separately.
- For deterministic assertions, use a processor that sends to the
  test process: `fn batch -> send(pid, {:batch, batch}) end` +
  `assert_receive`; keep `processing_interval` short (~100 ms).
- For `put_newer`, assert all three orderings explicitly: newer
  overwrites, older is ignored, equal is ignored.
- Test with tuple and list keys/values to exercise the
  `ms_literal/1` path.
- Cover graceful shutdown: `terminate/2` drains the current table
  synchronously via `process_batch/4` (no task hand-off) — see
  `architecture.md` → "Synchronous drain on shutdown".
