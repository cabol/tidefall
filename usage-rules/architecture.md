# Tidefall Architecture

This guide explains why Tidefall exists, who uses it, how it is
structured internally, and the non-negotiable design principles
that govern every contribution. Read it at session start to
establish project context, and refer back to it when making
structural changes.

---

## Why Tidefall Exists

Elixir applications routinely accumulate large volumes of data
that should be processed in batches, not one item at a time:
telemetry spans, metric points, audit events, change events,
ingestion payloads, sync deltas. Sending every item individually
to a downstream system (HTTP API, database, message broker) is
slow and expensive; doing it ad-hoc per call site leads to
duplicated buffering logic and inconsistent backpressure.

Tidefall solves this with a **reusable, ETS-based buffer that
gathers data continuously and drains it on a periodic interval**.
It is inspired by the
[OpenTelemetry Batch Processor][otel-batch-processor], generalised
into a library that any Elixir application can drop in. The model:

- Producers write to the buffer with no awareness of batching.
- The buffer accumulates entries in ETS.
- A processor function (user-provided) is called at a configurable
  interval with a batch of accumulated entries.
- Partitioning reduces lock contention; double-buffering keeps
  writes flowing while processing runs.

[otel-batch-processor]: https://github.com/open-telemetry/opentelemetry-erlang/blob/main/apps/opentelemetry/src/otel_batch_processor.erl

---

## Who Uses It

Tidefall is for Elixir teams who need:

- **High-throughput buffered ingestion** — many writers, periodic
  bulk export to a downstream sink (HTTP, database, queue).
- **Zero-downtime processing** — writes must continue while a batch
  is being drained; no pauses, no lost data on shutdown.
- **Lock-free hot paths** — partitioning + ETS write concurrency
  let many producers write in parallel without contention.
- **Predictable batching** — fixed-interval flushes regardless of
  fill rate, with bounded batch size to control memory.

Typical use cases: telemetry/metric exporters, event aggregators,
audit logs, change-data-capture buffers, deduplicating state caches
that flush periodically.

---

## High-Level Architecture

```ascii
                          [Tidefall.Supervisor]   (application root)
                                  |
                +-----------------+-----------------+
                |                 |                 |
        [Tidefall.Metadata] [Tidefall.Registry]  per-buffer trees, each rooted at:
                                                    v
                                  [Tidefall.Buffer.Supervisor]
                                            |
                                +-----------+----------------------+
                                |                                  |
                       [Task.Supervisor]      [Tidefall.Buffer.Partition.Supervisor]
                                                                   |
                                                +------------------+----------------------+
                                                |                  |                      |
                                      [Tidefall.Buffer.Partition 0]   [...Partition 1]   ...  [...Partition N-1]
                                                |                  |                      |
                                          [ETS tid]         [ETS tid]              [ETS tid]
```

`Tidefall.Registry` is a single application-level `:duplicate`
Registry shared by all buffers. Each partition registers itself
under its buffer's atom as the key; lookups by buffer return all
partitions for that buffer. Sharing avoids one Registry GenServer
+ N internal ETS partitions per buffer (which adds up at scale).

Each partition owns an **unnamed** ETS table (`:ets.new/2` is
called without `:named_table`), identified only by its tid. The
current write target is recorded in `Tidefall.Metadata` keyed by
`{Tidefall.Buffer.Partition, :current_table, <partition>}`. On
each processing tick, the partition allocates a **fresh** ETS
table, points the metadata pointer at it, and hands the previous
table off to the processing task. There are no per-table atoms
to manage. See "Double-Buffering Processing Cycle" below for the
full lifecycle.

### Module Layout — `lib/tidefall/`

| Module | Responsibility |
|---|---|
| `Tidefall` | Project landing-page moduledoc only (overview, architecture, telemetry events). No functions |
| `Tidefall.Queue` | Insertion-ordered buffer (`:ordered_set` ETS) |
| `Tidefall.CoalescedMap` | Last-write-wins key/value buffer (`:set` ETS), with optional versioned updates |
| `Tidefall.Buffer` | Behaviour + buffer-operations API (`start_link/1`, `stop/3`, `buffer_size/1`, `update_options/2`, `get_partition/3`) |
| `Tidefall.Buffer.Partition` | GenServer running a single partition: double-buffering, timer, processing dispatch. ETS-agnostic |
| `Tidefall.Buffer.Partition.Supervisor` | Supervises N partition GenServers |
| `Tidefall.Buffer.Supervisor` | Per-buffer supervisor — wires `Task.Supervisor` and `Tidefall.Buffer.Partition.Supervisor` |
| `Tidefall.Supervisor` | Application root supervisor — owns `Tidefall.Metadata`, `Tidefall.Registry`, and any buffers started under the app |
| `Tidefall.Metadata` | Process-owned `:set` ETS table for shared metadata. Backs the partition current-table pointer (replaces what used to live in `:persistent_term`) |
| `Tidefall.Registry` | App-level `:duplicate` Registry shared across all buffers. Each partition registers under its buffer atom as the key |
| `Tidefall.Application` | OTP application entry point — boots `Tidefall.Supervisor` |
| `Tidefall.Buffer.Options` | `NimbleOptions` schemas — start, runtime, and updatable options |

### Buffer Implementation Pattern

A buffer type (`Queue`, `CoalescedMap`, future additions) is a thin module
that:

1. Implements the `Tidefall.Buffer` behaviour — `ets_table_opts/0`
   (the list passed verbatim to `:ets.new/2`) and
   `ets_match_spec/0` (the spec used by the processing task to
   drain the swapped table).
2. Defines its own `entry` record via `defrecordp` — the impl
   chooses the layout (Queue uses 2 fields; CoalescedMap uses 4 to support
   `put_newer` semantics).
3. Provides domain-specific write/read functions (`push/3`,
   `put/4`, `put_newer/5`, etc.) that locate the right partition
   via `Tidefall.Buffer.get_partition/3`, resolve the active table via
   `Tidefall.Buffer.Partition.current_table/1`, then issue
   `:ets.*` operations directly with the impl's record shape.
4. Delegates shared operations (`size`, `update_options`, `stop`)
   to `Tidefall`.

The partition itself is ETS-agnostic: it allocates tables (using
the impl's `ets_table_opts/0`), swaps them on the processing
tick, gives them away to the processing task, and drains them
via the impl's `ets_match_spec/0`. Impl-specific concerns like
conditional updates (`CoalescedMap.put_newer` and its `replace_match_spec`
helper) live entirely inside the impl module.

### Double-Buffering Processing Cycle

Each partition has one **current** ETS table (a tid) recorded in
`Tidefall.Metadata` (see "Module Layout" above). On each
processing tick:

1. The partition allocates a **fresh** ETS table and updates the
   metadata pointer; new writes flow to this new table from this
   moment on.
2. Ownership of the previous (now-idle) table is transferred to
   a `Task.Supervisor` task via `:ets.give_away/3`.
3. The task reads the table in batches using `:ets.select/3` with
   continuations and invokes the processor function.
4. **Batch mode**: after processing, the task deletes the table
   via `:ets.delete/1`. **`:table` mode**: the processor takes
   ownership of the table; when the task exits, ETS auto-deletes
   it unless the processor has already transferred it elsewhere
   via `:ets.give_away/3`.
5. The partition resets its in-flight bookkeeping and waits for
   the next tick.

This swap is the "Tidefall" — accumulated data falls into the
processor on every interval while new writes continue uninterrupted
on a fresh table.

---

## Key Design Decisions

### Double-buffering over locking

Two ETS tables per partition let writes continue while a batch is
being drained. The alternative — locking the table during read or
copying entries one-by-one — would either pause writers or burn
CPU on duplicate work. The handoff via `:ets.give_away/3` isolates
the read side from the write side at the OS level.

### `Tidefall.Metadata` for the current-table pointer

The current-table reference is read on **every** write. Storing
it in the GenServer state would force a `GenServer.call` per
write and serialise all producers through one process.

The original design used `:persistent_term`, which makes reads a
direct globally-shared atom lookup and (because the stored value
is always an atom) avoids global GC on update. The problem is on
the *write* side: `:persistent_term.put/2` duplicates the entire
persistent-term store on every write. With many buffers × many
partitions × every processing tick, the copy cost adds up.

The current implementation reads/writes the pointer through
`Tidefall.Metadata` — a process-owned `:public, :named_table,
:set` ETS table. Reads remain `O(1)` (an ETS `lookup`, slightly
slower than `:persistent_term.get/1` but well within the same
order of magnitude); writes are `O(1)` with `write_concurrency:
true` and no copy-on-write cost. The trade is a small read-path
cost in exchange for substantially better write-path scaling.

### Async processing via `Task.Supervisor.async_nolink/3`

The processing task runs supervised but unlinked from the
partition, so a processor exception cannot crash the partition.
The task is also given a `shutdown` timeout
(`processing_timeout`) so the system can guarantee bounded
shutdown time even when the processor hangs.

### `ets:select/3` with continuations for batching

`processing_batch_size` controls the size of each chunk passed to
the processor. Continuations let the partition stream large tables
to the processor without ever materialising the whole table in
process memory. A `:table` mode is also available: the entire ETS
table is handed to the processor as-is, for cases where the
processor wants total control over iteration (or wants to take
ownership of the table for asynchronous processing).

### Partition routing via `:erlang.phash2/2`

Entries are distributed across partitions using
`:erlang.phash2(key, n_partitions)`. The routing key defaults to
the message itself but can be customised via the `partition_key`
runtime option (function, MFA, or static value). This lets users
keep related entries together (e.g., same user → same partition
for ordering) or spread unrelated entries widely (for parallelism).

### NimbleOptions for all option validation

Every public option set is validated through
[NimbleOptions][nimble_options] schemas. This produces consistent,
actionable error messages at startup or option update, rather than
cryptic failures later.

### Telemetry as the observability contract

The library emits a fixed set of `[:tidefall, :partition, ...]`
Telemetry span events for partition lifecycle and processing.
These events are public API — see `tidefall.md` for the canonical
list. Adapter or user code may emit additional events but must
not redefine or suppress core events.

[nimble_options]: https://hexdocs.pm/nimble_options

---

## Non-Negotiables

These rules are not open for debate. Any contribution that
violates them will not be merged, regardless of other merits.

### 1. Never read or write a partition table directly

All access to a partition's tables MUST go through the
`Tidefall.Buffer.Partition` public API (`put/2`, `put_newer/2`, `get/3`,
`delete/2`, `buffer_size/1`). Internally these helpers resolve
the current write table via a private `get_current_table/1` —
never duplicate that lookup or cache the table atom in a buffer
module. The "current" table can swap at any time on the
processing tick; holding a stale reference is a correctness bug,
not a performance detail.

### 2. No breaking public API changes without a major version

`Tidefall`, `Tidefall.Queue`, `Tidefall.CoalescedMap`, telemetry event
names, and the documented option schemas are public API. Removing
or renaming public functions, changing option semantics, or
altering telemetry event shapes requires a major version bump.
Deprecation warnings must precede removals by at least one minor
release.

### 3. Every public function must have a `@doc` and typespec

Module documentation (`@moduledoc`) is required for every module
that isn't `@moduledoc false`. Public functions require `@doc` and
`@spec`. Undocumented public API will not be merged.

### 4. New behaviour must have tests

Any new feature, option, or code path must be accompanied by
tests. Cover both the success path and the relevant failure modes
(timeouts, processor exceptions, shutdown, concurrent writes
during processing).

### 5. `mix test.ci` must pass

All changes must pass the full CI suite locally before opening a
PR. See `workflow.md` (Validation Commands) for the exact contents
of the `test.ci` alias. Green CI on the PR is a requirement, not
a courtesy check.

### 6. Keep this document up to date

After any structural change — new module, new behaviour callback,
new public option, new dependency, or changes to the layer
boundaries — review this document and update it if needed.
Architecture docs rot when nobody owns them. This is not a
checkbox on every PR, but a conscious check: "did my change
affect the architecture described here?"

---

## Where to Look for Intent

When you have a question about *why* the code looks the way it
does, consult these sources in order. (For *rule precedence* —
i.e. which rule wins when rules conflict — see `AGENTS.md`.)

1. **This document** — architectural decisions and
   non-negotiables.
2. **`usage-rules/tidefall.md`** — patterns and pitfalls at the
   code level.
3. **Module `@moduledoc` and function `@doc`** — local intent per
   API.
4. **`CHANGELOG.md`** — history of decisions and breaking
   changes.
