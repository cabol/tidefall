# Tidefall Design Decisions and Non-Negotiables

The code and its moduledocs teach the structure (supervision
tree, double-buffering cycle, module layout) — read them. This
file records only what the code cannot teach: the *why* behind
deliberate decisions, and the rules that are not open for
debate. Read it before structural changes, and before proposing
any "optimization" of the hot paths.

Tidefall is inspired by the [OpenTelemetry Batch
Processor][otel-batch-processor], generalised into a library.

[otel-batch-processor]: https://github.com/open-telemetry/opentelemetry-erlang/blob/main/apps/opentelemetry/src/otel_batch_processor.erl

## Design Decisions

### Storage is ETS, deliberately

The value proposition is a set of ETS primitives, not "periodic
batch processing": lock-free concurrent writes from producer
processes (`:public` + `write_concurrency`), ownership isolation
via `:ets.give_away/3`, zero-copy table swap, and atomic
match-spec CAS (`insert_new`/`select_replace` backing
`put_newer`). None of these has a portable equivalent — a
storage-agnostic callback layer would either keep taking match
specs (a fake abstraction) or serialize writes through a process
(destroying the core property). Do not abstract the storage
behind callbacks. The sanctioned seam for "other backends" is
the processor (forward batches anywhere) plus `:table` mode; if
in-engine durability is ever needed, the path is an opt-in WAL
alongside ETS (a deliberate delivery-guarantee change, major
version), not a backend swap. Reopen only on a real user need
that (a) fits the allocate/swap/give-away model and (b) cannot
be met by a processor forwarding to their store.

### Double-buffering over locking

Two ETS tables per partition let writes continue while a batch
drains; `:ets.give_away/3` isolates the read side from writers.
The alternatives (locking or copying) pause writers or burn CPU.

### `Tidefall.Metadata` over `:persistent_term`

The current-table pointer is read on every write — the hottest
read in the system. The original design used `:persistent_term`;
it was replaced deliberately, not by oversight. The problem is
the *write* side: `:persistent_term.put/2` duplicates the entire
persistent-term store on every write, and the pointer is
rewritten on every non-empty processing tick × every partition ×
every buffer. `Tidefall.Metadata` (tuned ETS) keeps reads `O(1)`
within the same order of magnitude and makes writes `O(1)` with
no copy-on-write cost. Do not move the pointer back to
`:persistent_term`, and do not route it through a
`GenServer.call` (single-process bottleneck).

### At-most-once delivery, no retries

Processing runs in a `Task.Supervisor.async_nolink/3` task —
supervised, unlinked — so a processor exception or timeout can
never crash the partition. The cost is explicit: **when the
processor crashes or exceeds `processing_timeout`, the in-flight
batch is lost**. This is the intended delivery guarantee
(at-most-once), not a gap. Do not add retry, re-insertion, or
dead-lettering to the engine — durability beyond at-most-once is
the processor's responsibility. The sanctioned escape hatch for
users who need stronger guarantees: `processing_batch_size:
:table`, where the processor takes table ownership and can
`:ets.give_away/3` it to a durable handler before doing risky
work.

### Empty ticks are no-ops

A tick on an empty table does nothing: no swap, no task, no
telemetry span. This is a deliberate optimization (idle buffers
cost nothing), not an oversight — don't add "always flush" or
size-0 spans.

### Synchronous drain on shutdown

The partition traps exits in `init/1` so `terminate/2` runs on
shutdown, and `terminate/2` drains leftovers by calling
`process_batch/4` directly — the only synchronous processing
path (no give-away, no task). Preserve this when extending
shutdown behaviour.

### Partition routing via `:erlang.phash2/2`

Routing is `:erlang.phash2(key, n_partitions)`; the
`partition_key` runtime option (fun, MFA, or static value) lets
users co-locate related entries (ordering) or spread unrelated
ones (parallelism).

## Non-Negotiables

### 1. Never read or write a partition table directly

The only sanctioned access to a partition's active write table
is `Tidefall.Buffer.Partition.current_table/1`, resolved fresh
inside each operation. Never cache the returned tid — in a
struct, another process, or across a pipeline. The table swaps
on every processing tick; a stale reference is a correctness
bug, not a performance detail.

### 2. No breaking public API changes without a major version

`Tidefall`, `Tidefall.Queue`, `Tidefall.HashMap`,
**telemetry event names/shapes**, and the documented option
schemas are public API — telemetry metadata keys included.
Removing or renaming any of these, or changing option semantics,
requires a major version bump; deprecation warnings must precede
removals by at least one minor release. This applies from 1.0.0
on — `-dev` status does not relax it.

### 3. Documentation contract

Public functions require `@doc` and `@spec`; public modules
require `@moduledoc`. Internal modules are `@moduledoc false`
and are documented in `usage-rules/` instead of hexdocs. After a
structural change, check whether this file and
`usage-rules/tidefall.md` still describe reality — stale docs
are bugs.
