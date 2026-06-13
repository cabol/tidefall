defmodule Tidefall do
  @moduledoc """
  ETS-backed buffer for Elixir — accumulate writes, drain in periodic batches.

  Tidefall accumulates data in partitioned ETS tables and drains it
  to a user-supplied processor function on a fixed interval. It is
  inspired by the
  [OpenTelemetry Batch Processor][otel_batch_processor], generalised
  as a reusable library.

  Concrete buffer types:

    * `Tidefall.Queue` — Insertion-ordered buffer (`:ordered_set` ETS).
    * `Tidefall.HashMap` — Coalescing key-value buffer (`:set` ETS;
      last-write-wins, optional version-aware conflict resolution).

  See `Tidefall.Buffer` for the buffer-operations API (start, stop,
  size, options, partition routing) and the behaviour callbacks
  every implementation must satisfy.

  [otel_batch_processor]: https://github.com/open-telemetry/opentelemetry-erlang/blob/main/apps/opentelemetry/src/otel_batch_processor.erl

  ## On this page

    * [Quick start](#module-quick-start) — start a buffer in a few lines
    * [Choosing a buffer type](#module-choosing-a-buffer-type) — Queue vs. HashMap
    * [Module-based buffers](#module-module-based-buffers-recommended) — the recommended pattern
    * [Direct usage](#module-direct-usage-quick-dynamic) — quick or fully dynamic instances
    * [Configuration](#module-configuration) — config file and supervision tree
    * [Architecture](#module-architecture) — supervision tree and partitions
    * [Application configuration](#module-application-configuration) — `:tidefall` app env
    * [Telemetry](#module-telemetry) — emitted events

  ## Quick start

  Start a buffer with a processor, push some items, and let the engine
  drain them on the next tick:

      # Start a queue buffer with a processor
      iex> {:ok, _pid} =
      ...>   Tidefall.Queue.start_link(
      ...>     name: :my_queue,
      ...>     processor: fn batch -> IO.inspect(batch, label: "batch") end
      ...>   )

      # Push items — single or in bulk
      iex> Tidefall.Queue.push(:my_queue, "event-1")
      :ok
      iex> Tidefall.Queue.push(:my_queue, ["event-2", "event-3"])
      :ok

  The processor runs in a task on each processing tick (default every
  five seconds) with the accumulated batch. The buffer is **drain-only**
  — producers write, the engine drains; there is no read-back queue.

  ## Choosing a buffer type

  Both buffer types share the same lifecycle, partitioning, and processor
  contract; they differ in **what survives to the next tick**.

    * **`Tidefall.Queue`** — every pushed item is buffered in insertion
      order and delivered to the processor. Reach for it when items are
      independent and all of them matter: event/log/metric forwarding,
      span export, batch writes to a sink.

    * **`Tidefall.HashMap`** — writes key into an entity, and same-key
      writes coalesce so only the latest value per key survives to the
      next tick. Reach for it when you care about the current state of a
      key, not every write: state snapshots, change deduplication,
      counters. Use `Tidefall.HashMap.put_newer/4` when conflict
      resolution must respect an explicit version (newer version wins).

  ## Module-based buffers (recommended)

  The recommended way to use a buffer is to define a dedicated module with
  `use Tidefall.Queue` or `use Tidefall.HashMap`. The module name becomes
  the default instance name, and start options are layered from
  compile-time `use` opts, the application environment, and explicit
  `start_link`/child-spec opts (in that order of increasing precedence):

      defmodule MyApp.EventQueue do
        use Tidefall.Queue, otp_app: :my_app
      end

      defmodule MyApp.StateMap do
        use Tidefall.HashMap, otp_app: :my_app
      end

  Add them to your supervision tree and call the generated functions on
  the default instance (named after the module):

      children = [MyApp.EventQueue, MyApp.StateMap]

      :ok = MyApp.EventQueue.push(event)
      :ok = MyApp.EventQueue.push(event, partition_key: 1)
      :ok = MyApp.StateMap.put(key, value)
      :ok = MyApp.StateMap.put_newer(key, value, version: v)

  The generated functions come in distinct arities: the nameless variants
  operate on the default instance, while a single full-arity variant takes
  the instance name as its first argument. To address a **dynamically
  started instance** of the same definition, use that full-arity form with
  every argument explicit (including the trailing options):

      {:ok, _} = MyApp.StateMap.start_link(name: :tenant_a)
      :ok = MyApp.StateMap.put(:tenant_a, key, value, [])

  ## Direct usage (quick / dynamic)

  For quick experiments or fully dynamic instances, the buffer types can
  be used directly with a runtime `:name` — no definition module required:

      {:ok, _pid} =
        Tidefall.Queue.start_link(
          name: :my_queue,
          processor: &MyApp.Sink.process/1
        )

      :ok = Tidefall.Queue.push(:my_queue, event)

  ## Configuration

  Start options can be supplied wherever a buffer is started. Two common
  setups:

  **Via the supervision tree** — pass options inline in the child spec.
  This works for both definition modules and direct usage:

      children = [
        # definition module (options layered over its `use`/app-env opts)
        {MyApp.EventQueue, processing_interval: 1_000},

        # direct usage (runtime name)
        {Tidefall.HashMap,
         name: :state_map,
         processor: &MyApp.StateProcessor.process_batch/1,
         partitions: 4}
      ]

      Supervisor.start_link(children, strategy: :one_for_one)

  **Via the application environment** — for definition modules with an
  `:otp_app`, options can live in `config/runtime.exs` and are read at
  start time:

      # config/runtime.exs
      import Config

      config :my_app, MyApp.StateMap,
        processor: &MyApp.Sink.process/1,
        partitions: 4

  > #### `:otp_app` is required for the config-file layer {: .warning}
  >
  > The application-environment layer is only consulted when the
  > definition module was declared with `use Tidefall.Queue,
  > otp_app: :my_app`. Without `:otp_app`, starting the buffer raises —
  > the env layer is not silently skipped. Direct usage (runtime `:name`)
  > does not read the application environment at all; pass its options
  > explicitly.

  See `Tidefall.Queue` and `Tidefall.HashMap` for the full list of start
  and runtime options.

  ## Architecture

  ```asciidoc
                          [Tidefall.Supervisor]   (application root)
                                  |
                +-----------------+-----------------+
                |                 |                 |
        [Tidefall.Metadata] [Tidefall.Registry]  per-buffer trees:
                                                    v
                                  [Tidefall.Buffer.Supervisor]
                                            |
                                +-----------+----------------------+
                                |                                  |
                       [Task.Supervisor]      [Tidefall.Buffer.Partition.Supervisor]
                                                                   |
                                                +------------------+----------------------+
                                                |                  |                      |
                                          [Partition 0]      [Partition 1]    ...     [Partition N-1]
                                                |                  |                      |
                                          [ETS tid]         [ETS tid]              [ETS tid]
  ```

  Each buffer write routes to a partition via `:erlang.phash2/2`, and each
  partition double-buffers its ETS table so processing swaps in a fresh
  table with zero downtime. Per-type data flow lives in the
  `Tidefall.Queue` and `Tidefall.HashMap` docs.

  ## Application configuration

  These options are read from the `:tidefall` application environment at
  startup (set them in `config/config.exs` or `config/runtime.exs`). They
  configure the library as a whole — distinct from the per-buffer start
  options above:

    * `:registry_partitions` (positive integer, default
      `System.schedulers_online()`) — number of internal ETS
      partitions for `Tidefall.Registry`, the shared registry
      used by all buffers to locate their partitions. Every
      buffer write performs one registry lookup, so contention
      here scales with overall write throughput across the
      whole app. Higher values reduce that contention at the
      cost of more ETS tables. The default matches the
      schedulers-online heuristic used elsewhere in OTP.

  Example:

      # config/runtime.exs
      import Config

      config :tidefall, registry_partitions: 16

  ## Telemetry

  `Tidefall` emits the following telemetry events.

    * `[:tidefall, :partition, :start]` - Dispatched when a partition
      is started.

      * Measurement: `%{system_time: integer}`
      * Metadata: `%{buffer: atom, partition: atom}`

    * `[:tidefall, :partition, :stop]` - Dispatched when a partition
      terminates (gracefully or abnormally).

      * Measurement: `%{duration: native_time}`
      * Metadata: `%{buffer: atom, partition: atom, reason: term}`

    * `[:tidefall, :partition, :processing, :start]` - Dispatched
      when a partition begins processing a batch of messages.

      * Measurement: `%{system_time: integer}`
      * Metadata: `%{buffer: atom, partition: atom}`

    * `[:tidefall, :partition, :processing, :stop]` - Dispatched
      when a partition completes processing a batch of messages.

      * Measurement: `%{duration: native_time, size: non_neg_integer}`
      * Metadata: `%{buffer: atom, partition: atom}`

    * `[:tidefall, :partition, :processing, :exception]` - Dispatched
      when an exception occurs during processing.

      * Measurement: `%{duration: native_time}`
      * Metadata:

      ```
      %{
        buffer: atom,
        partition: atom,
        kind: atom,
        reason: term,
        stacktrace: list
      }
      ```

    * `[:tidefall, :partition, :processing_failed]` - Dispatched
      when a processing task encounters an error and fails.

      * Measurement: `%{system_time: integer}`
      * Metadata: `%{buffer: atom, partition: atom, reason: any}`

  """
end
