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

  ## Options

  Tidefall reads the following options from the application
  environment at startup (set them in your `config/config.exs`
  or `config/runtime.exs`):

    * `:registry_partitions` (positive integer, default
      `System.schedulers_online()`) — number of internal ETS
      partitions for `Tidefall.Registry`, the shared registry
      used by all buffers to locate their partitions. Every
      buffer write performs one registry lookup, so contention
      here scales with overall write throughput across the
      whole app. Higher values reduce that contention at the
      cost of more ETS tables. The default matches the
      schedulers-online heuristic used elsewhere in OTP.

  ## Example

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
