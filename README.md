# Tidefall :ocean:
> _**DATA RISES. THEN IT FALLS.**_

![CI](https://github.com/cabol/tidefall/workflows/CI/badge.svg)
[![Codecov](https://codecov.io/gh/cabol/tidefall/graph/badge.svg)](https://codecov.io/gh/cabol/tidefall)
[![Hex.pm](https://img.shields.io/hexpm/v/tidefall.svg)](https://hex.pm/packages/tidefall)
[![Documentation](https://img.shields.io/badge/Documentation-ff69b4)](https://hexdocs.pm/tidefall)

ETS-backed buffer for Elixir — accumulate writes, drain in periodic batches.

`Tidefall` is a generic, reusable buffering system inspired by the
[OpenTelemetry Batch Processor](https://github.com/open-telemetry/opentelemetry-erlang/blob/main/apps/opentelemetry/src/otel_batch_processor.erl).
It efficiently buffers arbitrary data and periodically processes it using a
configurable processor function at regular intervals.

It ships with two concrete buffer implementations:

- **`Tidefall.Queue`** — Ordered queue buffer (insertion-time ordered,
  backed by `:ordered_set` ETS tables). Every pushed item is buffered and
  drained to the processor.
- **`Tidefall.HashMap`** — Coalescing key-value buffer (last-write-wins
  semantics, backed by `:set` ETS tables). Same-key writes coalesce, so only
  the latest value per key survives to the next tick.

Both use partitioning to reduce lock contention and double-buffering for
zero-downtime processing.

## 📦 Installation

Add `:tidefall` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:tidefall, "~> 1.0.0-rc.1"}
  ]
end
```

## 🚀 Usage

The recommended pattern is to define a dedicated buffer module and add it to
your supervision tree. The module name becomes the default instance name:

```elixir
defmodule MyApp.EventQueue do
  use Tidefall.Queue, otp_app: :my_app
end

defmodule MyApp.StateMap do
  use Tidefall.HashMap, otp_app: :my_app
end
```

```elixir
# Supervision tree
children = [
  {MyApp.EventQueue, processor: &MyApp.Sink.export/1, processing_interval: 1_000},
  {MyApp.StateMap, processor: &MyApp.Sink.export/1}
]

Supervisor.start_link(children, strategy: :one_for_one)
```

```elixir
# Queue — every item is buffered in insertion order and drained to the processor
:ok = MyApp.EventQueue.push(event)

# HashMap — same-key writes coalesce; only the latest value per key survives,
# and put_newer/4 resolves conflicts by version (newer version wins)
:ok = MyApp.StateMap.put(:user_1, %{name: "Alice"})
:ok = MyApp.StateMap.put_newer(:user_1, %{name: "Alice"}, version: 2)
```

The processor receives the accumulated batch on each tick and runs for its
side effects (its return value is discarded):

```elixir
def export(batch) do
  # Queue:   batch is [value, ...]
  # HashMap: batch is [%Tidefall.HashMap.Entry{key: k, value: v}, ...]
  Enum.each(batch, &MyApp.Sink.write/1)
end
```

For quick experiments or fully dynamic instances, a buffer can also be started
directly with a runtime `:name`, e.g.
`Tidefall.Queue.start_link(name: :my_queue, processor: &MyApp.Sink.export/1)`.

See the **[full documentation on HexDocs](https://hexdocs.pm/tidefall)** for
the module-based and direct-usage guides, configuration (config file and
supervision tree), choosing between Queue and HashMap, telemetry events, and
the complete start/runtime option reference.

## 🤝 Contributing

Contributions are welcome and appreciated! To report a bug, request a feature,
or open a pull request, see [`CONTRIBUTING.md`](CONTRIBUTING.md) for the
workflow, validation steps (`mix test.ci`), and commit conventions.

## 🙏 Acknowledgements

Tidefall began as a fork of
[`partitioned_buffer`](https://hex.pm/packages/partitioned_buffer), originally
developed at [Appcues](https://github.com/appcues). `partitioned_buffer` is
still maintained by Appcues; Tidefall is an independent fork that has since
taken a different direction. Many thanks to the Appcues team for releasing the
original implementation under the MIT license.

## 📄 Copyright and License

Copyright 2026 Carlos Bolaños (Tidefall)\
Copyright 2025 Appcues, Inc. (PartitionedBuffer)

Tidefall source code is licensed under the [MIT License](LICENSE.md).
