# Tidefall :ocean:
> DATA RISES. THEN IT FALLS.

![CI](https://github.com/cabol/tidefall/workflows/CI/badge.svg)
[![Hex.pm](https://img.shields.io/hexpm/v/tidefall.svg)](https://hex.pm/packages/tidefall)
[![Documentation](https://img.shields.io/badge/Documentation-ff69b4)](https://hexdocs.pm/tidefall)

ETS-backed buffer for Elixir — accumulate writes, drain in periodic batches.

`Tidefall` is a generic, reusable buffering system inspired by the
[OpenTelemetry Batch Processor](https://github.com/open-telemetry/opentelemetry-erlang/blob/main/apps/opentelemetry/src/otel_batch_processor.erl).
It efficiently buffers arbitrary data and periodically processes it using a
configurable processor function at regular intervals.

It ships with two concrete buffer implementations:

- **`Tidefall.Queue`** — Ordered queue buffer (insertion-time ordered,
  backed by `:ordered_set` ETS tables).
- **`Tidefall.CoalescedMap`** — Coalescing key-value buffer (last-write-wins semantics,
  backed by `:set` ETS tables).

Both use partitioning to reduce lock contention and double-buffering for
zero-downtime processing.

## Installation

Add `:tidefall` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:tidefall, "~> 1.0"}
  ]
end
```

## Usage

### Queue

`Tidefall.Queue` buffers items in insertion order and processes them
in batches:

```elixir
# Start a queue buffer
{:ok, _pid} =
  Tidefall.Queue.start_link(
    name: :my_queue,
    processor: fn batch -> IO.inspect(batch) end
  )

# Push items into the buffer
:ok = Tidefall.Queue.push(:my_queue, "message1")
:ok = Tidefall.Queue.push(:my_queue, ["message2", "message3"])

# Check buffer size
Tidefall.Queue.size(:my_queue)
```

### CoalescedMap

`Tidefall.CoalescedMap` buffers key-value entries with last-write-wins
semantics. Entries with the same key overwrite previous values:

```elixir
# Start a CoalescedMap buffer
# The processor receives a list of %Tidefall.CoalescedMap.Entry{} structs
{:ok, _pid} =
  Tidefall.CoalescedMap.start_link(
    name: :my_map,
    processor: fn batch -> IO.inspect(batch) end
  )

# Put entries into the buffer
:ok = Tidefall.CoalescedMap.put(:my_map, :key1, "value1")
:ok = Tidefall.CoalescedMap.put_all(:my_map, %{key2: "value2", key3: "value3"})

# Read and delete entries
"value1" = Tidefall.CoalescedMap.get(:my_map, :key1)
:ok = Tidefall.CoalescedMap.delete(:my_map, :key1)

# Check buffer size
Tidefall.CoalescedMap.size(:my_map)
```

#### Versioned Updates

For scenarios requiring "newer version wins" semantics (e.g., event sourcing,
state synchronization), use `put_newer/5` and `put_all_newer/3`:

```elixir
# Only updates if the version is greater than the existing one
:ok = Tidefall.CoalescedMap.put_newer(:my_map, :key1, "v1", 100)
:ok = Tidefall.CoalescedMap.put_newer(:my_map, :key1, "v2", 200)  # overwrites
:ok = Tidefall.CoalescedMap.put_newer(:my_map, :key1, "v3", 50)   # ignored (50 < 200)

"v2" = Tidefall.CoalescedMap.get(:my_map, :key1)

# Batch versioned updates
entries = [
  {:user_1, %{name: "Alice"}, 100},
  {:user_2, %{name: "Bob"}, 200}
]
:ok = Tidefall.CoalescedMap.put_all_newer(:my_map, entries)
```

### Adding to a Supervision Tree

In most applications, you'll want to add a buffer as a child in your
application's supervision tree:

```elixir
defmodule MyApp.Application do
  use Application

  @impl true
  def start(_type, _args) do
    children = [
      # Queue buffer
      {Tidefall.Queue,
       name: :event_queue,
       processor: &MyApp.EventProcessor.process_batch/1,
       processing_interval: 1000,
       partitions: 4},

      # CoalescedMap buffer
      {Tidefall.CoalescedMap,
       name: :state_map,
       processor: &MyApp.StateProcessor.process_batch/1}
    ]

    opts = [strategy: :one_for_one, name: MyApp.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
```

The buffer will be automatically started with your application and will process
any remaining items during graceful shutdown.

### Configuration Options

```elixir
{:ok, _pid} = Tidefall.Queue.start_link(
  name: :my_buffer,
  partitions: 4,                        # Number of partitions (default: schedulers_online)
  processing_interval: 1000,         # Process every second (default: 5000)
  processing_batch_size: 100,           # Batch size for processing (default: 10)
  processing_timeout: 5000,          # Timeout for processing tasks (default: 60000)
  processor: &MyApp.Exporter.export/1
)
```

See the `Tidefall` module documentation for the full list of start and
runtime options.

## Acknowledgements

Tidefall was originally developed at [Appcues](https://github.com/appcues)
under the name [`partitioned_buffer`](https://hex.pm/packages/partitioned_buffer).
It is now maintained independently under a new name and direction. Many thanks
to the Appcues team for releasing the original implementation under the MIT
license.
