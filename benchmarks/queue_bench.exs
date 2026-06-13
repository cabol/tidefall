# Benchmark: Tidefall.Queue operations
#
# Run with: mix run benchmarks/queue_bench.exs

alias Tidefall.Queue

IO.puts("Setting up Queue buffer for benchmarks...\n")

# Start the buffer with a no-op processor (we just want to measure write
# performance). Unlike HashMap, Queue does not coalesce, so we drain on a short
# interval with a large batch size to keep memory bounded during the run.
{:ok, _pid} =
  Queue.start_link(
    name: :bench_queue,
    processor: fn _batch -> :ok end,
    processing_interval: :timer.seconds(1),
    processing_batch_size: 50_000,
    partitions: System.schedulers_online()
  )

IO.puts("Buffer ready. Running benchmarks...\n")

# --- Benchmarks ---

item_pool_size = 100_000

# A single item and a batch of 10 distinct items (the batch spreads across
# partitions via phash2 routing).
item = Enum.random(1..item_pool_size)
batch = for _ <- 1..10, do: Enum.random(1..item_pool_size)

Benchee.run(
  %{
    "push/2 (single item)" => fn ->
      Queue.push(:bench_queue, item)
    end,
    "push/2 (batch of 10)" => fn ->
      Queue.push(:bench_queue, batch)
    end
  },
  warmup: 2,
  time: 10,
  memory_time: 2
)

# Cleanup
Queue.stop(:bench_queue)

IO.puts("\nBenchmark complete!")
