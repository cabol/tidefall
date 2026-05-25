# Benchmark: Tidefall.HashMap operations
#
# Run with: mix run benchmarks/hash_map_bench.exs

alias Tidefall.HashMap

IO.puts("Setting up HashMap buffer for benchmarks...\n")

# Start the buffer with a no-op processor (we just want to measure write performance)
{:ok, _pid} =
  HashMap.start_link(
    name: :bench_hash_map,
    processor: fn _batch -> :ok end,
    processing_interval: :timer.minutes(1),
    partitions: System.schedulers_online()
  )

# Pre-generate keys to avoid atom table exhaustion
# We'll cycle through these keys during benchmarks
key_pool_size = 100_000

# Use a tuple for O(1) access instead of list
entries = Enum.map(1..key_pool_size, &{&1, &1})

# Pre-populate entries for get/delete benchmarks
IO.puts("Pre-populating #{key_pool_size} entries...")

HashMap.put_all(:bench_hash_map, entries)

IO.puts("Buffer ready. Running benchmarks...\n")

# --- Benchmarks ---

# Generate a batch of 10 keys for put_all benchmarks
next_batch = fn count ->
  for _ <- 1..count do
    key = Enum.random(1..key_pool_size)

    {key, key}
  end
end

# Generate a batch of 10 versioned entries for put_all_newer benchmarks
next_versioned_batch = fn count ->
  for _ <- 1..count do
    key = Enum.random(1..key_pool_size)

    {key, key, key + 1}
  end
end

Benchee.run(
  %{
    "put_all/3" => fn {batch, _} ->
      HashMap.put_all(:bench_hash_map, batch)
    end,
    "put_all_newer/3" => fn {_, versioned_batch} ->
      HashMap.put_all_newer(:bench_hash_map, versioned_batch)
    end
  },
  inputs: %{
    "single entry" => {next_batch.(1), next_versioned_batch.(1)},
    "batch of 10 entries" => {next_batch.(10), next_versioned_batch.(10)}
  },
  warmup: 2,
  time: 10,
  memory_time: 2
)

# Cleanup
HashMap.stop(:bench_hash_map)

IO.puts("\nBenchmark complete!")
