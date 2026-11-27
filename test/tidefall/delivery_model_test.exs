defmodule Tidefall.DeliveryModelTest do
  use ExUnit.Case, async: false
  use ExUnitProperties

  import Tidefall.TestBuffer, only: [flush_batches: 0]

  alias Tidefall.{HashMap, Queue, TestBuffer}

  # Fixed names, so a case that fails leaves its buffer up for the next to stop.
  @queue :queue_delivery
  @hash_map :hash_map_delivery

  property "Queue: each tick delivers the items pushed since the last one, in order" do
    check all commands <- list_of(queue_command(), max_length: 30),
              batch_size <- integer(1..4),
              max_runs: 150 do
      TestBuffer.start!(Queue, @queue, partitions: 1, processing_batch_size: batch_size)

      pending =
        Enum.reduce(commands, [], fn
          {:push, items}, pending ->
            :ok = Queue.push(@queue, items)
            pending ++ items

          :tick, pending ->
            assert Queue.size(@queue) == length(pending)
            :ok = TestBuffer.tick!(@queue)
            assert flush_batches() == pending
            []
        end)

      assert Queue.size(@queue) == length(pending)
      TestBuffer.stop!(Queue, @queue)
      assert flush_batches() == pending
    end
  end

  property "HashMap: each tick delivers the coalesced window, then the window resets" do
    check all commands <- list_of(hash_map_command(), max_length: 30),
              batch_size <- integer(1..4),
              max_runs: 150 do
      TestBuffer.start!(HashMap, @hash_map, partitions: 1, processing_batch_size: batch_size)

      window =
        Enum.reduce(commands, %{}, fn
          :tick, window ->
            assert HashMap.size(@hash_map) == map_size(window)
            :ok = TestBuffer.tick!(@hash_map)
            assert delivered_window() == Enum.sort(window)
            %{}

          command, window ->
            :ok = run(command)
            step(command, window)
        end)

      assert HashMap.size(@hash_map) == map_size(window)
      TestBuffer.stop!(HashMap, @hash_map)
      assert delivered_window() == Enum.sort(window)
    end
  end

  ## Generators

  defp queue_command do
    one_of([
      tuple({constant(:push), list_of(integer(), min_length: 1, max_length: 5)}),
      constant(:tick)
    ])
  end

  # Few keys, so writes to one key coalesce. Values and versions are
  # plain integers: the model property covers their shapes.
  defp hash_map_command do
    key = integer(0..3)

    one_of([
      tuple({constant(:put), key, integer()}),
      tuple({constant(:put_newer), key, integer(), integer(0..5)}),
      tuple({constant(:delete), key}),
      constant(:tick)
    ])
  end

  ## HashMap system under test

  defp run({:put, key, value}) do
    HashMap.put(@hash_map, key, value)
  end

  defp run({:put_newer, key, value, version}) do
    HashMap.put_newer(@hash_map, key, value, version: version)
  end

  defp run({:delete, key}) do
    HashMap.delete(@hash_map, key)
  end

  ## HashMap model: key => {value, version}

  defp step({:put, key, value}, window) do
    Map.put(window, key, {value, 0})
  end

  defp step({:put_newer, key, value, version}, window) do
    case window do
      %{^key => {_value, old}} when version <= old -> window
      %{} -> Map.put(window, key, {value, version})
    end
  end

  defp step({:delete, key}, window) do
    Map.delete(window, key)
  end

  # Sorted pairs rather than a map, so a key delivered twice fails.
  defp delivered_window do
    flush_batches()
    |> Enum.map(&{&1.key, {&1.value, &1.version}})
    |> Enum.sort()
  end
end
