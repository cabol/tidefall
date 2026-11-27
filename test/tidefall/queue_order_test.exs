defmodule Tidefall.QueueOrderTest do
  use ExUnit.Case, async: false
  use ExUnitProperties

  alias Tidefall.{Generators, Queue, TestBuffer}

  # Fixed name, so a case that fails leaves its buffer up for the next to stop.
  @buffer :queue_order

  # Large enough to get one batch per partition at stop/1.
  @batch_size 1_000_000

  property "default sort_key: one partition delivers items in push order" do
    check all pushes <- pushes(Generators.any_term()), max_runs: 150 do
      TestBuffer.start!(Queue, @buffer, partitions: 1, processing_batch_size: @batch_size)

      Enum.each(pushes, &(:ok = Queue.push(@buffer, &1)))
      TestBuffer.stop!(Queue, @buffer)

      assert TestBuffer.flush_batches() === Enum.concat(pushes)
    end
  end

  property "custom sort_key: batches are sorted, partition keys co-located, nothing lost" do
    check all terms <- list_of(Generators.any_term(), min_length: 1, max_length: 6),
              pushes <- pushes(member_of(terms)),
              partitions <- integer(1..4),
              max_runs: 150 do
      TestBuffer.start!(Queue, @buffer, partitions: partitions, processing_batch_size: @batch_size)

      Enum.each(pushes, fn items ->
        :ok = Queue.push(@buffer, items, sort_key: &priority/1, partition_key: &partition_key/1)
      end)

      TestBuffer.stop!(Queue, @buffer)
      batches = TestBuffer.batches()

      # One batch per partition at most, so a batch index stands for a partition.
      assert length(batches) <= partitions

      for batch <- batches do
        priorities = Enum.map(batch, &priority/1)
        assert priorities == Enum.sort(priorities)
      end

      # Group batch indexes by the encoded key; before OTP 27, 0.0 and -0.0
      # are the same map key, but they may go to different partitions.
      indexes_by_key =
        batches
        |> Enum.with_index()
        |> Enum.flat_map(fn {batch, index} ->
          Enum.map(batch, &{:erlang.term_to_binary(partition_key(&1)), index})
        end)
        |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))

      for {_key, indexes} <- indexes_by_key do
        assert [_one] = Enum.uniq(indexes)
      end

      # Tags follow the push order, so sorting by them restores it.
      assert Enum.sort(Enum.concat(batches)) === Enum.concat(pushes)
    end
  end

  ## Generators

  # Lists of {term, priority} items, each tagged {n, item} in push order
  # to keep them distinct; priorities are few to get many ties.
  defp pushes(term_generator) do
    {term_generator, integer(-3..3)}
    |> tuple()
    |> list_of(max_length: 6)
    |> list_of(max_length: 8)
    |> map(&tag/1)
  end

  defp tag(pushes) do
    {tagged, _count} =
      Enum.map_reduce(pushes, 0, fn items, n ->
        Enum.map_reduce(items, n, fn item, n -> {{n, item}, n + 1} end)
      end)

    tagged
  end

  defp priority({_tag, {_term, priority}}), do: priority

  defp partition_key({_tag, {term, _priority}}), do: term
end
