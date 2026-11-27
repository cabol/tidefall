defmodule Tidefall.HashMapPutAllNewerTest do
  use ExUnit.Case, async: true

  alias Tidefall.HashMap

  describe "put_all_newer/3 while the partition swaps tables" do
    setup do
      name = :"put_all_newer_race_#{System.unique_integer([:positive])}"

      # A 5 ms processing interval makes the partition swap (and delete) its
      # table many times while a batch of large values is being written.
      start_supervised!(
        {HashMap,
         name: name, partitions: 1, processing_interval: 5, processor: fn _batch -> :ok end}
      )

      {:ok, buffer: name}
    end

    test "never writes to a table that was swapped out and deleted", %{buffer: buffer} do
      value =
        for i <- 1..500 do
          %{"title" => "row-#{i}", "facility" => "F#{i}", "status" => "completed"}
        end

      for round <- 1..200 do
        entries = for key <- 1..20, do: {{:key, key}, value, 2 * round}

        assert :ok = HashMap.put_all_newer(buffer, entries)

        # Writing the same keys again goes through the select_replace path.
        assert :ok =
                 HashMap.put_all_newer(
                   buffer,
                   Enum.map(entries, fn {key, value, version} -> {key, value, version + 1} end)
                 )
      end
    end
  end

  describe "put_all_newer/3 replacing an existing entry" do
    setup do
      name = :"put_all_newer_replace_#{System.unique_integer([:positive])}"

      start_supervised!(
        {HashMap,
         name: name,
         partitions: 1,
         processing_interval: :timer.hours(1),
         processor: fn _batch -> :ok end}
      )

      {:ok, buffer: name}
    end

    test "stores a value holding thousands of elements", %{buffer: buffer} do
      value = for i <- 1..50_000, do: %{"title" => "row-#{i}"}

      assert :ok = HashMap.put_all_newer(buffer, [{:key, :old, 1}])
      assert :ok = HashMap.put_all_newer(buffer, [{:key, value, 2}])

      assert HashMap.get(buffer, :key) == value
    end

    test "stores tuples and match spec variables as they are", %{buffer: buffer} do
      key = {:put, :key}
      value = {:put, [{:key, :"$1", :_}, %{nested: {1, [2, {:"$2"}]}}]}

      assert :ok = HashMap.put_all_newer(buffer, [{key, :old, 1}])
      assert :ok = HashMap.put_all_newer(buffer, [{key, value, 2}])

      assert HashMap.get(buffer, key) == value
    end

    test "keeps the newer version", %{buffer: buffer} do
      assert :ok = HashMap.put_all_newer(buffer, [{:key, :newer, 2}])
      assert :ok = HashMap.put_all_newer(buffer, [{:key, :older, 1}])

      assert HashMap.get(buffer, :key) == :newer
    end
  end
end
