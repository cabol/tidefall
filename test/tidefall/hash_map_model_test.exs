defmodule Tidefall.HashMapModelTest do
  use ExUnit.Case, async: false
  use ExUnitProperties

  alias Tidefall.{Generators, HashMap, TestBuffer}
  alias Tidefall.HashMap.Entry

  # Fixed name, so a case that fails leaves it up for the next to stop
  @buffer :hash_map_model

  # get/4 default that no generator produces
  @missing {__MODULE__, :missing}

  property "agrees with a map model: last-write-wins put, newer-version-wins put_newer" do
    check all key_hasher <- member_of([false, true, &sha256/1]),
              keys <-
                uniq_list_of(Generators.any_term(key_mode(key_hasher)),
                  uniq_fun: storage_key(key_hasher),
                  min_length: 1,
                  max_length: 6
                ),
              commands <- list_of(command(keys), max_length: 25),
              partition_key <- member_of([nil, :static, &:erlang.phash2/1, {:erlang, :phash2, []}]),
              partitions <- integer(1..4),
              batch_size <- integer(1..5),
              max_runs: 150 do
      TestBuffer.start!(HashMap, @buffer, partitions: partitions, processing_batch_size: batch_size)
      opts = [key_hasher: key_hasher, partition_key: partition_key]

      model =
        Enum.reduce(commands, %{}, fn command, model ->
          :ok = run(command, opts)
          step(command, model)
        end)

      assert HashMap.size(@buffer) == map_size(model)

      for key <- keys do
        expected =
          case model do
            %{^key => %Entry{value: value}} -> value
            %{} -> @missing
          end

        assert HashMap.get(@buffer, key, @missing, opts) === expected
      end

      TestBuffer.stop!(HashMap, @buffer)
      entries = TestBuffer.flush_batches()

      assert length(entries) == map_size(model)
      assert Map.new(entries, &{&1.key, &1}) === model
    end
  end

  ## Generators

  # Without a hasher, keys holding maps or pattern atoms are a
  # documented `put_newer` limitation; the hasher is the workaround.
  defp key_mode(false), do: :key_safe
  defp key_mode(_hasher), do: :any

  # Distinct keys can share a hash (`:erlang.phash2/1` maps 0.0 and
  # -0.0 to one value), so keep the pool unique by the stored key
  defp storage_key(false), do: & &1
  defp storage_key(true), do: &:erlang.phash2/1
  defp storage_key(hasher), do: hasher

  defp command(keys) do
    key = member_of(keys)
    value = Generators.any_term()
    version = Generators.version()

    frequency([
      {3, tuple({constant(:put), key, value})},
      {2, tuple({constant(:put_all), put_all_entries(key, value)})},
      {5, tuple({constant(:put_newer), key, value, version})},
      {3, tuple({constant(:put_all_newer), list_of({key, value, version}, max_length: 4)})},
      {1, tuple({constant(:delete), key})}
    ])
  end

  # Unique keys, as a map or a list: put_all/3 doesn't define which of
  # two entries with the same key wins
  defp put_all_entries(key, value) do
    entries = map(list_of({key, value}, max_length: 5), &Map.new/1)

    one_of([entries, map(entries, &Map.to_list/1)])
  end

  defp sha256(key), do: :crypto.hash(:sha256, :erlang.term_to_binary(key))

  ## System under test

  defp run({:put, key, value}, opts) do
    HashMap.put(@buffer, key, value, opts)
  end

  defp run({:put_all, entries}, opts) do
    HashMap.put_all(@buffer, entries, opts)
  end

  defp run({:put_newer, key, value, version}, opts) do
    HashMap.put_newer(@buffer, key, value, [version: version] ++ opts)
  end

  defp run({:put_all_newer, entries}, opts) do
    HashMap.put_all_newer(@buffer, entries, opts)
  end

  defp run({:delete, key}, opts) do
    HashMap.delete(@buffer, key, opts)
  end

  ## Model

  defp step({:put, key, value}, model) do
    Map.put(model, key, new_entry(key, value, 0))
  end

  defp step({:put_all, entries}, model) do
    Enum.reduce(entries, model, fn {key, value}, model -> step({:put, key, value}, model) end)
  end

  defp step({:put_newer, key, value, version}, model) do
    case model do
      %{^key => %Entry{version: old} = entry} when version > old ->
        %{model | key => %{entry | value: value, version: version, updates: entry.updates + 1}}

      %{^key => _older_or_equal} ->
        model

      %{} ->
        Map.put(model, key, new_entry(key, value, version))
    end
  end

  defp step({:put_all_newer, entries}, model) do
    Enum.reduce(entries, model, fn {key, value, version}, model ->
      step({:put_newer, key, value, version}, model)
    end)
  end

  defp step({:delete, key}, model) do
    Map.delete(model, key)
  end

  defp new_entry(key, value, version) do
    %Entry{key: key, value: value, version: version, updates: 0}
  end
end
