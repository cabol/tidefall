defmodule Tidefall.MetadataTest do
  use ExUnit.Case, async: true

  alias Tidefall.Metadata

  describe "start_link/1" do
    test "starts with the default name and creates the named ETS table" do
      # The application's supervision tree already started a
      # default-named Metadata, so its ETS table must exist.
      assert :ets.info(Tidefall.Metadata, :name) == Tidefall.Metadata
    end

    test "starts with a custom :name option" do
      name = :"metadata_#{System.unique_integer([:positive])}"

      assert {:ok, pid} = Metadata.start_link(name: name)
      assert Process.alive?(pid)
      assert :ets.info(name, :name) == name

      GenServer.stop(pid)
    end

    test "returns {:error, {:already_started, _}} on duplicate name" do
      name = :"metadata_#{System.unique_integer([:positive])}"

      {:ok, pid} = Metadata.start_link(name: name)

      assert Metadata.start_link(name: name) == {:error, {:already_started, pid}}
    end
  end

  describe "default table (app-started Tidefall.Metadata)" do
    test "put and get against the default table" do
      key = {:default_test, System.unique_integer([:positive])}

      assert Metadata.put(key, :value) == :ok
      assert Metadata.get(key) == :value

      # Cleanup so we don't leak state across runs.
      assert Metadata.delete(key) == :ok
    end
  end

  describe "put/3 and get/2" do
    setup do
      name = :"metadata_#{System.unique_integer([:positive])}"
      start_supervised!({Metadata, name: name})

      {:ok, table: name}
    end

    test "round-trips a value", %{table: table} do
      assert Metadata.put(table, :key, "value") == :ok
      assert Metadata.get(table, :key) == "value"
    end

    test "supports arbitrary key and value terms", %{table: table} do
      keys = [
        :atom,
        "string",
        {:tuple, 1},
        [1, 2, 3],
        %{nested: %{map: true}},
        {Tidefall.Metadata, :compound, "key"}
      ]

      for key <- keys do
        assert Metadata.put(table, key, key) == :ok
        assert Metadata.get(table, key) == key
      end
    end

    test "put replaces the existing value (last-write-wins)", %{table: table} do
      :ok = Metadata.put(table, :k, :first)
      :ok = Metadata.put(table, :k, :second)
      :ok = Metadata.put(table, :k, :third)

      assert Metadata.get(table, :k) == :third
    end

    test "get raises when the key is not present", %{table: table} do
      assert_raise RuntimeError,
                   ~r"unable to find metadata entry for key :missing",
                   fn -> Metadata.get(table, :missing) end
    end
  end

  describe "delete/2" do
    setup do
      name = :"metadata_#{System.unique_integer([:positive])}"
      start_supervised!({Metadata, name: name})

      {:ok, table: name}
    end

    test "removes an existing entry", %{table: table} do
      :ok = Metadata.put(table, :k, :v)
      assert Metadata.delete(table, :k) == :ok

      assert_raise RuntimeError, fn -> Metadata.get(table, :k) end
    end

    test "is a no-op for a missing key", %{table: table} do
      assert Metadata.delete(table, :never_set) == :ok
    end
  end
end
