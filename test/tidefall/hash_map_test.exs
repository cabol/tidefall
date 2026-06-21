defmodule Tidefall.HashMapTest do
  use ExUnit.Case, async: false

  alias Tidefall.HashMap
  alias Tidefall.HashMap.Entry

  # Omit logs during the tests
  @moduletag capture_log: true

  # Default waiting time when receiving a message (e.g., assert_receive ...)
  @default_timeout :timer.seconds(5)

  # Telemetry events
  @partition_stop_event [:tidefall, :partition, :stop]
  @processing_start_event [:tidefall, :partition, :processing, :start]
  @processing_stop_event [:tidefall, :partition, :processing, :stop]
  @processing_failed_event [:tidefall, :partition, :processing_failed]

  @telemetry_events [
    @partition_stop_event,
    @processing_start_event,
    @processing_stop_event,
    @processing_failed_event
  ]

  setup do
    handler_id = attach_telemetry_handler(@telemetry_events)

    on_exit(fn -> :telemetry.detach(handler_id) end)

    :ok
  end

  describe "stop/3" do
    test "ok: stops the buffer" do
      {:ok, _} =
        HashMap.start_link(
          name: :hm_stop_buff,
          processor: &__MODULE__.test_processor(self(), &1),
          partitions: 1
        )

      assert HashMap.stop(:hm_stop_buff) == :ok

      assert_receive {@partition_stop_event, %{duration: _},
                      %{buffer: :hm_stop_buff, partition: _, reason: _}},
                     @default_timeout
    end
  end

  describe "put/4 and put_all/3" do
    setup do
      self = self()

      pid =
        start_supervised!(
          {HashMap,
           name: __MODULE__,
           processing_interval: 500,
           processing_batch_size: 5,
           processor: &__MODULE__.test_processor(self, &1),
           partitions: 1}
        )

      {:ok, buffer: __MODULE__, pid: pid}
    end

    test "ok: single put is processed", %{buffer: buff} do
      assert HashMap.size(buff) == 0

      assert HashMap.put(buff, :key1, "value1") == :ok

      assert HashMap.size(buff) == 1
      assert HashMap.get(buff, :key1) == "value1"

      # Wait for processing to complete
      assert_receive {@processing_stop_event, %{duration: _, size: 1},
                      %{buffer: ^buff, partition: _}},
                     @default_timeout

      assert_receive {:process_completed,
                      [%Entry{key: :key1, value: "value1", version: 0, updates: 0}]},
                     @default_timeout
    end

    test "ok: batch put with list of tuples", %{buffer: buff} do
      entries = [{:a, 1}, {:b, 2}, {:c, 3}]

      assert HashMap.put_all(buff, entries) == :ok

      assert HashMap.size(buff) == 3

      # Wait for processing to complete
      assert_receive {@processing_stop_event, %{duration: _, size: 3},
                      %{buffer: ^buff, partition: _}},
                     @default_timeout

      assert_receive {:process_completed, batch}, @default_timeout

      assert Enum.sort_by(batch, & &1.key) == [
               %Entry{key: :a, value: 1, version: 0, updates: 0},
               %Entry{key: :b, value: 2, version: 0, updates: 0},
               %Entry{key: :c, value: 3, version: 0, updates: 0}
             ]
    end

    test "ok: batch put with map", %{buffer: buff} do
      entries = %{x: "hello", y: "world"}

      assert HashMap.put_all(buff, entries) == :ok

      assert HashMap.size(buff) == 2

      # Wait for processing to complete
      assert_receive {@processing_stop_event, %{duration: _, size: 2},
                      %{buffer: ^buff, partition: _}},
                     @default_timeout

      assert_receive {:process_completed, batch}, @default_timeout

      assert Enum.sort_by(batch, & &1.key) == [
               %Entry{key: :x, value: "hello", version: 0, updates: 0},
               %Entry{key: :y, value: "world", version: 0, updates: 0}
             ]
    end

    test "ok: last-write-wins for duplicate keys", %{buffer: buff} do
      assert HashMap.put(buff, :dup, "first") == :ok
      assert HashMap.get(buff, :dup) == "first"

      assert HashMap.put(buff, :dup, "second") == :ok
      assert HashMap.get(buff, :dup) == "second"

      # Only one entry should remain (last write wins)
      assert HashMap.size(buff) == 1

      # Wait for processing to complete
      assert_receive {@processing_stop_event, %{duration: _, size: 1},
                      %{buffer: ^buff, partition: _}},
                     @default_timeout

      assert_receive {:process_completed,
                      [%Entry{key: :dup, value: "second", version: 0, updates: 0}]},
                     @default_timeout
    end

    test "ok: entries are processed in batches", %{buffer: buff} do
      entries = Enum.map(1..10, fn i -> {:"key_#{i}", i} end)

      assert HashMap.put_all(buff, entries) == :ok

      # Wait for processing to complete
      assert_receive {@processing_stop_event, %{duration: _, size: 10},
                      %{buffer: ^buff, partition: _}},
                     @default_timeout

      # Should receive two batches of 5 (processing_batch_size is 5)
      assert_receive {:process_completed, batch1}, @default_timeout
      assert_receive {:process_completed, batch2}, @default_timeout
      assert length(batch1) == 5
      assert length(batch2) == 5

      # Extract values from %Entry{} structs and sort
      values = Enum.map(batch1 ++ batch2, & &1.value)
      assert Enum.sort(values) == Enum.to_list(1..10)
    end

    test "ok: entries are partitioned using a function", %{buffer: buff} do
      assert HashMap.put(buff, :key1, "value1", partition_key: &__MODULE__.partition_key_fun/1) ==
               :ok

      assert HashMap.size(buff) == 1

      assert_receive {@processing_stop_event, %{duration: _, size: 1},
                      %{buffer: ^buff, partition: _}},
                     @default_timeout
    end

    test "ok: entries are partitioned using an MFA tuple", %{buffer: buff} do
      assert HashMap.put(buff, :key1, "value1", partition_key: {__MODULE__, :partition_key_fun, []}) ==
               :ok

      assert HashMap.size(buff) == 1

      assert_receive {@processing_stop_event, %{duration: _, size: 1},
                      %{buffer: ^buff, partition: _}},
                     @default_timeout
    end

    test "ok: entries are partitioned using a static key", %{buffer: buff} do
      assert HashMap.put(buff, :key1, "value1", partition_key: :static) == :ok

      assert HashMap.size(buff) == 1

      assert_receive {@processing_stop_event, %{duration: _, size: 1},
                      %{buffer: ^buff, partition: _}},
                     @default_timeout
    end

    test "error: no partitions available for the given buffer" do
      {:ok, buff} =
        HashMap.start_link(
          name: :hm_error_buff,
          processor: &__MODULE__.test_processor(self(), &1),
          partitions: 0
        )

      assert_raise RuntimeError, ~r"no partitions available for buffer :hm_error_buff", fn ->
        HashMap.put(:hm_error_buff, :key, "value")
      end

      assert HashMap.stop(buff) == :ok
    end
  end

  describe "get/3" do
    setup do
      self = self()

      pid =
        start_supervised!(
          {HashMap, name: __MODULE__, processor: &__MODULE__.test_processor(self, &1)}
        )

      {:ok, buffer: __MODULE__, pid: pid}
    end

    test "ok: returns value for existing key", %{buffer: buff} do
      :ok = HashMap.put(buff, :key1, "value1")

      assert HashMap.get(buff, :key1) == "value1"
    end

    test "ok: returns nil for missing key", %{buffer: buff} do
      assert HashMap.get(buff, :missing) == nil
    end

    test "ok: returns custom default for missing key", %{buffer: buff} do
      assert HashMap.get(buff, :missing, :not_found) == :not_found
    end

    test "ok: reflects latest value after overwrite", %{buffer: buff} do
      :ok = HashMap.put(buff, :key1, "v1")
      assert HashMap.get(buff, :key1) == "v1"

      :ok = HashMap.put(buff, :key1, "v2")
      assert HashMap.get(buff, :key1) == "v2"
    end
  end

  describe "delete/3" do
    setup do
      self = self()

      pid =
        start_supervised!(
          {HashMap,
           name: __MODULE__,
           processing_interval: 1_000,
           processing_batch_size: 10,
           processor: &__MODULE__.test_processor(self, &1),
           partitions: 1}
        )

      {:ok, buffer: __MODULE__, pid: pid}
    end

    test "ok: put then delete before processing", %{buffer: buff} do
      assert HashMap.put(buff, :to_delete, "gone") == :ok
      assert HashMap.put(buff, :to_keep, "stays") == :ok
      assert HashMap.size(buff) == 2

      # Verify both exist before delete
      assert HashMap.get(buff, :to_delete) == "gone"
      assert HashMap.get(buff, :to_keep) == "stays"

      assert HashMap.delete(buff, :to_delete) == :ok
      assert HashMap.size(buff) == 1

      # Verify deleted key is gone and kept key remains
      assert HashMap.get(buff, :to_delete) == nil
      assert HashMap.get(buff, :to_keep) == "stays"

      # Wait for processing to complete
      assert_receive {@processing_stop_event, %{duration: _, size: 1},
                      %{buffer: ^buff, partition: _}},
                     @default_timeout

      # Only the kept entry should be processed
      assert_receive {:process_completed,
                      [%Entry{key: :to_keep, value: "stays", version: 0, updates: 0}]},
                     @default_timeout
    end
  end

  describe "put_newer/4 and put_all_newer/3" do
    setup do
      self = self()

      pid =
        start_supervised!(
          {HashMap,
           name: __MODULE__,
           processing_interval: 500,
           processing_batch_size: 10,
           processor: &__MODULE__.test_processor(self, &1),
           partitions: 1}
        )

      {:ok, buffer: __MODULE__, pid: pid}
    end

    test "ok: inserts new entry", %{buffer: buff} do
      assert HashMap.put_newer(buff, :key1, "value1", version: 100) == :ok

      assert HashMap.size(buff) == 1
      assert HashMap.get(buff, :key1) == "value1"

      assert_receive {@processing_stop_event, %{duration: _, size: 1},
                      %{buffer: ^buff, partition: _}},
                     @default_timeout

      assert_receive {:process_completed,
                      [%Entry{key: :key1, value: "value1", version: 100, updates: 0}]},
                     @default_timeout
    end

    test "ok: defaults version to a positive monotonic integer when omitted", %{buffer: buff} do
      assert HashMap.put_newer(buff, :key1, "value1") == :ok

      assert HashMap.size(buff) == 1
      assert HashMap.get(buff, :key1) == "value1"

      assert_receive {@processing_stop_event, %{duration: _, size: 1},
                      %{buffer: ^buff, partition: _}},
                     @default_timeout

      assert_receive {:process_completed,
                      [%Entry{key: :key1, value: "value1", version: version, updates: 0}]},
                     @default_timeout

      assert is_integer(version) and version > 0
    end

    test "ok: newer version wins", %{buffer: buff} do
      assert HashMap.put_newer(buff, :key1, "v1", version: 100) == :ok
      assert HashMap.get(buff, :key1) == "v1"

      # Newer version should overwrite
      assert HashMap.put_newer(buff, :key1, "v2", version: 200) == :ok
      assert HashMap.get(buff, :key1) == "v2"

      assert HashMap.size(buff) == 1

      assert_receive {@processing_stop_event, %{duration: _, size: 1},
                      %{buffer: ^buff, partition: _}},
                     @default_timeout

      assert_receive {:process_completed,
                      [%Entry{key: :key1, value: "v2", version: 200, updates: 1}]},
                     @default_timeout
    end

    test "ok: older version is ignored", %{buffer: buff} do
      assert HashMap.put_newer(buff, :key1, "v1", version: 200) == :ok
      assert HashMap.get(buff, :key1) == "v1"

      # Older version should be ignored
      assert HashMap.put_newer(buff, :key1, "v2", version: 100) == :ok
      assert HashMap.get(buff, :key1) == "v1"

      assert HashMap.size(buff) == 1

      assert_receive {@processing_stop_event, %{duration: _, size: 1},
                      %{buffer: ^buff, partition: _}},
                     @default_timeout

      assert_receive {:process_completed,
                      [%Entry{key: :key1, value: "v1", version: 200, updates: 0}]},
                     @default_timeout
    end

    test "ok: same version is ignored", %{buffer: buff} do
      assert HashMap.put_newer(buff, :key1, "v1", version: 100) == :ok
      assert HashMap.get(buff, :key1) == "v1"

      # Same version should be ignored
      assert HashMap.put_newer(buff, :key1, "v2", version: 100) == :ok
      assert HashMap.get(buff, :key1) == "v1"

      assert HashMap.size(buff) == 1

      assert_receive {@processing_stop_event, %{duration: _, size: 1},
                      %{buffer: ^buff, partition: _}},
                     @default_timeout

      assert_receive {:process_completed,
                      [%Entry{key: :key1, value: "v1", version: 100, updates: 0}]},
                     @default_timeout
    end

    test "ok: put_all_newer with multiple entries", %{buffer: buff} do
      entries = [
        {:a, "val_a", 100},
        {:b, "val_b", 200},
        {:c, "val_c", 300}
      ]

      assert HashMap.put_all_newer(buff, entries) == :ok

      assert HashMap.size(buff) == 3
      assert HashMap.get(buff, :a) == "val_a"
      assert HashMap.get(buff, :b) == "val_b"
      assert HashMap.get(buff, :c) == "val_c"

      assert_receive {@processing_stop_event, %{duration: _, size: 3},
                      %{buffer: ^buff, partition: _}},
                     @default_timeout

      assert_receive {:process_completed, batch}, @default_timeout

      assert Enum.sort_by(batch, & &1.key) == [
               %Entry{key: :a, value: "val_a", version: 100, updates: 0},
               %Entry{key: :b, value: "val_b", version: 200, updates: 0},
               %Entry{key: :c, value: "val_c", version: 300, updates: 0}
             ]
    end

    test "ok: put_all_newer respects version ordering", %{buffer: buff} do
      # Insert initial values
      initial = [
        {:a, "a1", 100},
        {:b, "b1", 200}
      ]

      assert HashMap.put_all_newer(buff, initial) == :ok

      # Update with mixed versions
      updates = [
        {:a, "a2", 200},
        {:b, "b2", 100}
      ]

      assert HashMap.put_all_newer(buff, updates) == :ok

      # :a should be updated (200 > 100), :b should not (100 < 200)
      assert HashMap.get(buff, :a) == "a2"
      assert HashMap.get(buff, :b) == "b1"

      assert_receive {@processing_stop_event, %{duration: _, size: 2},
                      %{buffer: ^buff, partition: _}},
                     @default_timeout

      assert_receive {:process_completed, batch}, @default_timeout

      assert Enum.sort_by(batch, & &1.key) == [
               %Entry{key: :a, value: "a2", version: 200, updates: 1},
               %Entry{key: :b, value: "b1", version: 200, updates: 0}
             ]
    end

    test "ok: works with timestamp versions", %{buffer: buff} do
      t1 = System.monotonic_time()
      assert HashMap.put_newer(buff, :event, %{data: "first"}, version: t1) == :ok

      t2 = System.monotonic_time()
      assert HashMap.put_newer(buff, :event, %{data: "second"}, version: t2) == :ok

      # t2 > t1, so second should win
      assert HashMap.get(buff, :event) == %{data: "second"}

      assert_receive {@processing_stop_event, %{duration: _, size: 1},
                      %{buffer: ^buff, partition: _}},
                     @default_timeout
    end

    test "ok: updates existing entry with tuple key and higher version", %{buffer: buff} do
      assert HashMap.put_newer(buff, {1, "key1"}, {"value0"}, version: 100) == :ok
      assert HashMap.put_newer(buff, {1, "key1"}, {"value1"}, version: 200) == :ok

      assert HashMap.size(buff) == 1
      assert HashMap.get(buff, {1, "key1"}) == {"value1"}

      assert_receive {@processing_stop_event, %{duration: _, size: 1},
                      %{buffer: ^buff, partition: _}},
                     @default_timeout

      assert_receive {:process_completed,
                      [%Entry{key: {1, "key1"}, value: {"value1"}, version: 200, updates: 1}]},
                     @default_timeout
    end

    test "ok: updates existing entry with list key and list value containing tuples",
         %{buffer: buff} do
      key = ["account1", "user1", "event"]
      value0 = [{1, "old"}, {2, "data"}]
      value1 = [{1, "new"}, {3, "updated"}]

      assert HashMap.put_newer(buff, key, value0, version: 100) == :ok
      assert HashMap.put_newer(buff, key, value1, version: 200) == :ok

      assert HashMap.size(buff) == 1
      assert HashMap.get(buff, key) == value1

      assert_receive {@processing_stop_event, %{duration: _, size: 1},
                      %{buffer: ^buff, partition: _}},
                     @default_timeout

      assert_receive {:process_completed,
                      [%Entry{key: ^key, value: ^value1, version: 200, updates: 1}]},
                     @default_timeout
    end

    test "ok: updates existing entry with nested map value", %{buffer: buff} do
      key = :nested_map

      value0 = %{
        users: %{admin: %{name: "alice", roles: [:admin, :user]}},
        meta: %{nested: %{deep: %{level: 3}}}
      }

      value1 = %{
        users: %{admin: %{name: {:x, "bob"}, roles: [:user]}},
        meta: %{nested: %{deep: %{level: 5, extra: true}}}
      }

      assert HashMap.put_newer(buff, key, value0, version: 100) == :ok
      assert HashMap.put_newer(buff, key, value1, version: 200) == :ok

      assert HashMap.size(buff) == 1
      assert HashMap.get(buff, key) == value1

      assert_receive {@processing_stop_event, %{duration: _, size: 1},
                      %{buffer: ^buff, partition: _}},
                     @default_timeout

      assert_receive {:process_completed,
                      [%Entry{key: ^key, value: ^value1, version: 200, updates: 1}]},
                     @default_timeout
    end

    test "ok: updates existing entry with tuple value containing maps", %{buffer: buff} do
      value0 = {:ok, %{a: 1, b: %{c: [1, 2, %{d: 3}]}}}
      value1 = {:ok, %{a: 2, b: %{c: [3, 4, %{d: 5}]}}}

      assert HashMap.put_newer(buff, :tuple_map, value0, version: 100) == :ok
      assert HashMap.put_newer(buff, :tuple_map, value1, version: 200) == :ok

      assert HashMap.size(buff) == 1
      assert HashMap.get(buff, :tuple_map) == value1

      assert_receive {@processing_stop_event, %{duration: _, size: 1},
                      %{buffer: ^buff, partition: _}},
                     @default_timeout

      assert_receive {:process_completed,
                      [%Entry{key: :tuple_map, value: ^value1, version: 200, updates: 1}]},
                     @default_timeout
    end

    test "ok: accepts binary and atom versions (compared via Erlang term order)",
         %{buffer: buff} do
      assert HashMap.put_newer(buff, :str_key, "v1", version: "01HXYZ-001") == :ok
      assert HashMap.put_newer(buff, :str_key, "v2", version: "01HXYZ-002") == :ok
      assert HashMap.put_newer(buff, :str_key, "v_old", version: "01HXYZ-000") == :ok

      assert HashMap.put_newer(buff, :atom_key, "v1", version: :a) == :ok
      assert HashMap.put_newer(buff, :atom_key, "v2", version: :b) == :ok

      assert HashMap.size(buff) == 2
      assert HashMap.get(buff, :str_key) == "v2"
      assert HashMap.get(buff, :atom_key) == "v2"
    end

    test "error: put_newer raises NimbleOptions.ValidationError for unsupported version type",
         %{buffer: buff} do
      assert_raise NimbleOptions.ValidationError, ~r/:version option/, fn ->
        HashMap.put_newer(buff, :key1, "value1", version: {1, 5})
      end
    end

    test "error: put_newer raises NimbleOptions.ValidationError for invalid :key_hasher",
         %{buffer: buff} do
      assert_raise NimbleOptions.ValidationError, ~r/:key_hasher option/, fn ->
        HashMap.put_newer(buff, :key1, "value1", key_hasher: :not_a_function)
      end
    end

    test "error: put_all_newer raises ArgumentError for invalid entry",
         %{buffer: buff} do
      entries = [
        {:a, "val_a", 100},
        # Tuple version is not supported.
        {:b, "val_b", {1, 5}}
      ]

      assert_raise ArgumentError, ~r/invalid entry/, fn ->
        HashMap.put_all_newer(buff, entries)
      end
    end
  end

  describe "key_hasher option" do
    setup do
      self = self()

      pid =
        start_supervised!(
          {HashMap,
           name: __MODULE__,
           processing_interval: 500,
           processing_batch_size: 10,
           partitions: 1,
           processor: &__MODULE__.test_processor(self, &1)}
        )

      {:ok, buffer: __MODULE__, pid: pid}
    end

    test "ok: put + get + delete round-trip with key_hasher: true", %{buffer: buff} do
      key = %{tenant: "acme", id: 42}

      assert HashMap.put(buff, key, "v1", key_hasher: true) == :ok
      assert HashMap.get(buff, key, nil, key_hasher: true) == "v1"
      assert HashMap.delete(buff, key, key_hasher: true) == :ok
      assert HashMap.get(buff, key, nil, key_hasher: true) == nil
    end

    test "ok: put_newer round-trip with key_hasher: true and map keys", %{buffer: buff} do
      key = %{tenant: "acme", id: 42}

      assert HashMap.put_newer(buff, key, "v1", version: 100, key_hasher: true) == :ok
      assert HashMap.put_newer(buff, key, "v2", version: 200, key_hasher: true) == :ok
      # Older version is rejected by the match-spec guard
      assert HashMap.put_newer(buff, key, "v_old", version: 50, key_hasher: true) == :ok

      assert HashMap.size(buff) == 1
      assert HashMap.get(buff, key, nil, key_hasher: true) == "v2"
    end

    test "ok: put_all_newer with key_hasher: true", %{buffer: buff} do
      hasher = true
      key1 = %{tenant: "acme", id: 1}
      key2 = %{tenant: "acme", id: 2}

      assert HashMap.put_all_newer(
               buff,
               [{key1, "v1", 100}, {key2, "v2", 200}],
               key_hasher: hasher
             ) == :ok

      assert HashMap.size(buff) == 2
      assert HashMap.get(buff, key1, nil, key_hasher: hasher) == "v1"
      assert HashMap.get(buff, key2, nil, key_hasher: hasher) == "v2"
    end

    test "ok: processor receives the ORIGINAL key, not the hash", %{buffer: buff} do
      key = %{tenant: "acme", id: 42}

      assert HashMap.put_newer(buff, key, "v1", version: 100, key_hasher: true) == :ok

      assert_receive {@processing_stop_event, %{duration: _, size: 1},
                      %{buffer: ^buff, partition: _}},
                     @default_timeout

      # Entry.key is the ORIGINAL map, not phash2(map)
      assert_receive {:process_completed,
                      [%Entry{key: ^key, value: "v1", version: 100, updates: 0}]},
                     @default_timeout
    end

    test "ok: lambda hasher (sha256 over term_to_binary)", %{buffer: buff} do
      hasher = fn k -> :crypto.hash(:sha256, :erlang.term_to_binary(k)) end
      key = %{complex: %{nested: :term}}

      assert HashMap.put(buff, key, "v1", key_hasher: hasher) == :ok
      assert HashMap.get(buff, key, nil, key_hasher: hasher) == "v1"

      assert_receive {:process_completed, [%Entry{key: ^key, value: "v1", version: 0, updates: 0}]},
                     @default_timeout
    end

    test "ok: non-hashed and hashed entries coexist in the same partition", %{buffer: buff} do
      # Without key_hasher: stored under the original key, raw_key is nil
      assert HashMap.put(buff, :plain, "v_plain") == :ok
      # With key_hasher: stored under the hash, raw_key holds the original
      assert HashMap.put(buff, %{m: 1}, "v_map", key_hasher: true) == :ok

      assert HashMap.size(buff) == 2
      assert HashMap.get(buff, :plain) == "v_plain"
      assert HashMap.get(buff, %{m: 1}, nil, key_hasher: true) == "v_map"

      # Processor should see both with their ORIGINAL keys
      assert_receive {:process_completed, batch}, @default_timeout
      keys = batch |> Enum.map(& &1.key) |> Enum.sort_by(&inspect/1)
      assert keys == [:plain, %{m: 1}] |> Enum.sort_by(&inspect/1)
    end
  end

  describe "processing" do
    setup do
      self = self()

      pid =
        start_supervised!(
          {HashMap,
           name: __MODULE__,
           processing_interval: 10,
           processing_batch_size: 5,
           partitions: 1,
           processor: &__MODULE__.test_processor(self, &1)}
        )

      {:ok, buffer: __MODULE__, pid: pid}
    end

    test "error: task fails", %{buffer: buff} do
      :ok = HashMap.put(buff, :err, %{error: true})

      # Wait for the processing task to fail and emit the event
      assert_receive {@processing_failed_event, %{system_time: _},
                      %{buffer: ^buff, partition: _, reason: {%RuntimeError{}, _stacktrace}}},
                     @default_timeout
    end

    test "error: task exits", %{buffer: buff} do
      Process.flag(:trap_exit, true)

      :ok = HashMap.put(buff, :exit_key, %{exit: :exit})

      # Wait for the processing task to exit and emit the event
      assert_receive {@processing_failed_event, %{system_time: _},
                      %{buffer: ^buff, partition: _, reason: :exit}},
                     @default_timeout
    end

    test "shutdown: partition handles supervisor kill", %{pid: pid} do
      Process.flag(:trap_exit, true)

      assert Supervisor.stop(pid, :kill) == :ok

      # Verify the partition was stopped and emitted the telemetry event
      assert_receive {@partition_stop_event, %{duration: _},
                      %{buffer: _, partition: _, reason: :shutdown}},
                     @default_timeout
    end

    test "shutdown: entries are processed before dying", %{buffer: buff, pid: pid} do
      # Wait for the processing interval (no entries were written yet)
      :ok = Process.sleep(100)

      # Put an entry with custom sleep time
      :ok = HashMap.put(buff, :slow, %{sleep_ms: 500, data: "first"})

      # Make sure the processing started
      assert_receive {@processing_start_event, %{}, %{buffer: ^buff, partition: _}},
                     @default_timeout

      # Wait for the next processing interval
      :ok = Process.sleep(100)

      # Put another entry while processing is running
      :ok = HashMap.put(buff, :fast, %{id: 2, data: "second"})

      # Stop the buffer normally
      assert Supervisor.stop(pid) == :ok

      # Verify the partition was stopped and emitted the telemetry event
      assert_receive {@partition_stop_event, %{duration: _},
                      %{buffer: ^buff, partition: _, reason: :shutdown}},
                     @default_timeout

      # Verify the entries were processed
      # (HashMap returns %Entry{} structs in the batch)
      assert_receive {:process_completed,
                      [
                        %Entry{
                          key: :slow,
                          value: %{sleep_ms: 500, data: "first"},
                          version: 0,
                          updates: 0
                        }
                      ]},
                     @default_timeout

      assert_receive {:process_completed,
                      [
                        %Entry{
                          key: :fast,
                          value: %{id: 2, data: "second"},
                          version: 0,
                          updates: 0
                        }
                      ]},
                     @default_timeout
    end
  end

  describe "processor as MFA" do
    setup do
      self = self()

      start_supervised!(
        {HashMap,
         name: :hm_mfa_test,
         processing_interval: 500,
         processing_batch_size: 5,
         processor: {__MODULE__, :mfa_processor, [self]},
         partitions: 1}
      )

      {:ok, buffer: :hm_mfa_test}
    end

    test "ok: processes entries using MFA processor", %{buffer: buff} do
      assert HashMap.put(buff, :key1, "value1") == :ok

      assert_receive {@processing_stop_event, %{duration: _, size: 1},
                      %{buffer: ^buff, partition: _}},
                     @default_timeout

      assert_receive {:process_completed,
                      [%Entry{key: :key1, value: "value1", version: 0, updates: 0}]},
                     @default_timeout
    end
  end

  describe "update_options/2" do
    setup do
      self = self()

      start_supervised!(
        {HashMap, name: __MODULE__, processor: &__MODULE__.test_processor(self, &1), partitions: 1}
      )

      {:ok, buffer: __MODULE__}
    end

    test "ok: updates the given options", %{buffer: buff} do
      # Lower the interval so processing triggers
      assert HashMap.update_options(buff,
               processing_interval: 200,
               processing_batch_size: 2
             ) == :ok

      # Insert 4 entries
      assert HashMap.put_all(buff, [{:a, 1}, {:b, 2}, {:c, 3}, {:d, 4}]) == :ok

      # Wait for processing to complete (after 200ms)
      assert_receive {@processing_stop_event, %{duration: _, size: 4},
                      %{buffer: ^buff, partition: _}},
                     @default_timeout

      # Should receive 2 batches of 2 (batch_size is now 2)
      assert_receive {:process_completed, batch1}, @default_timeout
      assert_receive {:process_completed, batch2}, @default_timeout
      assert length(batch1) == 2
      assert length(batch2) == 2
    end

    test "error: raises on invalid options", %{buffer: buff} do
      assert_raise NimbleOptions.ValidationError, fn ->
        HashMap.update_options(buff, processing_interval: -1)
      end
    end
  end

  describe "processing_batch_size: :table" do
    setup do
      self = self()

      pid =
        start_supervised!(
          {HashMap,
           name: :hm_table_test,
           processing_interval: 500,
           processing_batch_size: :table,
           processor: &__MODULE__.table_processor(self, &1),
           partitions: 1}
        )

      {:ok, buffer: :hm_table_test, pid: pid}
    end

    test "ok: processor receives the ETS table", %{buffer: buff} do
      assert HashMap.put_all(buff, [{:a, 1}, {:b, 2}, {:c, 3}]) == :ok

      assert_receive {@processing_stop_event, %{duration: _, size: 3},
                      %{buffer: ^buff, partition: _}},
                     @default_timeout

      # The processor reads the table from inside the task (while it's
      # still owned/alive) and reports back the type + size it observed.
      assert_receive {:table_processed, table_type, 3}, @default_timeout
      assert table_type == :set
    end

    test "ok: processor can take ownership and keep the table", %{buffer: buff} do
      assert HashMap.put_all(buff, [{:x, 10}, {:y, 20}]) == :ok

      assert_receive {@processing_stop_event, %{duration: _, size: 2},
                      %{buffer: ^buff, partition: _}},
                     @default_timeout

      assert_receive {:table_kept, kept_table}, @default_timeout

      # In :table mode the buffer does NOT delete the table. The processor
      # gave it away to this test process, so it survives the task exit.
      assert :ets.info(kept_table, :size) == 2

      # Clean up
      :ets.delete(kept_table)
    end
  end

  describe "partitioning" do
    setup do
      self = self()

      start_supervised!(
        {HashMap,
         name: :hm_partitioning_test,
         processing_interval: 500,
         processing_batch_size: 10,
         partitions: 2,
         processor: &__MODULE__.test_processor(self, &1)}
      )

      {:ok, buffer: :hm_partitioning_test}
    end

    test "ok: all entries are processed across partitions", %{buffer: buff} do
      entries = Enum.map(1..10, fn i -> {:"key_#{i}", %{id: i, data: "val#{i}"}} end)

      # Put entries
      :ok = HashMap.put_all(buff, entries)

      # Make sure processing completed on both partitions
      assert_receive {@processing_stop_event, %{duration: _, size: s1},
                      %{buffer: :hm_partitioning_test, partition: p1}},
                     @default_timeout

      assert_receive {@processing_stop_event, %{duration: _, size: s2},
                      %{buffer: :hm_partitioning_test, partition: p2}},
                     @default_timeout

      # Check the total processed entries
      assert s1 + s2 == 10

      # Entries should have been routed to different partitions
      assert p1 != p2
    end
  end

  describe ":drain_threshold (early drain — shared with Queue)" do
    test "ok: drains early when the partition reaches drain_threshold" do
      # Parity check (R5): the size-trigger lives in the shared partition, so
      # it must behave identically for HashMap. Long processing_interval
      # isolates the size trigger from the timer.
      self = self()

      start_supervised!(
        {HashMap,
         name: :hm_drain,
         processing_interval: 60_000,
         processing_batch_size: 100,
         partitions: 1,
         drain_threshold: 5,
         drain_check_interval: 50,
         processor: &__MODULE__.test_processor(self, &1)},
        id: :hm_drain
      )

      for i <- 1..5, do: :ok = HashMap.put(:hm_drain, {:k, i}, i)

      assert_receive {:process_completed, batch}, @default_timeout
      assert length(batch) == 5
    end
  end

  ## Helpers

  # HashMap processor receives %Entry{} structs in the batch.
  def test_processor(_pid, [%Entry{value: %{error: true}} | _]) do
    raise "task error"
  end

  def test_processor(_pid, [%Entry{value: %{exit: reason}} | _]) do
    exit(reason)
  end

  def test_processor(pid, [%Entry{value: %{sleep_ms: time_ms}} | _] = chunk) do
    :ok = Process.sleep(time_ms)

    send(pid, {:process_completed, chunk})
  end

  def test_processor(pid, chunk) do
    # Simulate processing time
    :ok = Process.sleep(200)

    send(pid, {:process_completed, chunk})
  end

  # MFA-compatible processor (batch is prepended to args)
  def mfa_processor(chunk, pid) do
    :ok = Process.sleep(200)

    send(pid, {:process_completed, chunk})
  end

  # Table mode processor: inspects the table from inside the task
  # (while the table is still owned/alive) and sends results back.
  # On the "keep" path, give_away the table to the test process so it
  # survives the task exit — in :table mode the buffer itself does not
  # delete the table, but the task's exit would.
  def table_processor(pid, table) do
    size = :ets.info(table, :size)

    if size > 2 do
      type = :ets.info(table, :type)
      send(pid, {:table_processed, type, size})
    else
      if Process.alive?(pid) do
        true = :ets.give_away(table, pid, :kept)
        send(pid, {:table_kept, table})
      end
    end
  end

  def partition_key_fun(key) do
    key
  end

  defp attach_telemetry_handler(handler_id \\ self(), events) do
    :ok =
      :telemetry.attach_many(
        handler_id,
        events,
        &__MODULE__.handle_event/4,
        %{pid: self()}
      )

    handler_id
  end

  def handle_event(event, measurements, metadata, %{pid: pid}) do
    send(pid, {event, measurements, metadata})
  end
end
