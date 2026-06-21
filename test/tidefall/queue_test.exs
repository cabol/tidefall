defmodule Tidefall.QueueTest do
  use ExUnit.Case, async: false

  alias Tidefall.Queue, as: Q

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

  describe "push/3" do
    setup do
      self = self()

      pid =
        start_supervised!(
          {Q,
           name: __MODULE__,
           processing_interval: 500,
           processing_batch_size: 5,
           processor: &__MODULE__.test_processor(self, &1),
           partitions: 1}
        )

      {:ok, buffer: __MODULE__, pid: pid}
    end

    test "ok: messages are processed in ordered batches", %{buffer: buff} do
      assert Q.size(buff) == 0

      {expected_batch1, expected_batch2} =
        Enum.map(1..10, fn i ->
          %{id: i, data: "message#{i}"}
        end)
        |> Enum.split(5)

      # Push messages
      assert Q.push(buff, expected_batch1 ++ expected_batch2) == :ok

      # Make sure the processing completed
      assert_receive {@processing_stop_event, %{duration: _, size: 10},
                      %{buffer: ^buff, partition: _}},
                     @default_timeout

      # Push more messages while processing
      assert Q.push(buff, expected_batch1) == :ok

      # Check the processed messages
      assert_receive {:process_completed, ^expected_batch1}, @default_timeout
      assert_receive {:process_completed, ^expected_batch2}, @default_timeout
      assert_receive {:process_completed, ^expected_batch1}, @default_timeout

      # Push more after processing
      assert Q.push(buff, expected_batch2) == :ok

      # Make sure the processing completed
      assert_receive {@processing_stop_event, %{duration: _, size: 5},
                      %{buffer: ^buff, partition: _}},
                     @default_timeout

      # Check the processed messages
      assert_receive {:process_completed, ^expected_batch2}, @default_timeout

      assert Q.size(buff) == 0
    end

    test "ok: handles single message pushes", %{buffer: buff} do
      msg = %{id: 1, data: "single"}

      assert Q.push(buff, msg) == :ok

      assert Q.size(buff) == 1

      # Wait for processing to complete
      assert_receive {@processing_stop_event, %{duration: _, size: 1},
                      %{buffer: ^buff, partition: _}},
                     @default_timeout

      assert_receive {:process_completed, [^msg]}, @default_timeout
    end

    test "ok: messages are partitioned using a function", %{buffer: buff} do
      assert Q.push(buff, %{id: 1, data: "message1"},
               partition_key: &__MODULE__.partition_key_fun/1
             ) == :ok

      assert Q.size(buff) == 1

      assert_receive {@processing_stop_event, %{duration: _, size: 1},
                      %{buffer: ^buff, partition: _}},
                     @default_timeout
    end

    test "ok: messages are partitioned using an MFA tuple", %{buffer: buff} do
      assert Q.push(buff, %{id: 1, data: "message1"},
               partition_key: {__MODULE__, :partition_key_fun, []}
             ) == :ok

      assert Q.size(buff) == 1

      assert_receive {@processing_stop_event, %{duration: _, size: 1},
                      %{buffer: ^buff, partition: _}},
                     @default_timeout
    end

    test "ok: messages are partitioned using a custom key", %{buffer: buff} do
      assert Q.push(buff, %{id: 1, data: "message1"}, partition_key: 1) ==
               :ok

      assert Q.size(buff) == 1

      assert_receive {@processing_stop_event, %{duration: _, size: 1},
                      %{buffer: ^buff, partition: _}},
                     @default_timeout
    end

    test "error: no partitions available for the given buffer" do
      {:ok, buff} =
        Q.start_link(
          name: :error_buff,
          processor: &__MODULE__.test_processor(self(), &1),
          partitions: 0
        )

      assert_raise RuntimeError, ~r"no partitions available for buffer :error_buff", fn ->
        Q.push(:error_buff, "message")
      end

      assert Q.stop(buff) == :ok
    end
  end

  describe "stop/3" do
    test "ok: stops the buffer" do
      {:ok, _} =
        Q.start_link(
          name: :stop_buff,
          processor: &__MODULE__.test_processor(self(), &1),
          partitions: 1
        )

      assert Q.stop(:stop_buff) == :ok
    end
  end

  describe "processing" do
    setup do
      self = self()

      pid =
        start_supervised!(
          {Q,
           name: __MODULE__,
           processing_interval: 10,
           processing_batch_size: 5,
           partitions: 1,
           processor: &__MODULE__.test_processor(self, &1)}
        )

      {:ok, buffer: __MODULE__, pid: pid}
    end

    test "error: task fails", %{buffer: buff} do
      :ok = Q.push(buff, %{error: true})

      # Wait for the processing task to fail and emit the event
      assert_receive {@processing_failed_event, %{system_time: _},
                      %{buffer: ^buff, partition: _, reason: {%RuntimeError{}, _stacktrace}}},
                     @default_timeout
    end

    test "error: task exits", %{buffer: buff} do
      Process.flag(:trap_exit, true)

      :ok = Q.push(buff, %{exit: :exit})

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

    test "shutdown: messages are processed before dying", %{buffer: buff, pid: pid} do
      # Wait for the processing interval (no messages were written yet)
      :ok = Process.sleep(100)

      # Send a message with custom sleep time
      msg1 = %{sleep_ms: 500, data: "first"}
      :ok = Q.push(buff, msg1)

      # Make sure the processing started
      assert_receive {@processing_start_event, %{}, %{buffer: ^buff, partition: _}},
                     @default_timeout

      # Wait for the next processing interval
      :ok = Process.sleep(100)

      # Send another message while the processing is running
      msg2 = %{id: 2, data: "second"}
      :ok = Q.push(buff, msg2)

      # Stop the buffer normally
      assert Supervisor.stop(pid) == :ok

      # Verify the partition was stopped and emitted the telemetry event
      assert_receive {@partition_stop_event, %{duration: _},
                      %{buffer: ^buff, partition: _, reason: :shutdown}},
                     @default_timeout

      # Verify the messages were processed
      assert_receive {:process_completed, [^msg1]}, @default_timeout
      assert_receive {:process_completed, [^msg2]}, @default_timeout
    end
  end

  describe "processor as MFA" do
    setup do
      self = self()

      start_supervised!(
        {Q,
         name: :queue_mfa_test,
         processing_interval: 500,
         processing_batch_size: 5,
         processor: {__MODULE__, :mfa_processor, [self]},
         partitions: 1}
      )

      {:ok, buffer: :queue_mfa_test}
    end

    test "ok: processes messages using MFA processor", %{buffer: buff} do
      msg = %{id: 1, data: "mfa_test"}

      assert Q.push(buff, msg) == :ok

      assert_receive {@processing_stop_event, %{duration: _, size: 1},
                      %{buffer: ^buff, partition: _}},
                     @default_timeout

      assert_receive {:process_completed, [^msg]}, @default_timeout
    end
  end

  describe "update_options/2" do
    setup do
      self = self()

      start_supervised!(
        {Q, name: __MODULE__, processor: &__MODULE__.test_processor(self, &1), partitions: 1}
      )

      {:ok, buffer: __MODULE__}
    end

    test "ok: updates the given options", %{buffer: buff} do
      # Lower the interval so processing triggers
      assert Q.update_options(buff, processing_interval: 200, processing_batch_size: 2) == :ok

      # Push 4 messages
      msgs = Enum.map(1..4, fn i -> %{id: i, data: "msg#{i}"} end)
      assert Q.push(buff, msgs) == :ok

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
        Q.update_options(buff, processing_interval: -1)
      end
    end
  end

  describe "processing_batch_size: :table" do
    setup do
      self = self()

      pid =
        start_supervised!(
          {Q,
           name: :queue_table_test,
           processing_interval: 500,
           processing_batch_size: :table,
           processor: &__MODULE__.table_processor(self, &1),
           partitions: 1}
        )

      {:ok, buffer: :queue_table_test, pid: pid}
    end

    test "ok: processor receives the ETS table", %{buffer: buff} do
      msgs = [%{id: 1}, %{id: 2}, %{id: 3}]
      assert Q.push(buff, msgs) == :ok

      assert_receive {@processing_stop_event, %{duration: _, size: 3},
                      %{buffer: ^buff, partition: _}},
                     @default_timeout

      # The processor reads the table from inside the task (while it's
      # still owned/alive) and reports back the type + size it observed.
      assert_receive {:table_processed, table_type, 3}, @default_timeout
      assert table_type == :ordered_set
    end

    test "ok: processor can take ownership and keep the table", %{buffer: buff} do
      # Push some messages
      assert Q.push(buff, [%{id: 1}, %{id: 2}]) == :ok

      # Wait for processing to complete
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
        {Q,
         name: :partitioning_test,
         processing_interval: 500,
         processing_batch_size: 10,
         partitions: 2,
         processor: &__MODULE__.test_processor(self, &1)}
      )

      {:ok, buffer: :partitioning_test}
    end

    test "ok: all messages are processed across partitions", %{buffer: buff} do
      batch = Enum.map(1..10, fn i -> %{id: i, data: "msg#{i}"} end)

      # Push messages
      :ok = Q.push(buff, batch)

      # Make sure processing completed on both partitions
      assert_receive {@processing_stop_event, %{duration: _, size: s1},
                      %{buffer: :partitioning_test, partition: p1}},
                     @default_timeout

      assert_receive {@processing_stop_event, %{duration: _, size: s2},
                      %{buffer: :partitioning_test, partition: p2}},
                     @default_timeout

      # Check the total processed messages
      assert s1 + s2 == 10

      # Messages should have been routed to different partitions
      assert p1 != p2
    end
  end

  describe ":sort_key (runtime option)" do
    setup do
      self = self()

      pid =
        start_supervised!(
          {Q,
           name: :sort_key_test,
           processing_interval: 100,
           processing_batch_size: 100,
           partitions: 1,
           processor: &__MODULE__.test_processor(self, &1)}
        )

      {:ok, buffer: :sort_key_test, pid: pid}
    end

    test "ok: orders within a partition by an arity-1 :sort_key (value-derived)", %{buffer: buff} do
      :ok =
        Q.push(buff, [%{priority: 3, id: :c}, %{priority: 1, id: :a}, %{priority: 2, id: :b}],
          sort_key: & &1.priority
        )

      assert_receive {:process_completed, batch}, @default_timeout
      assert Enum.map(batch, & &1.priority) == [1, 2, 3]
    end

    test "ok: applies an arity-0 :sort_key per item (runtime-generated)", %{buffer: buff} do
      # A strictly-decreasing key => items drain in reverse insertion order,
      # proving the arity-0 function is evaluated for each item.
      :ok =
        Q.push(buff, [%{id: 1}, %{id: 2}, %{id: 3}],
          sort_key: fn -> -System.unique_integer([:monotonic]) end
        )

      assert_receive {:process_completed, batch}, @default_timeout
      assert Enum.map(batch, & &1.id) == [3, 2, 1]
    end

    test "no data loss: a colliding :sort_key never overwrites distinct items", %{buffer: buff} do
      # Constant sort term => every item ties on the primary component; `ref`
      # keeps the full key unique so nothing is overwritten (R3). Asserts on
      # the delivered set/count, NOT order (tie order is unspecified).
      :ok = Q.push(buff, Enum.map(1..20, &%{id: &1}), sort_key: fn _ -> :same end)

      assert_receive {:process_completed, batch}, @default_timeout
      assert length(batch) == 20
      assert MapSet.new(batch, & &1.id) == MapSet.new(1..20)
    end

    test "ok: default (no :sort_key) preserves insertion order", %{buffer: buff} do
      :ok = Q.push(buff, Enum.map(1..5, &%{id: &1}))

      assert_receive {:process_completed, batch}, @default_timeout
      assert Enum.map(batch, & &1.id) == [1, 2, 3, 4, 5]
    end

    test "ok: a single-item (non-list) push carries :sort_key through", %{buffer: buff} do
      # Exercises the push(buffer, item, opts) clause that delegates to the
      # list clause — confirms :sort_key is threaded, not dropped.
      assert :ok = Q.push(buff, %{id: 1, priority: 9}, sort_key: & &1.priority)

      assert_receive {:process_completed, [%{id: 1}]}, @default_timeout
    end

    test "error: a raising :sort_key propagates from push (caller-side, no rescue)", %{buffer: buff} do
      assert_raise RuntimeError, "boom", fn ->
        Q.push(buff, %{id: 1}, sort_key: fn _ -> raise "boom" end)
      end
    end

    test "ok: mixed-type sort terms never crash and never lose items", %{buffer: buff} do
      # Erlang total term order spans types (number < atom < bitstring), so
      # mixed terms order deterministically; `ref` keeps every item distinct.
      items = [%{id: 1, k: 7}, %{id: 2, k: :a}, %{id: 3, k: "z"}]
      :ok = Q.push(buff, items, sort_key: & &1.k)

      assert_receive {:process_completed, batch}, @default_timeout
      assert MapSet.new(batch, & &1.id) == MapSet.new([1, 2, 3])
    end
  end

  describe ":sort_key across partitions" do
    setup do
      self = self()

      start_supervised!(
        {Q,
         name: :sort_key_multi,
         processing_interval: 100,
         processing_batch_size: 100,
         partitions: 2,
         processor: &__MODULE__.test_processor(self, &1)}
      )

      {:ok, buffer: :sort_key_multi}
    end

    test "ok: each partition's batch is independently ordered; no item lost", %{buffer: buff} do
      items = Enum.map([3, 1, 5, 2, 6, 4], &%{id: &1, priority: &1})
      :ok = Q.push(buff, items, partition_key: & &1.id, sort_key: & &1.priority)

      # Items spread across both partitions; collect every delivered batch.
      batches = collect_completed(6)

      # No data loss across partitions.
      delivered = batches |> List.flatten() |> MapSet.new(& &1.id)
      assert delivered == MapSet.new(1..6)

      # Ordering is per-partition: each delivered batch is priority-sorted.
      for batch <- batches do
        priorities = Enum.map(batch, & &1.priority)
        assert priorities == Enum.sort(priorities)
      end
    end
  end

  ## Helpers

  # Collect {:process_completed, batch} messages until `count` items are seen,
  # returning the batches in arrival order. Tolerates any partition split.
  defp collect_completed(count), do: collect_completed(count, [])

  defp collect_completed(count, batches) when count <= 0, do: Enum.reverse(batches)

  defp collect_completed(count, batches) do
    receive do
      {:process_completed, batch} -> collect_completed(count - length(batch), [batch | batches])
    after
      @default_timeout -> flunk("timed out; #{count} items still missing")
    end
  end

  def test_processor(_pid, [%{error: true} | _]) do
    raise "task error"
  end

  def test_processor(_pid, [%{exit: reason} | _]) do
    exit(reason)
  end

  def test_processor(pid, [%{sleep_ms: time_ms} | _] = chunk) do
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

  def partition_key_fun(%{id: id}) do
    id
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
