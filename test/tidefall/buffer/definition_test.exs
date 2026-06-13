defmodule Tidefall.Buffer.DefinitionTest do
  use ExUnit.Case, async: false

  alias Tidefall.Buffer.Definition

  # Omit logs during the tests
  @moduletag capture_log: true

  # Default waiting time when receiving a message (e.g., assert_receive ...)
  @default_timeout :timer.seconds(5)

  # Telemetry events
  @processing_stop_event [:tidefall, :partition, :processing, :stop]

  ## Definition modules under test

  defmodule EventQueue do
    use Tidefall.Queue, otp_app: :tidefall
  end

  defmodule StateMap do
    use Tidefall.HashMap, otp_app: :tidefall
  end

  # A definition module with a concrete compile-time opt, to pin the
  # lowest precedence layer (compile opts beaten by env).
  defmodule CompiledMap do
    use Tidefall.HashMap, otp_app: :tidefall, partitions: 2
  end

  setup do
    handler_id = attach_telemetry_handler([@processing_stop_event])

    on_exit(fn -> :telemetry.detach(handler_id) end)

    :ok
  end

  describe "defined Queue" do
    setup do
      self = self()

      start_supervised!(
        {EventQueue,
         processing_interval: 100,
         processing_batch_size: 10,
         partitions: 1,
         processor: &send(self, {:batch, &1})}
      )

      :ok
    end

    test "ok: push -> processor receives the batch; name defaults to module" do
      assert EventQueue.size() == 0

      assert EventQueue.push(%{id: 1}) == :ok
      assert EventQueue.push([%{id: 2}, %{id: 3}]) == :ok

      assert_receive {@processing_stop_event, %{size: 3}, %{buffer: EventQueue}},
                     @default_timeout

      assert_receive {:batch, batch}, @default_timeout
      assert length(batch) == 3
    end

    test "ok: nameless push with runtime opts routes correctly (no misroute)" do
      # The full-arity-vs-nameless scheme means this binds %{id: 1} as the
      # item and the keyword list as opts — NOT the item as a name.
      assert EventQueue.push(%{id: 1}, partition_key: 1) == :ok

      assert_receive {@processing_stop_event, %{size: 1}, %{buffer: EventQueue}},
                     @default_timeout
    end
  end

  describe "defined HashMap" do
    setup do
      self = self()

      start_supervised!(
        {StateMap,
         processing_interval: 100,
         processing_batch_size: 10,
         partitions: 1,
         processor: &send(self, {:batch, &1})}
      )

      :ok
    end

    test "ok: put/get/delete/size on the default instance" do
      assert StateMap.put(:k1, "v1") == :ok
      assert StateMap.put(:k2, "v2", partition_key: 1) == :ok
      assert StateMap.put_all(%{k3: "v3"}) == :ok
      assert StateMap.put_newer(:k4, "v4", version: 100) == :ok
      assert StateMap.put_all_newer([{:k5, "v5", 1}]) == :ok

      assert StateMap.get(:k1) == "v1"
      assert StateMap.get(:missing, :default) == :default
      assert StateMap.size() == 5

      assert StateMap.delete(:k1) == :ok
      assert StateMap.get(:k1) == nil

      assert_receive {:batch, _batch}, @default_timeout
    end

    test "ok: put -> processor receives Entry batch" do
      assert StateMap.put(:k1, "v1") == :ok

      assert_receive {@processing_stop_event, %{size: 1}, %{buffer: StateMap}},
                     @default_timeout

      assert_receive {:batch, [%Tidefall.HashMap.Entry{key: :k1, value: "v1"}]},
                     @default_timeout
    end
  end

  describe "name override at start_link" do
    test "ok: an explicit name wins over the default module name" do
      self = self()

      start_supervised!(
        {StateMap,
         name: :custom_name,
         processing_interval: 100,
         partitions: 1,
         processor: &send(self, {:batch, &1})}
      )

      assert StateMap.put(:custom_name, :k, "v", []) == :ok
      assert StateMap.get(:custom_name, :k, nil, []) == "v"
    end
  end

  describe "dynamic instances" do
    setup do
      self = self()

      start_supervised!(
        {StateMap, processing_interval: 100, partitions: 1, processor: &send(self, {:batch, &1})}
      )

      start_supervised!(
        {StateMap,
         name: :tenant_a,
         processing_interval: 100,
         partitions: 1,
         processor: &send(self, {:batch, &1})},
        id: :tenant_a
      )

      :ok
    end

    test "ok: full-arity calls route to the named instance; default unaffected" do
      assert StateMap.put(:tenant_a, :k, "tenant_val", []) == :ok

      assert StateMap.get(:tenant_a, :k, nil, []) == "tenant_val"

      # The default instance is a distinct buffer — it never saw the write.
      assert StateMap.get(:k) == nil
    end
  end

  describe "HashMap generated arity coverage" do
    setup do
      self = self()

      start_supervised!(
        {StateMap, processing_interval: 5_000, partitions: 1, processor: &send(self, {:batch, &1})}
      )

      :ok
    end

    test "ok: nameless variants with trailing opts route the key correctly (no misroute)" do
      # put/3, get/3, put_all/2, delete/2 are nameless-with-opts: the first
      # arg is the key/entries, not an instance name.
      assert StateMap.put(:k, "v", partition_key: 1) == :ok
      assert StateMap.get(:k, nil, partition_key: 1) == "v"
      assert StateMap.put_all(%{a: "1"}, partition_key: 1) == :ok
      assert StateMap.delete(:k, partition_key: 1) == :ok
      assert StateMap.get(:k, nil, partition_key: 1) == nil
    end

    test "ok: nameful variants delegate to a named instance" do
      self = self()

      start_supervised!(
        {StateMap, name: :cov, partitions: 1, processor: &send(self, {:batch, &1})},
        id: :cov
      )

      assert StateMap.put(:cov, :a, "1", []) == :ok
      assert StateMap.put_newer(:cov, :b, "1", version: 1) == :ok
      assert StateMap.put_all(:cov, %{c: "1"}, []) == :ok
      assert StateMap.put_all_newer(:cov, [{:d, "1", 1}], []) == :ok
      assert StateMap.get(:cov, :a, nil, []) == "1"
      assert StateMap.size(:cov) == 4
      assert StateMap.update_options(:cov, processing_interval: 50) == :ok
      assert StateMap.delete(:cov, :a, []) == :ok
      assert StateMap.get(:cov, :a, nil, []) == nil
    end

    test "documented sharp edge: a 2-arg named-style get reads the DEFAULT instance" do
      # `get/2` is the nameless `get(key, default)` form — NOT `get(name, key)`.
      # So this reads the default instance for key `:not_a_buffer` and returns
      # the `:sentinel` default. Pins the requirement that a named instance
      # must be addressed via the full arity (`get(name, key, default, opts)`).
      assert StateMap.get(:not_a_buffer, :sentinel) == :sentinel
    end
  end

  describe "Queue generated arity coverage" do
    test "ok: nameful push/3, size/1, update_options/2, stop/3 delegate to a named instance" do
      self = self()

      start_supervised!(
        {EventQueue,
         name: :covq, processing_interval: 5_000, partitions: 1, processor: &send(self, {:b, &1})},
        id: :covq
      )

      assert EventQueue.push(:covq, "x", []) == :ok
      assert EventQueue.size(:covq) == 1
      assert EventQueue.update_options(:covq, processing_interval: 50) == :ok
      assert EventQueue.stop(:covq, :normal, :infinity) == :ok
    end

    test "ok: nameless stop/1 stops the default instance" do
      self = self()

      start_supervised!({EventQueue, partitions: 1, processor: &send(self, {:b, &1})})

      assert EventQueue.stop(:normal) == :ok
    end
  end

  describe "config precedence" do
    test "ok: each layer beats the one below" do
      # compile-time `use` opts are the lowest layer: CompiledMap declares
      # `partitions: 2`, which resolves through with no env and no explicit.
      assert Definition.resolve_opts(CompiledMap, [])[:partitions] == 2

      # The env layer beats compile opts...
      Application.put_env(:tidefall, CompiledMap, partitions: 7)
      on_exit(fn -> Application.delete_env(:tidefall, CompiledMap) end)
      assert Definition.resolve_opts(CompiledMap, [])[:partitions] == 7

      # ...and explicit start_link opts beat the env layer.
      assert Definition.resolve_opts(CompiledMap, partitions: 9)[:partitions] == 9
    end

    test "ok: an explicit name always wins over the default module name" do
      resolved = Definition.resolve_opts(StateMap, name: :explicit)

      assert resolved[:name] == :explicit
    end

    test "error: :otp_app is required at compile time" do
      assert_raise ArgumentError, ~r/requires the :otp_app option/, fn ->
        Code.eval_string("""
        defmodule MissingOtpApp do
          use Tidefall.HashMap
        end
        """)
      end
    end

    test "error: an op spec the backend does not export fails the build" do
      assert_raise ArgumentError, ~r/does not export bogus_op/, fn ->
        Definition.define(Tidefall.Queue, [{:bogus_op, 0, 0, 0}], otp_app: :tidefall)
      end
    end

    test "error: :otp_app must be a non-nil atom" do
      assert_raise ArgumentError, ~r/:otp_app option to be an OTP/, fn ->
        Definition.define(Tidefall.Queue, [], otp_app: nil)
      end

      assert_raise ArgumentError, ~r/:otp_app option to be an OTP/, fn ->
        Definition.define(Tidefall.Queue, [], otp_app: "my_app")
      end
    end

    test "error: a backend module that cannot be loaded fails the build" do
      assert_raise ArgumentError, ~r/could not be loaded/, fn ->
        Definition.define(__MODULE__.NoSuchBackend, [{:x, 0, 0, 0}], otp_app: :tidefall)
      end
    end
  end

  describe "child_spec/1" do
    test "ok: ids differ for two named instances and both supervise in one tree" do
      default_spec = StateMap.child_spec([])
      named_spec = StateMap.child_spec(name: :tenant_b)

      assert default_spec.id == StateMap
      assert named_spec.id == :tenant_b
      assert default_spec.id != named_spec.id
      assert default_spec.type == :supervisor

      self = self()

      children = [
        Supervisor.child_spec(
          {StateMap, processing_interval: 100, partitions: 1, processor: &send(self, {:b, &1})},
          []
        ),
        Supervisor.child_spec(
          {StateMap,
           name: :tenant_b,
           processing_interval: 100,
           partitions: 1,
           processor: &send(self, {:b, &1})},
          id: :tenant_b
        )
      ]

      pid =
        start_supervised!(%{
          id: :tree,
          start: {Supervisor, :start_link, [children, [strategy: :one_for_one]]}
        })

      assert is_pid(pid)
      assert StateMap.put(:k, "v") == :ok
      assert StateMap.put(:tenant_b, :k, "v", []) == :ok
    end
  end

  describe "stop/size/update_options variants" do
    test "ok: generated stop/size/update_options work on the default instance" do
      self = self()

      start_supervised!(
        {EventQueue, processing_interval: 5_000, partitions: 1, processor: &send(self, {:b, &1})}
      )

      assert EventQueue.size() == 0
      assert EventQueue.push("a") == :ok
      assert EventQueue.size() == 1

      assert EventQueue.update_options(processing_interval: 50) == :ok

      assert_receive {@processing_stop_event, %{size: 1}, %{buffer: EventQueue}},
                     @default_timeout

      assert EventQueue.stop() == :ok
    end

    test "ok: stop accepts a reason" do
      self = self()

      start_supervised!(
        {EventQueue, name: :stoppable, partitions: 1, processor: &send(self, {:b, &1})},
        id: :stoppable
      )

      assert EventQueue.stop(:stoppable, :normal, :infinity) == :ok
    end
  end

  ## Helpers

  defp attach_telemetry_handler(events) do
    handler_id = self()

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
