defmodule Tidefall.Buffer do
  @moduledoc """
  Buffer operations and behaviour.

  This module is the public interface for buffer-level concerns:

    * Starting and stopping buffers
    * Total size across partitions
    * Mutating runtime options
    * Locating the partition for a routing key
    * The behaviour contract every buffer implementation must satisfy

  End-user code typically goes through the buffer-type module
  (`Tidefall.Queue`, `Tidefall.HashMap`), which delegates the shared
  operations here.

  ## Start options

  #{Tidefall.Buffer.Options.start_options_docs()}

  ## Runtime options

  The following runtime options are shared by `Tidefall.Queue` and
  `Tidefall.HashMap`:

  #{Tidefall.Buffer.Options.runtime_options_docs()}

  Each buffer type may accept additional runtime options of its own —
  see `Tidefall.Queue` (e.g. `:sort_key`) and `Tidefall.HashMap`
  (e.g. `:key_hasher`) for their full option docs.

  """

  alias Tidefall.Buffer.{Options, Partition}

  @typedoc "Buffer name"
  @type buffer() :: atom()

  ## Callbacks

  @doc """
  Returns the list of options passed verbatim to `:ets.new/2` when
  the partition creates one of its two backing tables.

  The list must include the ETS table type
  (`:set` / `:ordered_set` / `:bag` / `:duplicate_bag`), the
  `:keypos`, and any concurrency / access knobs the impl wants.

  The partition does not augment or rewrite this list; what the
  impl returns is exactly what `:ets.new/2` gets.
  """
  @callback ets_table_opts() :: [atom() | {atom(), any()}]

  @doc """
  Returns the match spec used by the processing task when it drains
  the swapped table via `:ets.select/3`. The spec determines the
  shape of each element handed to the processor.
  """
  @callback ets_match_spec() :: :ets.match_spec()

  ## API

  @doc """
  Starts a new buffer.

  > #### Prefer implementation-specific functions {: .tip}
  >
  > It is recommended to use `Tidefall.Queue.start_link/1` or
  > `Tidefall.HashMap.start_link/1` instead, as they automatically
  > set the `:module` option for you.

  ## Examples

      iex> Tidefall.Buffer.start_link(
      ...>   module: Tidefall.Queue,
      ...>   name: :my_buffer
      ...> )
      {:ok, #PID<0.123.0>}

  > Notice that the `:module` option must be set to `Tidefall.Queue` or
  > `Tidefall.HashMap`.

  """
  @spec start_link(keyword()) :: Supervisor.on_start()
  defdelegate start_link(opts), to: Tidefall.Buffer.Supervisor

  @doc """
  Stops a buffer gracefully.

  ## Examples

      iex> Tidefall.Buffer.stop(:my_buffer)
      :ok

  """
  @spec stop(buffer() | pid(), reason :: any(), timeout()) :: :ok
  def stop(buffer, reason, timeout)

  def stop(buffer, reason, timeout) when is_atom(buffer) do
    [buffer, Supervisor]
    |> Module.concat()
    |> Supervisor.stop(reason, timeout)
  end

  def stop(buffer, reason, timeout) when is_pid(buffer) do
    Supervisor.stop(buffer, reason, timeout)
  end

  @doc """
  Returns the buffer size (total number of messages across all partitions).

  Exposed as `size/1` on `Tidefall.Queue` and `Tidefall.HashMap` — most
  code calls those rather than `buffer_size/1` directly.

  ## Examples

      iex> Tidefall.Buffer.buffer_size(:my_buffer)
      10

  """
  @spec buffer_size(buffer()) :: non_neg_integer()
  def buffer_size(buffer) do
    buffer
    |> lookup()
    |> Enum.map(&Partition.buffer_size(elem(&1, 1)))
    |> Enum.sum()
  end

  @doc """
  Updates the options for the buffer.

  ## Examples

      iex> Tidefall.Buffer.update_options(:my_buffer, processing_interval: 1000)
      :ok

  > Notice that the options are updated for all partitions of the buffer.
  """
  @spec update_options(buffer(), keyword()) :: :ok
  def update_options(buffer, opts) do
    opts = Options.validate_update_options!(opts)

    buffer
    |> lookup()
    |> Enum.each(&Partition.update_options(elem(&1, 0), opts))
  end

  ## Shared routing helpers (used by Queue, HashMap, etc.)

  @doc """
  Returns the partition based on the given arguments.
  """
  @spec get_partition(buffer(), any(), any()) :: atom()
  def get_partition(buffer, partition_key, object) do
    key = partition_key(partition_key, object)

    case lookup(buffer) do
      [] ->
        raise "no partitions available for buffer #{inspect(buffer)}. " <>
                "The buffer is not running, possibly because it is not " <>
                "started or does not exist"

      partitions ->
        partitions
        |> Enum.at(:erlang.phash2(key, length(partitions)))
        |> elem(1)
    end
  end

  ## Private functions

  @compile inline: [lookup: 1]
  defp lookup(buffer) do
    Registry.lookup(Tidefall.Registry, buffer)
  end

  # Compute the partition key
  defp partition_key(partition_key, object)

  # The partition key is not provided, use the message hash as the key
  defp partition_key(nil, object) do
    :erlang.phash2(object)
  end

  # The partition key is a function, apply it to the message
  defp partition_key(partition_key, object) when is_function(partition_key, 1) do
    partition_key.(object)
  end

  # The partition key is an MFA tuple, apply it (the message is prepended to the args)
  defp partition_key({m, f, a}, object) when is_atom(m) and is_atom(f) and is_list(a) do
    apply(m, f, [object | a])
  end

  # The partition key is a static value, return it
  defp partition_key(partition_key, _object) do
    partition_key
  end
end
