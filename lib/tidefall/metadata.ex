defmodule Tidefall.Metadata do
  @moduledoc false

  use GenServer

  ## API

  @doc """
  Starts the metadata server.

  ## Options

    * `:name` (atom, default `Tidefall.Metadata`) — registered
      name for both the GenServer and the underlying ETS table.
      Use a custom name to run multiple instances in parallel
      (typically in tests).

  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)

    GenServer.start_link(__MODULE__, name, name: name)
  end

  @doc """
  Stores `value` under `key` in the metadata table.

  Atomic and unconditional — replaces any previous value.
  Similar to `:persistent_term.put/2`.
  """
  @spec put(atom(), any(), any()) :: :ok
  def put(table \\ __MODULE__, key, value) do
    true = :ets.insert(table, {key, value})

    :ok
  end

  @doc """
  Fetches the value for `key` from the metadata table.

  Raises `RuntimeError` if the key has no entry. Similar to
  `:persistent_term.get/1`.
  """
  @spec get(atom(), any()) :: any()
  def get(table \\ __MODULE__, key) do
    case :ets.lookup(table, key) do
      [{^key, value}] ->
        value

      [] ->
        raise "unable to find metadata entry for key #{inspect(key)} " <>
                "in table #{inspect(table)}. Either the key is invalid " <>
                "or the entry has not been set, possibly because it was " <>
                "never put or has been deleted"
    end
  end

  @doc """
  Deletes the entry for `key` from the metadata table.

  No-op if the key has no entry. Similar to
  `:persistent_term.erase/1`.
  """
  @spec delete(atom(), any()) :: :ok
  def delete(table \\ __MODULE__, key) do
    true = :ets.delete(table, key)

    :ok
  end

  ## GenServer callbacks

  @impl true
  def init(name) do
    _table =
      :ets.new(name, [
        :set,
        :named_table,
        :public,
        read_concurrency: true,
        write_concurrency: true,
        decentralized_counters: true
      ])

    {:ok, name}
  end
end
