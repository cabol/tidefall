defmodule Tidefall.Buffer.Partition.Supervisor do
  @moduledoc false

  use Supervisor

  alias Tidefall.Buffer.Partition

  ## API

  @doc """
  Starts the partition pool supervisor.
  """
  @spec start_link({atom(), non_neg_integer(), keyword()}) :: Supervisor.on_start()
  def start_link({name, partitions, opts}) do
    Supervisor.start_link(__MODULE__, {name, partitions, opts})
  end

  ## Supervisor callbacks

  @impl true
  def init({name, partitions, opts}) do
    children =
      for idx <- 0..(partitions - 1)//1 do
        Supervisor.child_spec({Partition, [partition_index: idx] ++ opts},
          id: {name, idx}
        )
      end

    Supervisor.init(children, strategy: :one_for_one)
  end
end
