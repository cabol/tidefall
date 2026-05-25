defmodule Tidefall.Buffer.Supervisor do
  @moduledoc false

  use Supervisor

  alias Tidefall.Buffer.{Options, Partition}

  ## API

  @doc false
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts \\ []) do
    opts = Options.validate_start_options!(opts)

    name = Keyword.fetch!(opts, :name)
    supervisor_name = Module.concat([name, Supervisor])
    {partitions, opts} = Keyword.pop_lazy(opts, :partitions, fn -> System.schedulers_online() end)

    Supervisor.start_link(__MODULE__, {name, partitions, opts}, name: supervisor_name)
  end

  ## Supervisor callbacks

  @impl true
  def init({name, partitions, opts}) do
    task_supervisor_name = Module.concat([name, TaskSupervisor])

    children = [
      {Task.Supervisor, name: task_supervisor_name},
      {Partition.Supervisor, {name, partitions, opts}}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end
end
