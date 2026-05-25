defmodule Tidefall.Application do
  @moduledoc false

  use Application

  ## Application callbacks

  @impl true
  def start(_type, _args) do
    children = [
      Tidefall.Metadata,
      {Registry, keys: :duplicate, name: Tidefall.Registry, partitions: registry_partitions()}
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: Tidefall.Supervisor)
  end

  ## Internal functions

  defp registry_partitions do
    Application.get_env(:tidefall, :registry_partitions, System.schedulers_online())
  end
end
