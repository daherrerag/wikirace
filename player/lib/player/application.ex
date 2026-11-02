defmodule Player.Application do
  @moduledoc """
  Starts the application.
  Status table - Finch pool for Req,
  and the supervisor that holds one coordinator per race.
  """

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      Player.StatusTable,
      {Finch, name: Player.Finch, pools: %{default: [size: 25, count: 8]}},
      {DynamicSupervisor, name: Player.RaceSupervisor, strategy: :one_for_one}
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: Player.Supervisor)
  end
end
