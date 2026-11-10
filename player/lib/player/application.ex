defmodule Player.Application do
  @moduledoc """
  Starts the application.

  Add your supervisors to `children`. The list starts empty, so no crawler runs until
  you add one.
  """

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      {Player.TaskSupervisor, []},
      {Player.CrawlerSupervisor, []},
      {Finch, name: Player.Finch, pools: %{default: [size: 25, count: 8]}},
      {Player.Infrastructure.Supervisor, []}
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: Player.Supervisor)
  end
end
