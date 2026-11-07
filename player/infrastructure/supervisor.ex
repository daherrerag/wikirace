defmodule Player.Infrastructure.Supervisor do
  @moduledoc """
  Supervises shared infrastructure services.
  """

  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, :ok, Keyword.put(opts, :name, __MODULE__))
  end

  @impl true
  def init(:ok) do
    children = [
      Player.Infrastructure.State
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
