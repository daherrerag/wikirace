defmodule Player.StatusTable do
  @moduledoc "Owns the public ETS table that `status/1` reads, so it never waits on the crawl."
  use GenServer

  def start_link(_), do: GenServer.start_link(__MODULE__, nil, name: __MODULE__)

  @impl true
  def init(nil) do
    :ets.new(:player_status, [:named_table, :public, :set, read_concurrency: true])
    {:ok, nil}
  end
end
