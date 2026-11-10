defmodule Player.Crawler do
  @moduledoc """
  Implementation of the WikiRace crawler contract.
  """

  @behaviour WikiRace.Crawler

  @impl WikiRace.Crawler
  def start(start_url, target_url) do
    race_id = System.unique_integer([:positive, :monotonic])

    args = %{
      race_id: race_id,
      start_url: start_url,
      target_url: target_url
    }

    DynamicSupervisor.start_child(
      Player.CrawlerSupervisor,
      {Player.Search.Coordinator, args}
    )
  end

  @impl WikiRace.Crawler
  def status(pid) do
    Player.Search.Coordinator.status(pid)
  end
end
