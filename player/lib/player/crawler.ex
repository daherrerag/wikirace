defmodule Player.Crawler do
  @moduledoc """
  The harness calls `start/2` and `status/1` of this module.
  `start/2` launches one coordinator under `Player.RaceSupervisor` and returns at once.
  `status/1` is an ETS lookup.
  """

  @behaviour WikiRace.Crawler

  @impl WikiRace.Crawler
  def start(start_url, target_url) do
    stop_previous_races()

    DynamicSupervisor.start_child(
      Player.RaceSupervisor,
      {Player.Coordinator, {start_url, target_url}}
    )
  end

  @impl WikiRace.Crawler
  def status(pid) do
    case :ets.lookup(:player_status, pid) do
      [{_pid, status}] -> status
      [] -> WikiRace.Status.new()
    end
  end

  # Clean up. old race must not keep loading Kiwix.
  defp stop_previous_races do
    for {_, pid, _, _} <- DynamicSupervisor.which_children(Player.RaceSupervisor) do
      DynamicSupervisor.terminate_child(Player.RaceSupervisor, pid)
    end

    :ok
  end
end
