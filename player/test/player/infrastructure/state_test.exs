defmodule Player.Infrastructure.StateTest do
  use ExUnit.Case, async: false

  alias Player.Infrastructure.State

  test "article cache is available to concurrent processes through ETS" do
    url = "http://kiwix.test/content/book/#{System.unique_integer([:positive])}"
    links = [url <> "/next"]

    assert :ok = State.save_page(url, links)

    results =
      1..32
      |> Task.async_stream(fn _ -> State.get_page(url) end, max_concurrency: 16)
      |> Enum.to_list()

    assert Enum.all?(results, &(&1 == {:ok, {:ok, links}}))
  end

  test "checkpoint operations use the shared table" do
    race_id = System.unique_integer([:positive])
    checkpoint = %{race_id: race_id, state: :running}

    assert :ok = State.save_checkpoint(race_id, checkpoint)
    assert {:ok, ^checkpoint} = State.get_checkpoint(race_id)
    assert race_id in State.list_checkpoints()
    assert :ok = State.delete_checkpoint(race_id)
    assert :error = State.get_checkpoint(race_id)
  end

  test "progress snapshots can be read without calling the coordinator" do
    status = WikiRace.Status.new(current_url: "page", visited_count: 7)

    assert :ok = State.save_status(self(), status)
    assert {:ok, ^status} = State.get_status(self())
    assert :ok = State.delete_status(self())
    assert :error = State.get_status(self())
  end
end
