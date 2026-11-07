defmodule Player.Infrastructure.State do
  @moduledoc """
  Maintains shared checkpoints and cached article links across crawler races.
  """

  use GenServer

  def start_link(opts \\ []) do
    GenServer.start_link(
      __MODULE__,
      %{checkpoints: %{}, pages: %{}},
      Keyword.put(opts, :name, __MODULE__)
    )
  end

  # Checkpoints

  def save_checkpoint(race_id, checkpoint) do
    GenServer.call(__MODULE__, {:save_checkpoint, race_id, checkpoint})
  end

  def get_checkpoint(race_id) do
    GenServer.call(__MODULE__, {:get_checkpoint, race_id})
  end

  def delete_checkpoint(race_id) do
    GenServer.call(__MODULE__, {:delete_checkpoint, race_id})
  end

  def list_checkpoints do
    GenServer.call(__MODULE__, :list_checkpoints)
  end

  # Article cache

  def save_page(url, links) when is_binary(url) and is_list(links) do
    GenServer.call(__MODULE__, {:save_page, url, links})
  end

  def get_page(url) when is_binary(url) do
    GenServer.call(__MODULE__, {:get_page, url})
  end

  # Server callbacks

  @impl true
  def init(initial_state) do
    {:ok, initial_state}
  end

  # Checkpoint callbacks

  @impl true
  def handle_call({:save_checkpoint, race_id, checkpoint}, _from, state) do
    new_checkpoints = Map.put(state.checkpoints, race_id, checkpoint)

    {:reply, :ok, %{state | checkpoints: new_checkpoints}}
  end

  @impl true
  def handle_call({:get_checkpoint, race_id}, _from, state) do
    result =
      case Map.fetch(state.checkpoints, race_id) do
        {:ok, checkpoint} -> {:ok, checkpoint}
        :error -> :error
      end

    {:reply, result, state}
  end

  @impl true
  def handle_call({:delete_checkpoint, race_id}, _from, state) do
    new_checkpoints = Map.delete(state.checkpoints, race_id)

    {:reply, :ok, %{state | checkpoints: new_checkpoints}}
  end

  @impl true
  def handle_call(:list_checkpoints, _from, state) do
    race_ids = Map.keys(state.checkpoints)

    {:reply, race_ids, state}
  end

  # Article cache callbacks

  @impl true
  def handle_call({:save_page, url, links}, _from, state) do
    new_pages = Map.put(state.pages, url, links)

    {:reply, :ok, %{state | pages: new_pages}}
  end

  @impl true
  def handle_call({:get_page, url}, _from, state) do
    result =
      case Map.fetch(state.pages, url) do
        {:ok, links} -> {:ok, links}
        :error -> :error
      end

    {:reply, result, state}
  end
end
