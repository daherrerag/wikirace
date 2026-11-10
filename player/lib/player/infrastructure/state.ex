defmodule Player.Infrastructure.State do
  use GenServer

  @checkpoints_table :player_checkpoints
  @pages_table :player_pages
  @statuses_table :player_statuses

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, :ok, Keyword.put(opts, :name, __MODULE__))
  end

  def save_checkpoint(race_id, checkpoint) do
    true = :ets.insert(@checkpoints_table, {race_id, checkpoint})
    :ok
  end

  def get_checkpoint(race_id) do
    case :ets.lookup(@checkpoints_table, race_id) do
      [{^race_id, checkpoint}] -> {:ok, checkpoint}
      [] -> :error
    end
  end

  def delete_checkpoint(race_id) do
    true = :ets.delete(@checkpoints_table, race_id)
    :ok
  end

  def list_checkpoints do
    :ets.select(@checkpoints_table, [{{:"$1", :"$2"}, [], [:"$1"]}])
  end

  def save_status(pid, status) when is_pid(pid) do
    true = :ets.insert(@statuses_table, {pid, status})
    :ok
  end

  def get_status(pid) when is_pid(pid) do
    case :ets.lookup(@statuses_table, pid) do
      [{^pid, status}] -> {:ok, status}
      [] -> :error
    end
  end

  def delete_status(pid) when is_pid(pid) do
    true = :ets.delete(@statuses_table, pid)
    :ok
  end

  def save_page(url, links) when is_binary(url) and is_list(links) do
    true = :ets.insert(@pages_table, {url, links})
    :ok
  end

  def get_page(url) when is_binary(url) do
    case :ets.lookup(@pages_table, url) do
      [{^url, links}] -> {:ok, links}
      [] -> :error
    end
  end

  @impl true
  def init(:ok) do
    :ets.new(@checkpoints_table, [
      :named_table,
      :set,
      :public,
      read_concurrency: true,
      write_concurrency: true
    ])

    :ets.new(@pages_table, [
      :named_table,
      :set,
      :public,
      read_concurrency: true,
      write_concurrency: true
    ])

    :ets.new(@statuses_table, [
      :named_table,
      :set,
      :public,
      read_concurrency: true,
      write_concurrency: true
    ])

    {:ok, :ok}
  end
end
