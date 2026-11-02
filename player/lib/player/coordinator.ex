defmodule Player.Coordinator do
  @moduledoc """
  Runs one race: owns the frontier, claims pages and publishes status.

  Parallel BFS. Fetching and parsing happen in workers.
  Coordinator only reacts to their results.
  Each page is claimed once with `:ets.insert_new/2` and keeps its parent,
  so every reported path is a chain
  of links that were actually seen.
  """

  use GenServer, restart: :temporary

  alias WikiRace.Status

  @max_in_flight 100

  def start_link(args), do: GenServer.start_link(__MODULE__, args)

  @impl true
  def init({start_url, target_url}) do
    :ets.delete_all_objects(:player_status)
    start_c = Player.Links.canonical(start_url)
    table = :ets.new(:visited, [:set, :private])
    :ets.insert(table, {start_c, start_url, nil})
    {:ok, workers} = DynamicSupervisor.start_link(strategy: :one_for_one)

    state = %{
      table: table,
      workers: workers,
      prefix: Player.Links.prefix(start_url),
      target_url: target_url,
      target_c: Player.Links.canonical(target_url),
      frontier: :queue.from_list([{start_c, start_url}]),
      queued: 1,
      in_flight: 0,
      fetched: 0,
      current: start_url,
      path: [],
      status: :running
    }

    publish(state)
    {:ok, state, {:continue, :dispatch}}
  end

  @impl true
  def handle_continue(:dispatch, state), do: {:noreply, state |> dispatch() |> publish()}

  @impl true
  def handle_info({:expanded, canon, links}, state) do
    state = %{state | in_flight: state.in_flight - 1, fetched: state.fetched + 1}

    state =
      if state.status == :running, do: absorb(state, canon, links), else: state

    {:noreply, state |> dispatch() |> publish()}
  end

  def handle_info(_other, state), do: {:noreply, state}

  # Claim new pages, or finish if the target is among the links.
  defp absorb(state, canon, links) do
    Enum.reduce_while(links, state, fn {c, url}, acc ->
      cond do
        c == acc.target_c ->
          {:halt, found(acc, canon)}

        :ets.insert_new(acc.table, {c, url, canon}) ->
          {:cont, %{acc | frontier: :queue.in({c, url}, acc.frontier), queued: acc.queued + 1}}

        true ->
          {:cont, acc}
      end
    end)
  end

  defp found(state, canon) do
    path = chain(state.table, canon, []) ++ [state.target_url]
    WikiRace.Harness.report(path)
    %{state | path: path, status: :found}
  end

  defp chain(table, canon, acc) do
    [{_, url, parent}] = :ets.lookup(table, canon)
    if parent == nil, do: [url | acc], else: chain(table, parent, [url | acc])
  end

  defp dispatch(%{status: :running, in_flight: n} = state) when n < @max_in_flight do
    case :queue.out(state.frontier) do
      {{:value, {canon, url}}, rest} ->
        spec = %{
          id: make_ref(),
          restart: :temporary,
          start: {Player.Worker, :start_link, [self(), canon, url, state.prefix]}
        }

        DynamicSupervisor.start_child(state.workers, spec)

        dispatch(%{
          state
          | frontier: rest,
            queued: state.queued - 1,
            in_flight: state.in_flight + 1,
            current: url
        })

      {:empty, _} ->
        maybe_exhausted(state)
    end
  end

  defp dispatch(state), do: state

  defp maybe_exhausted(%{in_flight: 0, queued: 0, status: :running} = state) do
    WikiRace.Harness.report_exhausted()
    %{state | status: :exhausted}
  end

  defp maybe_exhausted(state), do: state

  defp publish(state) do
    status =
      Status.new(
        current_url: state.current,
        frontier_size: state.queued,
        visited_count: state.fetched,
        path: state.path,
        state: state.status
      )

    :ets.insert(:player_status, {self(), status})
    state
  end
end
