defmodule Player.Coordinator do
  @moduledoc """
  Owns one race: frontier, claims and status. It never fetches or parses, it
  only reacts to `{:expanded, canon, links, outcome}` messages, so it stays light.

  Parallel BFS: up to `MAX_IN_FLIGHT` (env, default 100) fetch workers at a
  time. Every page is claimed once with `:ets.insert_new/2`; the claim stores
  its parent, so a path is always a chain of links seen in fetched pages.
  """

  use GenServer, restart: :temporary

  # begin:bench
  require Logger
  # end:bench

  alias WikiRace.Status

  @default_max_in_flight 100

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
      max: System.get_env("MAX_IN_FLIGHT", "#{@default_max_in_flight}") |> String.to_integer(),
      prefix: Player.Links.prefix(start_url),
      target_url: target_url,
      target_c: Player.Links.canonical(target_url),
      frontier: :queue.from_list([{start_c, start_url}]),
      queued: 1,
      in_flight: 0,
      fetched: 0,
      errors: 0,
      current: start_url,
      path: [],
      status: :running,
      # begin:bench
      started_ms: System.monotonic_time(:millisecond),
      wall0: bench_start()
      # end:bench
    }

    publish(state)
    {:ok, state, {:continue, :dispatch}}
  end

  @impl true
  def handle_continue(:dispatch, state), do: {:noreply, state |> dispatch() |> publish()}

  @impl true
  def handle_info({:expanded, canon, links, outcome}, state) do
    state = %{
      state
      | in_flight: state.in_flight - 1,
        fetched: state.fetched + 1,
        errors: state.errors + if(outcome == :error, do: 1, else: 0)
    }

    state =
      if state.status == :running, do: absorb(state, canon, links), else: state

    {:noreply, state |> dispatch() |> publish()}
  end

  # begin:bench
  def handle_info(:bench_tick, %{status: :running} = state) do
    log_bench("running", state)
    Process.send_after(self(), :bench_tick, 1_000)
    {:noreply, state}
  end

  def handle_info(:bench_tick, state), do: {:noreply, state}
  # end:bench

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
    stop_workers(state.workers)
    found = %{state | path: path, status: :found}
    # begin:bench
    log_bench("found", found)
    # end:bench
    found
  end

  # The race is decided: stop loading Kiwix.
  defp stop_workers(sup) do
    for {_, pid, _, _} <- DynamicSupervisor.which_children(sup) do
      DynamicSupervisor.terminate_child(sup, pid)
    end

    :ok
  end

  defp chain(table, canon, acc) do
    [{_, url, parent}] = :ets.lookup(table, canon)
    if parent == nil, do: [url | acc], else: chain(table, parent, [url | acc])
  end

  defp dispatch(%{status: :running, in_flight: n, max: max} = state) when n < max do
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

  # begin:bench
  # Benchmark logging. Delete every `begin:bench` .. `end:bench` block to remove it.
  # Grep the player logs for BENCH; `phase=found` lines are the per-race results.
  defp bench_start do
    :erlang.system_flag(:scheduler_wall_time, true)
    Process.send_after(self(), :bench_tick, 1_000)
    :erlang.statistics(:scheduler_wall_time_all)
  end

  defp log_bench(phase, state) do
    ms = System.monotonic_time(:millisecond) - state.started_ms
    hops = if state.path == [], do: 0, else: length(state.path)

    Logger.info(
      "BENCH phase=#{phase} ms=#{ms} hops=#{hops} fetched=#{state.fetched} " <>
        "errors=#{state.errors} in_flight=#{state.in_flight} queued=#{state.queued} " <>
        "max=#{state.max} util=#{utilization(state.wall0)} " <>
        "current=#{state.current} target=#{state.target_url}"
    )
  end

  defp utilization(wall0) do
    wall1 = :erlang.statistics(:scheduler_wall_time_all)

    {busy, total} =
      Enum.zip(Enum.sort(wall0), Enum.sort(wall1))
      |> Enum.reduce({0, 0}, fn {{_, a0, t0}, {_, a1, t1}}, {a, t} ->
        {a + a1 - a0, t + t1 - t0}
      end)

    if total == 0, do: 0.0, else: Float.round(busy / total, 3)
  end

  # end:bench
end
