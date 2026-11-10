defmodule Player.Search.Strategies.BFS do
  @moduledoc """
  Level-based concurrent Breadth-First Search.

  BFS discovers candidate paths as soon as the target is found.
  Candidate paths are stored independently from the search state,
  allowing the coordinator to validate them concurrently while
  BFS continues processing the current level.

  The search does not stop immediately when the target is discovered.
  It finishes processing the current level to collect alternative
  shortest paths, but does not expand deeper levels.
  """

  @max_candidate_paths 100
  @max_target_parents 8

  def new(start_url, target_url) do
    target_key = Player.Search.Parser.canonical(target_url)
    target_found? = Player.Search.Parser.canonical(start_url) == target_key

    initial_frontier =
      if target_found? do
        :queue.new()
      else
        :queue.from_list([start_url])
      end

    initial_state = %{
      strategy: :bfs,
      start_url: start_url,
      target_url: target_url,
      target_key: target_key,
      frontier: initial_frontier,
      frontier_count: if(target_found?, do: 0, else: 1),
      next_frontier: :queue.new(),
      next_frontier_count: 0,
      parent_candidates: %{start_url => []},
      depths: %{start_url => 0},
      in_flight: %{},
      target_discovered?: target_found?,
      candidate_paths:
        if target_found? do
          :queue.from_list([[start_url]])
        else
          :queue.new()
        end,
      emitted_candidates:
        if target_found? do
          MapSet.new([[start_url]])
        else
          MapSet.new()
        end,
      level: 0,
      current_url: nil,
      frontier_size: if(target_found?, do: 0, else: 1),
      visited_count: if(target_found?, do: 1, else: 0),
      state: if(target_found?, do: :exhausted, else: :running)
    }

    initial_state
  end

  def take_next(state) do
    state = advance_level_if_ready(state)

    cond do
      state.state == :exhausted ->
        {:empty, state}

      true ->
        case :queue.out(state.frontier) do
          {{:value, url}, updated_frontier} ->
            updated_state = %{
              state
              | frontier: updated_frontier,
                frontier_count: state.frontier_count - 1,
                in_flight: Map.put(state.in_flight, url, %{}),
                current_url: url
            }

            updated_state =
              update_frontier_size(updated_state)

            {:ok, url, updated_state}

          {:empty, _queue} ->
            {:waiting, state}
        end
    end
  end

  def complete_article(state, url, links) when is_list(links) do
    cond do
      state.state != :running ->
        {:error, :search_finished, state}

      not Map.has_key?(state.in_flight, url) ->
        {:error, :not_in_flight, state}

      true ->
        case register_links(state, url, links) do
          {:error, :invalid_links} ->
            {:error, :invalid_links, state}

          {:ok, updated_state} ->
            completed_state = %{
              updated_state
              | in_flight: Map.delete(updated_state.in_flight, url),
                visited_count: updated_state.visited_count + 1,
                current_url: url
            }

            completed_state =
              update_frontier_size(completed_state)

            completed_state =
              advance_level_if_ready(completed_state)

            case completed_state.state do
              :exhausted ->
                {:exhausted, completed_state}

              :running ->
                {:ok, completed_state}
            end
        end
    end
  end

  def complete_article(state, _url, _links) do
    {:error, :invalid_links, state}
  end

  defp register_links(state, parent_url, links) do
    Enum.reduce_while(links, {:ok, state}, fn link, {:ok, acc_state} ->
      case link_pair(link) do
        {key, url} ->
          {:cont, {:ok, register_link(acc_state, parent_url, key, url)}}

        :error ->
          {:halt, {:error, :invalid_links}}
      end
    end)
  end

  defp register_link(state, parent_url, link_key, link) do
    target_link? = link_key == state.target_key
    link = if target_link?, do: state.target_url, else: link
    parent_depth = Map.fetch!(state.depths, parent_url)
    child_depth = parent_depth + 1

    updated_state =
      case Map.fetch(state.depths, link) do
        :error ->
          %{
            state
            | parent_candidates: Map.put(state.parent_candidates, link, [parent_url]),
              depths: Map.put(state.depths, link, child_depth),
              next_frontier: :queue.in(link, state.next_frontier),
              next_frontier_count: state.next_frontier_count + 1,
              target_discovered?: state.target_discovered? or target_link?
          }

        {:ok, ^child_depth} ->
          existing_parents = Map.get(state.parent_candidates, link, [])

          if not target_link? or parent_url in existing_parents or
               length(existing_parents) >= @max_target_parents do
            state
          else
            %{
              state
              | parent_candidates:
                  Map.put(state.parent_candidates, link, [parent_url | existing_parents])
            }
          end

        {:ok, _other_depth} ->
          state
      end

    target_parent_added? =
      target_link? and
        Map.get(state.parent_candidates, link) !=
          Map.get(updated_state.parent_candidates, link)

    if target_parent_added? do
      enqueue_target_candidates(updated_state, parent_url)
    else
      updated_state
    end
  end

  defp link_pair({key, url}) when is_binary(key) and is_binary(url) and key != "" and url != "" do
    {key, url}
  end

  defp link_pair(url) when is_binary(url) and url != "" do
    {Player.Search.Parser.canonical(url), url}
  end

  defp link_pair(_), do: :error

  def build_path(state, target_url) do
    case build_paths(state, target_url) do
      [path | _rest] ->
        {:ok, path}

      [] ->
        {:error, :path_not_found}
    end
  end

  def build_paths(state, target_url) do
    state.parent_candidates
    |> build_paths_recursive(target_url, MapSet.new(), @max_candidate_paths)
    |> Enum.map(&Enum.reverse/1)
    |> Enum.filter(fn
      [start_url | _rest] -> start_url == state.start_url
      _ -> false
    end)
  end

  defp enqueue_target_candidates(state, parent_url) do
    remaining =
      @max_candidate_paths - MapSet.size(state.emitted_candidates)

    if remaining <= 0 do
      state
    else
      state
      |> build_paths(parent_url)
      |> Enum.take(remaining)
      |> Enum.reduce(state, fn parent_path, acc_state ->
        path = parent_path ++ [state.target_url]

        if MapSet.member?(acc_state.emitted_candidates, path) do
          acc_state
        else
          %{
            acc_state
            | candidate_paths: :queue.in(path, acc_state.candidate_paths),
              emitted_candidates: MapSet.put(acc_state.emitted_candidates, path)
          }
        end
      end)
    end
  end

  defp build_paths_recursive(_parents, _current_url, _visited, limit)
       when limit <= 0 do
    []
  end

  defp build_paths_recursive(parents, current_url, visited, limit) do
    if MapSet.member?(visited, current_url) do
      []
    else
      updated_visited =
        MapSet.put(visited, current_url)

      case Map.fetch(parents, current_url) do
        {:ok, []} ->
          [[current_url]]

        {:ok, parent_urls} when is_list(parent_urls) ->
          parent_urls
          |> Enum.reduce_while({[], 0}, fn parent_url, {accumulated_paths, count} ->
            remaining = limit - count

            if remaining <= 0 do
              {:halt, {accumulated_paths, count}}
            else
              parent_paths =
                build_paths_recursive(
                  parents,
                  parent_url,
                  updated_visited,
                  remaining
                )

              new_paths =
                Enum.map(parent_paths, fn path ->
                  [current_url | path]
                end)

              combined_paths =
                accumulated_paths ++ new_paths

              combined_count = count + length(new_paths)

              if combined_count >= limit do
                {:halt, {Enum.take(combined_paths, limit), limit}}
              else
                {:cont, {combined_paths, combined_count}}
              end
            end
          end)
          |> elem(0)

        _ ->
          []
      end
    end
  end

  def take_candidate(state) do
    case :queue.out(state.candidate_paths) do
      {{:value, path}, remaining_candidates} ->
        updated_state = %{
          state
          | candidate_paths: remaining_candidates
        }

        {:ok, path, updated_state}

      {:empty, _queue} ->
        {:empty, state}
    end
  end

  def reject_candidate(state) do
    case take_candidate(state) do
      {:ok, _path, updated_state} ->
        updated_state

      {:empty, updated_state} ->
        updated_state
    end
  end

  def check_exhausted(state) do
    updated_state = advance_level_if_ready(state)

    case updated_state.state do
      :exhausted ->
        {:exhausted, updated_state}

      :running ->
        {:running, updated_state}
    end
  end

  defp advance_level_if_ready(state) do
    can_advance? =
      state.state == :running and
        :queue.is_empty(state.frontier) and
        map_size(state.in_flight) == 0

    if can_advance? do
      cond do
        state.target_discovered? ->
          %{
            state
            | state: :exhausted,
              next_frontier: :queue.new(),
              frontier_count: 0,
              next_frontier_count: 0,
              frontier_size: 0
          }

        :queue.is_empty(state.next_frontier) ->
          %{
            state
            | state: :exhausted,
              frontier_count: 0,
              frontier_size: 0
          }

        true ->
          updated_state = %{
            state
            | frontier: state.next_frontier,
              frontier_count: state.next_frontier_count,
              next_frontier: :queue.new(),
              next_frontier_count: 0,
              level: state.level + 1
          }

          update_frontier_size(updated_state)
      end
    else
      state
    end
  end

  defp update_frontier_size(state) do
    frontier_size = state.frontier_count + state.next_frontier_count

    %{state | frontier_size: frontier_size}
  end
end
