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

  # ---------------------------------------------------------
  # Initialization
  # ---------------------------------------------------------

  def new(start_url, target_url) do
    target_found? = start_url == target_url

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

      # Articles waiting in the current level
      frontier: initial_frontier,

      # Articles discovered for the next level
      next_frontier: :queue.new(),

      # Articles whose processing has completed
      visited:
        if target_found? do
          MapSet.new([start_url])
        else
          MapSet.new()
        end,

      # All discovered articles
      discovered: MapSet.new([start_url]),

      # Parents that reach an article at its minimum depth
      parent_candidates: %{start_url => []},

      # Minimum BFS depth of each discovered article
      depths: %{start_url => 0},

      # Articles currently assigned to workers
      in_flight: %{},

      # Indicates whether the target has been discovered
      target_discovered?: target_found?,

      # Candidate paths waiting for validation
      candidate_paths:
        if target_found? do
          :queue.from_list([[start_url]])
        else
          :queue.new()
        end,

      # Paths already added to the candidate queue
      emitted_candidates:
        if target_found? do
          MapSet.new([[start_url]])
        else
          MapSet.new()
        end,

      # Current BFS level
      level: 0,

      # Public search progress
      current_url: nil,
      frontier_size: :queue.len(initial_frontier),
      visited_count: if(target_found?, do: 1, else: 0),

      # Internal search state: :running | :exhausted
      state: if(target_found?, do: :exhausted, else: :running)
    }

    initial_state
  end

  # ---------------------------------------------------------
  # Article dispatch
  # ---------------------------------------------------------

  @doc """
  Returns the next article to process.

  BFS continues processing articles from the current level
  even after discovering the target.

  It does not expand the next level after target discovery.
  """

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

  # ---------------------------------------------------------
  # Article completion
  # ---------------------------------------------------------

  @doc """
  Registers the links extracted from a processed article.

  Returns:
    {:ok, updated_state}
    {:exhausted, updated_state}
    {:error, reason, original_state}
  """

  def complete_article(state, url, links) when is_list(links) do
    cond do
      state.state != :running ->
        {:error, :search_finished, state}

      not Map.has_key?(state.in_flight, url) ->
        {:error, :not_in_flight, state}

      not Enum.all?(links, &(is_binary(&1) and &1 != "")) ->
        {:error, :invalid_links, state}

      true ->
        updated_state =
          Enum.reduce(links, state, fn link, acc_state ->
            register_link(acc_state, url, link)
          end)

        updated_visited =
          MapSet.put(updated_state.visited, url)

        completed_state = %{
          updated_state
          | in_flight: Map.delete(updated_state.in_flight, url),
            visited: updated_visited,
            visited_count: MapSet.size(updated_visited),
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

  def complete_article(state, _url, _links) do
    {:error, :invalid_links, state}
  end

  # ---------------------------------------------------------
  # Link registration
  # ---------------------------------------------------------

  defp register_link(state, parent_url, link) do
    parent_depth = Map.fetch!(state.depths, parent_url)
    child_depth = parent_depth + 1

    updated_state =
      cond do
        not MapSet.member?(state.discovered, link) ->
          %{
            state
            | discovered: MapSet.put(state.discovered, link),
              parent_candidates: Map.put(state.parent_candidates, link, [parent_url]),
              depths: Map.put(state.depths, link, child_depth),
              next_frontier: :queue.in(link, state.next_frontier),
              target_discovered?: state.target_discovered? or link == state.target_url
          }

        Map.get(state.depths, link) == child_depth ->
          existing_parents =
            Map.get(state.parent_candidates, link, [])

          if parent_url in existing_parents do
            state
          else
            updated_parents =
              Map.put(
                state.parent_candidates,
                link,
                existing_parents ++ [parent_url]
              )

            %{state | parent_candidates: updated_parents}
          end

        true ->
          state
      end

    if link == state.target_url do
      enqueue_target_candidates(updated_state)
    else
      updated_state
    end
  end

  # ---------------------------------------------------------
  # Candidate path generation
  # ---------------------------------------------------------

  @doc """
  Builds one shortest path to the target.

  Returns {:ok, path} or {:error, :path_not_found}.
  """

  def build_path(state, target_url) do
    case build_paths(state, target_url) do
      [path | _rest] ->
        {:ok, path}

      [] ->
        {:error, :path_not_found}
    end
  end

  @doc """
  Builds all available shortest paths to a target,
  up to the configured candidate limit.
  """

  def build_paths(state, target_url) do
    build_paths_recursive(
      state.parent_candidates,
      target_url,
      MapSet.new(),
      @max_candidate_paths
    )
    |> Enum.filter(fn
      [start_url | _rest] ->
        start_url == state.start_url

      _ ->
        false
    end)
  end

  defp enqueue_target_candidates(state) do
    candidates =
      build_paths(state, state.target_url)

    Enum.reduce(candidates, state, fn path, acc_state ->
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
          |> Enum.reduce_while([], fn parent_url, accumulated_paths ->
            remaining =
              limit - length(accumulated_paths)

            if remaining <= 0 do
              {:halt, accumulated_paths}
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
                  path ++ [current_url]
                end)

              combined_paths =
                accumulated_paths ++ new_paths

              if length(combined_paths) >= limit do
                {:halt, Enum.take(combined_paths, limit)}
              else
                {:cont, combined_paths}
              end
            end
          end)

        _ ->
          []
      end
    end
  end

  # ---------------------------------------------------------
  # Candidate retrieval
  # ---------------------------------------------------------

  @doc """
  Retrieves the next candidate path for validation.

  Returns:
    {:ok, path, updated_state}
    {:empty, updated_state}
  """

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

  # ---------------------------------------------------------
  # Candidate rejection
  # ---------------------------------------------------------

  @doc """
  Compatibility function.

  Removes the next candidate from the queue.
  It does not stop or modify the BFS search.
  """

  def reject_candidate(state) do
    case take_candidate(state) do
      {:ok, _path, updated_state} ->
        updated_state

      {:empty, updated_state} ->
        updated_state
    end
  end

  # ---------------------------------------------------------
  # Search completion
  # ---------------------------------------------------------

  @doc """
  Checks whether BFS has completed all work in its current level.
  """

  def check_exhausted(state) do
    updated_state =
      advance_level_if_ready(state)

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
        # The target was found. Finish the current level,
        # but do not expand deeper levels.
        state.target_discovered? ->
          %{
            state
            | state: :exhausted,
              next_frontier: :queue.new(),
              frontier_size: 0
          }

        # No more articles remain.
        :queue.is_empty(state.next_frontier) ->
          %{
            state
            | state: :exhausted,
              frontier_size: 0
          }

        # Continue with the next BFS level.
        true ->
          updated_state = %{
            state
            | frontier: state.next_frontier,
              next_frontier: :queue.new(),
              level: state.level + 1
          }

          update_frontier_size(updated_state)
      end
    else
      state
    end
  end

  # ---------------------------------------------------------
  # Progress
  # ---------------------------------------------------------

  defp update_frontier_size(state) do
    frontier_size =
      :queue.len(state.frontier) +
        :queue.len(state.next_frontier)

    %{state | frontier_size: frontier_size}
  end
end
