defmodule Player.Search.Strategies.BFSTest do
  use ExUnit.Case, async: false
  @max_candidate_paths 100
  alias Player.Search.Strategies.BFS

  describe "BFS correctness under out-of-order worker completion" do
    test "does not advance while an article from the current level is still in flight" do
      state = BFS.new("A", "T")
      {:ok, "A", state} = dispatch(state, "A")
      {:ok, state} = complete(state, "A", ["B", "C"])

      {:ok, "B", state} = dispatch(state, "B")
      {:ok, "C", state} = dispatch(state, "C")

      {:ok, state} = complete(state, "C", ["T", "D"])

      assert state.level == 1
      assert state.state == :running
      assert Map.has_key?(state.in_flight, "B")
      assert state.target_discovered?
      assert state.depths["T"] == 2

      {:exhausted, state} = complete(state, "B", ["T", "E"])

      assert state.state == :exhausted
      assert state.level == 1
      assert map_size(state.in_flight) == 0
      assert :queue.is_empty(state.frontier)
      assert :queue.is_empty(state.next_frontier)
    end

    test "preserves all discovered shortest target paths when workers finish in either order" do
      paths_for_order = fn order ->
        state = BFS.new("A", "T")
        {:ok, "A", state} = dispatch(state, "A")
        {:ok, state} = complete(state, "A", ["B", "C"])
        {:ok, "B", state} = dispatch(state, "B")
        {:ok, "C", state} = dispatch(state, "C")

        state =
          Enum.reduce(order, state, fn url, acc ->
            case complete(acc, url, ["T"]) do
              {:ok, next_state} -> next_state
              {:exhausted, next_state} -> next_state
            end
          end)

        BFS.build_paths(state, "T") |> MapSet.new()
      end

      expected =
        MapSet.new([
          ["A", "B", "T"],
          ["A", "C", "T"]
        ])

      assert paths_for_order.(["B", "C"]) == expected
      assert paths_for_order.(["C", "B"]) == expected
    end

    test "does not expand the next level after the target is found at the current level" do
      state = BFS.new("A", "T")
      {:ok, "A", state} = dispatch(state, "A")
      {:ok, state} = complete(state, "A", ["B", "C"])

      {:ok, "B", state} = dispatch(state, "B")
      {:ok, "C", state} = dispatch(state, "C")

      {:ok, state} = complete(state, "B", ["T", "D"])
      {:exhausted, state} = complete(state, "C", ["E"])

      assert state.depths["T"] == 2
      assert state.depths["D"] == 2
      assert state.depths["E"] == 2
      assert state.state == :exhausted
      assert :queue.is_empty(state.frontier)
      assert :queue.is_empty(state.next_frontier)
      assert {:empty, _} = BFS.take_next(state)
    end

    test "does not let a later, deeper route replace the shortest route" do
      state = BFS.new("A", "T")
      {:ok, "A", state} = dispatch(state, "A")
      {:ok, state} = complete(state, "A", ["B", "C"])

      {:ok, "B", state} = dispatch(state, "B")
      {:ok, "C", state} = dispatch(state, "C")

      {:ok, state} = complete(state, "C", ["D"])
      {:exhausted, state} = complete(state, "B", ["T"])

      assert state.depths["T"] == 2
      assert state.depths["D"] == 2
      assert state.state == :exhausted
      assert BFS.build_path(state, "T") == {:ok, ["A", "B", "T"]}
    end
  end

  describe "discovery, parents and candidate handling" do
    test "keeps one parent for regular nodes and bounded alternatives for the target" do
      state = BFS.new("A", "T")
      {:ok, "A", state} = dispatch(state, "A")
      {:ok, state} = complete(state, "A", ["B", "C"])

      {:ok, "B", state} = dispatch(state, "B")
      {:ok, "C", state} = dispatch(state, "C")

      {:ok, state} = complete(state, "B", ["D", "T"])
      {:exhausted, state} = complete(state, "C", ["D", "T"])

      assert state.depths["D"] == 2
      assert state.parent_candidates["D"] == ["B"]
      assert MapSet.new(state.parent_candidates["T"]) == MapSet.new(["B", "C"])
    end

    test "does not duplicate the same parent or candidate path" do
      state = BFS.new("A", "T")
      {:ok, "A", state} = dispatch(state, "A")
      {:ok, state} = complete(state, "A", ["B", "B"])

      {:ok, "B", state} = dispatch(state, "B")
      {:exhausted, state} = complete(state, "B", ["T", "T"])

      assert state.parent_candidates["B"] == ["A"]
      assert BFS.build_paths(state, "T") == [["A", "B", "T"]]

      {:ok, ["A", "B", "T"], state} = BFS.take_candidate(state)
      assert {:empty, _state} = BFS.take_candidate(state)
    end

    test "returns candidate paths in full start-to-target order" do
      state = BFS.new("A", "T")
      {:ok, "A", state} = dispatch(state, "A")
      {:ok, state} = complete(state, "A", ["B", "C"])

      {:ok, "B", state} = dispatch(state, "B")
      {:ok, "C", state} = dispatch(state, "C")

      {:ok, state} = complete(state, "B", ["T"])
      {:exhausted, state} = complete(state, "C", ["T"])

      candidates = drain_candidates(state)

      assert MapSet.new(candidates) ==
               MapSet.new([
                 ["A", "B", "T"],
                 ["A", "C", "T"]
               ])
    end

    test "handles start equal to target as a zero-edge path" do
      state = BFS.new("A", "A")

      assert state.state == :exhausted
      assert state.target_discovered?
      assert state.depths["A"] == 0
      assert BFS.build_path(state, "A") == {:ok, ["A"]}
      assert BFS.build_paths(state, "A") == [["A"]]
      assert {:empty, _} = BFS.take_next(state)
      assert {:ok, ["A"], state} = BFS.take_candidate(state)
      assert {:empty, _} = BFS.take_candidate(state)
    end

    test "returns no path when the reachable graph is exhausted" do
      state = BFS.new("A", "T")
      {:ok, "A", state} = dispatch(state, "A")
      {:exhausted, state} = complete(state, "A", [])

      assert state.state == :exhausted
      assert state.visited_count == 1
      assert BFS.build_path(state, "T") == {:error, :path_not_found}
      assert {:empty, _} = BFS.take_next(state)
    end
  end

  describe "state and input safety" do
    test "tracks frontier sizes incrementally as levels advance" do
      state = BFS.new("A", "T")
      assert state.frontier_size == 1

      {:ok, "A", state} = dispatch(state, "A")
      assert state.frontier_size == 0
      {:ok, state} = complete(state, "A", ["B", "C"])

      assert state.frontier_size == 2
      assert state.frontier_count == 2
      assert state.next_frontier_count == 0

      {:ok, "B", state} = dispatch(state, "B")
      assert state.frontier_size == 1
    end

    test "matches the target when Kiwix returns an equivalent escaped URL" do
      start = "http://kiwix.test/content/book/Start"
      target = "http://kiwix.test/content/book/Target"
      escaped_target = "http://kiwix.test/content/book/T%61rget"

      state = BFS.new(start, target)
      {:ok, ^start, state} = dispatch(state, start)
      {:exhausted, state} = complete(state, start, [escaped_target])

      assert BFS.build_paths(state, target) == [[start, target]]
    end

    test "returns waiting when frontier is empty but a worker is still in flight" do
      state = BFS.new("A", "T")
      {:ok, "A", state} = dispatch(state, "A")

      assert {:waiting, waiting_state} = BFS.take_next(state)
      assert Map.has_key?(waiting_state.in_flight, "A")
      assert waiting_state.state == :running
    end

    test "rejects completion for an article that is not in flight" do
      state = BFS.new("A", "T")

      assert {:error, :not_in_flight, ^state} =
               BFS.complete_article(state, "A", ["B"])
    end

    test "rejects invalid links without mutating the original state" do
      state = BFS.new("A", "T")
      {:ok, "A", state} = dispatch(state, "A")

      assert {:error, :invalid_links, ^state} =
               BFS.complete_article(state, "A", ["B", nil])
    end

    test "rejects completion after the search has finished" do
      state = BFS.new("A", "T")
      {:ok, "A", state} = dispatch(state, "A")
      {:exhausted, state} = complete(state, "A", [])

      assert {:error, :search_finished, ^state} =
               BFS.complete_article(state, "A", ["T"])
    end
  end

  @tag :benchmark
  test "measures BFS candidate generation on a wide layered graph" do
    parent_count = 200

    state = BFS.new("root", "target")
    {:ok, "root", state} = dispatch(state, "root")

    parents = Enum.map(1..parent_count, &"p#{&1}")
    {:ok, state} = complete(state, "root", parents)

    state =
      Enum.reduce(parents, state, fn parent, acc ->
        {:ok, ^parent, acc} = dispatch(acc)

        case complete(acc, parent, ["target"]) do
          {:ok, next_state} -> next_state
          {:exhausted, next_state} -> next_state
        end
      end)

    {microseconds, paths} =
      :timer.tc(fn -> BFS.build_paths(state, "target") end)

    IO.puts(
      "\nBFS benchmark: #{parent_count} equal-depth parents, " <>
        "#{length(paths)} generated paths, #{microseconds} µs"
    )

    assert length(paths) <= 100

    assert Enum.all?(paths, fn path ->
             length(path) == 3 and
               hd(path) == "root" and
               String.starts_with?(Enum.at(path, 1), "p") and
               List.last(path) == "target"
           end)
  end

  defp dispatch(state) do
    case BFS.take_next(state) do
      {:ok, url, next_state} ->
        {:ok, url, next_state}

      other ->
        flunk("Expected an article to dispatch, got: #{inspect(other)}")
    end
  end

  defp dispatch(state, expected_url) do
    case BFS.take_next(state) do
      {:ok, ^expected_url, next_state} ->
        {:ok, expected_url, next_state}

      other ->
        flunk("Expected #{inspect(expected_url)} to dispatch, got: #{inspect(other)}")
    end
  end

  defp complete(state, url, links) do
    case BFS.complete_article(state, url, links) do
      {:ok, next_state} ->
        {:ok, next_state}

      {:exhausted, next_state} ->
        {:exhausted, next_state}

      other ->
        flunk("Expected completion of #{inspect(url)}, got: #{inspect(other)}")
    end
  end

  defp drain_candidates(state, acc \\ []) do
    case BFS.take_candidate(state) do
      {:ok, path, next_state} ->
        drain_candidates(next_state, [path | acc])

      {:empty, _state} ->
        Enum.reverse(acc)
    end
  end

  @tag :benchmark
  test "compares original and incremental candidate generation" do
    parent_counts = [200, 500, 1000]
    repetitions = 10

    IO.puts("\n=== BFS candidate generation comparison ===")

    Enum.each(parent_counts, fn parent_count ->
      parents = Enum.map(1..parent_count, &"p#{&1}")
      state = benchmark_state(parents)

      {original_time, original_paths} =
        :timer.tc(fn ->
          Enum.reduce(1..repetitions, [], fn _, _ ->
            benchmark_original(parents, state)
          end)
        end)

      {optimized_time, optimized_paths} =
        :timer.tc(fn ->
          Enum.reduce(1..repetitions, [], fn _, _ ->
            benchmark_incremental(parents, state)
          end)
        end)

      original_average = original_time / repetitions
      optimized_average = optimized_time / repetitions

      assert original_paths == optimized_paths

      IO.puts("""
      Parents: #{parent_count}
        Original:   #{Float.round(original_average, 2)} µs
        Optimized:  #{Float.round(optimized_average, 2)} µs
        Paths:      #{length(original_paths)}
        Improvement: #{Float.round(original_average / max(optimized_average, 1), 2)}x
      """)
    end)
  end

  defp benchmark_state(parents) do
    parent_candidates =
      Enum.reduce(parents, %{"root" => []}, fn parent, acc ->
        Map.put(acc, parent, ["root"])
      end)

    %{
      start_url: "root",
      parent_candidates: Map.put(parent_candidates, "target", [])
    }
  end

  # Simulates the original strategy:
  # rebuild all target paths after every new parent.
  defp benchmark_original(parents, state) do
    parents
    |> Enum.with_index(1)
    |> Enum.reduce([], fn {_parent, index}, _acc ->
      current_parents = Enum.take(parents, index)

      updated_state = %{
        state
        | parent_candidates: Map.put(state.parent_candidates, "target", current_parents)
      }

      BFS.build_paths(updated_state, "target")
    end)
    |> Enum.take(@max_candidate_paths)
  end

  defp benchmark_incremental(parents, state) do
    Enum.reduce(parents, [], fn parent, acc ->
      remaining = @max_candidate_paths - length(acc)

      if remaining <= 0 do
        acc
      else
        parent_path =
          BFS.build_paths(state, parent)

        new_paths =
          Enum.map(parent_path, &(&1 ++ ["target"]))

        acc ++ Enum.take(new_paths, remaining)
      end
    end)
  end
end
