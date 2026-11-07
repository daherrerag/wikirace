defmodule Player.Search.Coordinator do
  @moduledoc """
  Coordinates a single Wikipedia search race.

  Manages concurrent article workers, BFS execution,
  concurrent candidate path validation, checkpoint persistence
  and search metrics.

  The first valid path is reported immediately.
  Shorter valid paths are reported when discovered.
  """

  use GenServer

  alias WikiRace.Status

  alias Player.Infrastructure.State, as: InfrastructureState
  alias Player.Search.Worker
  alias Player.Search.PathValidator
  alias Player.Search.Strategies.BFS

  @max_concurrent_workers 32
  @max_concurrent_validators 8

  @progress_fields [
    :current_url,
    :frontier_size,
    :visited_count,
    :path,
    :state
  ]

  @checkpoint_batch_size 50
  @checkpoint_interval_ms 1_000

  # ---------------------------------------------------------
  # Client API
  # ---------------------------------------------------------

  def start_link(args) do
    GenServer.start_link(__MODULE__, args)
  end

  def status(pid) do
    GenServer.call(pid, :status)
  end

  def metrics(pid) do
    GenServer.call(pid, :metrics)
  end

  def update_progress(pid, updates) do
    GenServer.call(pid, {:update_progress, updates})
  end

  def flush_checkpoint(pid) do
    GenServer.call(pid, :flush_checkpoint)
  end

  def worker_info(pid) do
    GenServer.call(pid, :worker_info)
  end

  # ---------------------------------------------------------
  # Initialization
  # ---------------------------------------------------------

  @impl true
  def init(%{
        race_id: race_id,
        start_url: start_url,
        target_url: target_url
      }) do
    checkpoint = %{
      race_id: race_id,
      start_url: start_url,
      target_url: target_url,
      current_url: nil,
      frontier_size: 1,
      visited_count: 0,
      path: [],
      state: :running
    }

    :ok = InfrastructureState.save_checkpoint(race_id, checkpoint)

    timer =
      Process.send_after(
        self(),
        :checkpoint,
        @checkpoint_interval_ms
      )

    bfs_state = BFS.new(start_url, target_url)

    started_at = System.monotonic_time(:millisecond)

    runtime_state = %{
      checkpoint: checkpoint,
      pending_updates: 0,
      checkpoint_dirty?: false,
      checkpoint_timer: timer,

      bfs: bfs_state,

      # Article download tasks
      workers: %{},

      # Candidate validation tasks
      validators: %{},

      # Best valid path reported so far
      best_path: nil,

      started_at: started_at,

      max_active_workers: 0,
      total_workers_started: 0,

      max_active_validators: 0,
      total_validators_started: 0,

      articles_completed: 0,
      failed_articles: 0,

      validated_candidates: 0,
      rejected_candidates: 0,
      validation_errors: []
    }

    send(self(), :pump)

    {:ok, runtime_state}
  end

  # ---------------------------------------------------------
  # Status
  # ---------------------------------------------------------

  @impl true
  def handle_call(:status, _from, runtime_state) do
    checkpoint = runtime_state.checkpoint

    status =
      Status.new(
        current_url: checkpoint.current_url,
        frontier_size: checkpoint.frontier_size,
        visited_count: checkpoint.visited_count,
        path: checkpoint.path,
        state: checkpoint.state
      )

    {:reply, status, runtime_state}
  end

  @impl true
  def handle_call(:metrics, _from, runtime_state) do
    metrics = %{
      active_workers: map_size(runtime_state.workers),
      max_active_workers: runtime_state.max_active_workers,
      total_workers_started: runtime_state.total_workers_started,

      active_validators: map_size(runtime_state.validators),
      max_active_validators: runtime_state.max_active_validators,
      total_validators_started: runtime_state.total_validators_started,

      articles_completed: runtime_state.articles_completed,
      failed_articles: runtime_state.failed_articles,

      validated_candidates: runtime_state.validated_candidates,
      rejected_candidates: runtime_state.rejected_candidates,

      best_path: runtime_state.best_path,

      elapsed_ms:
        System.monotonic_time(:millisecond) -
          runtime_state.started_at,

      validation_errors: runtime_state.validation_errors
    }

    {:reply, metrics, runtime_state}
  end

  @impl true
  def handle_call(:worker_info, _from, runtime_state) do
    {:reply, runtime_state.workers, runtime_state}
  end

  # ---------------------------------------------------------
  # External progress updates
  # ---------------------------------------------------------

  @impl true
  def handle_call({:update_progress, updates}, _from, runtime_state)
      when is_map(updates) do
    invalid_fields = Map.keys(updates) -- @progress_fields

    if invalid_fields == [] do
      new_state = apply_progress_updates(runtime_state, updates)

      {:reply, :ok, new_state}
    else
      {:reply, {:error, {:invalid_fields, invalid_fields}}, runtime_state}
    end
  end

  @impl true
  def handle_call({:update_progress, _updates}, _from, runtime_state) do
    {:reply, {:error, :invalid_updates}, runtime_state}
  end

  # ---------------------------------------------------------
  # Checkpoint persistence
  # ---------------------------------------------------------

  @impl true
  def handle_call(:flush_checkpoint, _from, runtime_state) do
    case persist_checkpoint(runtime_state) do
      {:ok, saved_state} ->
        {:reply, :ok, saved_state}

      {:error, reason, unsaved_state} ->
        {:reply, {:error, reason}, unsaved_state}
    end
  end

  # ---------------------------------------------------------
  # Main work pump
  # ---------------------------------------------------------

  @impl true
  def handle_info(:pump, runtime_state) do
    new_state =
      runtime_state
      |> dispatch_workers()
      |> dispatch_validators()
      |> sync_public_progress()

    {:noreply, new_state}
  end

  # ---------------------------------------------------------
  # Task results
  # ---------------------------------------------------------

  @impl true
  def handle_info({task_ref, result}, runtime_state)
      when is_reference(task_ref) do
    cond do
      Map.has_key?(runtime_state.validators, task_ref) ->
        Process.demonitor(task_ref, [:flush])

        new_state =
          process_validator_result(
            runtime_state,
            task_ref,
            result
          )

        send(self(), :pump)

        {:noreply, new_state}

      Map.has_key?(runtime_state.workers, task_ref) ->
        Process.demonitor(task_ref, [:flush])

        new_state =
          process_worker_result_message(
            runtime_state,
            task_ref,
            result
          )

        send(self(), :pump)

        {:noreply, new_state}

      true ->
        {:noreply, runtime_state}
    end
  end

  # ---------------------------------------------------------
  # Task termination
  # ---------------------------------------------------------

  @impl true
  def handle_info(
        {:DOWN, task_ref, :process, _pid, reason},
        runtime_state
      )
      when is_reference(task_ref) do
    cond do
      Map.has_key?(runtime_state.validators, task_ref) ->
        new_state =
          process_validator_down(
            runtime_state,
            task_ref,
            reason
          )

        send(self(), :pump)

        {:noreply, new_state}

      Map.has_key?(runtime_state.workers, task_ref) ->
        new_state =
          process_worker_down(
            runtime_state,
            task_ref,
            reason
          )

        send(self(), :pump)

        {:noreply, new_state}

      true ->
        {:noreply, runtime_state}
    end
  end

  # ---------------------------------------------------------
  # Checkpoint timer
  # ---------------------------------------------------------

  @impl true
  def handle_info(:checkpoint, runtime_state) do
    new_state =
      case persist_checkpoint(runtime_state) do
        {:ok, saved_state} -> saved_state
        {:error, _reason, unsaved_state} -> unsaved_state
      end

    timer =
      Process.send_after(
        self(),
        :checkpoint,
        @checkpoint_interval_ms
      )

    {:noreply, %{new_state | checkpoint_timer: timer}}
  end

  # ---------------------------------------------------------
  # Internal: dispatch article workers
  # ---------------------------------------------------------

  defp dispatch_workers(runtime_state) do
    cond do
      runtime_state.bfs.state != :running ->
        runtime_state

      map_size(runtime_state.workers) >= @max_concurrent_workers ->
        runtime_state

      true ->
        case BFS.take_next(runtime_state.bfs) do
          {:ok, url, updated_bfs} ->
            new_state = %{runtime_state | bfs: updated_bfs}

            case start_worker_task(new_state, url) do
              {:ok, updated_state} ->
                dispatch_workers(updated_state)

              {:error, updated_state} ->
                dispatch_workers(updated_state)
            end

          {:waiting, updated_bfs} ->
            %{runtime_state | bfs: updated_bfs}

          {:empty, updated_bfs} ->
            %{runtime_state | bfs: updated_bfs}
        end
    end
  end

  # ---------------------------------------------------------
  # Internal: start article worker
  # ---------------------------------------------------------

  defp start_worker_task(runtime_state, url) do
    try do
      task =
        Task.Supervisor.async_nolink(
          Player.TaskSupervisor,
          fn -> Worker.run(url) end
        )

      worker = %{
        url: url,
        pid: task.pid
      }

      active_workers = map_size(runtime_state.workers) + 1

      updated_state = %{
        runtime_state
        | workers: Map.put(runtime_state.workers, task.ref, worker),
          total_workers_started: runtime_state.total_workers_started + 1,
          max_active_workers: max(runtime_state.max_active_workers, active_workers)
      }

      {:ok, updated_state}
    catch
      :exit, reason ->
        updated_state =
          complete_failed_article(
            runtime_state,
            url,
            {:worker_start_failed, reason}
          )

        {:error, updated_state}
    end
  end

  # ---------------------------------------------------------
  # Internal: process article worker result
  # ---------------------------------------------------------

  defp process_worker_result_message(runtime_state, task_ref, result) do
    {worker, updated_workers} =
      Map.pop(runtime_state.workers, task_ref)

    new_state = %{
      runtime_state
      | workers: updated_workers
    }

    new_state =
      process_worker_result(new_state, worker, result)

    new_state
  end

  defp process_worker_result(runtime_state, worker, result) do
    case result do
      {:ok, url, links}
      when url == worker.url and is_list(links) ->
        new_state = %{
          runtime_state
          | articles_completed: runtime_state.articles_completed + 1
        }

        case BFS.complete_article(
               new_state.bfs,
               url,
               links
             ) do
          {result, updated_bfs}
          when result in [:ok, :exhausted] ->
            %{new_state | bfs: updated_bfs}

          {:error, _reason, updated_bfs} ->
            %{new_state | bfs: updated_bfs}
        end

      {:error, url, reason}
      when url == worker.url ->
        complete_failed_article(runtime_state, url, reason)

      unexpected_result ->
        complete_failed_article(
          runtime_state,
          worker.url,
          {:unexpected_worker_result, unexpected_result}
        )
    end
  end

  # ---------------------------------------------------------
  # Internal: worker termination
  # ---------------------------------------------------------

  defp process_worker_down(runtime_state, task_ref, reason) do
    {worker, updated_workers} =
      Map.pop(runtime_state.workers, task_ref)

    new_state = %{
      runtime_state
      | workers: updated_workers
    }

    complete_failed_article(
      new_state,
      worker.url,
      {:worker_down, reason}
    )
  end

  # ---------------------------------------------------------
  # Internal: failed article
  # ---------------------------------------------------------

  defp complete_failed_article(runtime_state, url, reason) do
    new_state = %{
      runtime_state
      | failed_articles: runtime_state.failed_articles + 1
    }

    case BFS.complete_article(
           new_state.bfs,
           url,
           []
         ) do
      {result, updated_bfs}
      when result in [:ok, :exhausted] ->
        %{
          new_state
          | bfs: updated_bfs,
            validation_errors:
              Enum.take(
                [{:article_failed, url, reason} | new_state.validation_errors],
                20
              )
        }

      {:error, _error, updated_bfs} ->
        %{new_state | bfs: updated_bfs}
    end
  end

  # ---------------------------------------------------------
  # Internal: dispatch candidate validators
  # ---------------------------------------------------------

  defp dispatch_validators(runtime_state) do
    if map_size(runtime_state.validators) >= @max_concurrent_validators do
      runtime_state
    else
      case BFS.take_candidate(runtime_state.bfs) do
        {:ok, path, updated_bfs} ->
          new_state = %{runtime_state | bfs: updated_bfs}

          case start_validator_task(new_state, path) do
            {:ok, updated_state} ->
              dispatch_validators(updated_state)

            {:error, updated_state} ->
              dispatch_validators(updated_state)
          end

        {:empty, updated_bfs} ->
          %{runtime_state | bfs: updated_bfs}
      end
    end
  end

  # ---------------------------------------------------------
  # Internal: start candidate validator
  # ---------------------------------------------------------

  defp start_validator_task(runtime_state, path) do
    try do
      task =
        Task.Supervisor.async_nolink(
          Player.TaskSupervisor,
          fn ->
            PathValidator.validate(path)
          end
        )

      validator = %{
        pid: task.pid,
        path: path,
        length: length(path)
      }

      active_validators = map_size(runtime_state.validators) + 1

      updated_state = %{
        runtime_state
        | validators: Map.put(runtime_state.validators, task.ref, validator),
          total_validators_started:
            runtime_state.total_validators_started + 1,
          max_active_validators:
            max(runtime_state.max_active_validators, active_validators)
      }

      {:ok, updated_state}
    catch
      :exit, reason ->
        updated_state =
          reject_candidate(
            runtime_state,
            path,
            {:validator_start_failed, reason}
          )

        {:error, updated_state}
    end
  end

  # ---------------------------------------------------------
  # Internal: process validator result
  # ---------------------------------------------------------

  defp process_validator_result(runtime_state, task_ref, result) do
    {validator, updated_validators} =
      Map.pop(runtime_state.validators, task_ref)

    new_state = %{
      runtime_state
      | validators: updated_validators
    }

    case result do
      {:ok, validated_path}
      when validated_path == validator.path ->
        accept_candidate(new_state, validated_path)

      {:ok, unexpected_path} ->
        reject_candidate(
          new_state,
          validator.path,
          {:validated_path_mismatch,
           %{
             expected: validator.path,
             received: unexpected_path
           }}
        )

      {:error, reason} ->
        reject_candidate(
          new_state,
          validator.path,
          reason
        )

      unexpected_result ->
        reject_candidate(
          new_state,
          validator.path,
          {:unexpected_validation_result, unexpected_result}
        )
    end
  end

  # ---------------------------------------------------------
  # Internal: validator termination
  # ---------------------------------------------------------

  defp process_validator_down(runtime_state, task_ref, reason) do
    {validator, updated_validators} =
      Map.pop(runtime_state.validators, task_ref)

    new_state = %{
      runtime_state
      | validators: updated_validators
    }

    reject_candidate(
      new_state,
      validator.path,
      {:validator_down, reason}
    )
  end

  # ---------------------------------------------------------
  # Internal: accept valid candidate
  # ---------------------------------------------------------

  defp accept_candidate(runtime_state, path) do
    new_state = %{
      runtime_state
      | validated_candidates: runtime_state.validated_candidates + 1
    }

    case new_state.best_path do
      nil ->
        WikiRace.Harness.report(path)

        %{new_state | best_path: path}

      current_best ->
        if length(path) < length(current_best) do
          WikiRace.Harness.report(path)

          %{new_state | best_path: path}
        else
          new_state
        end
    end
  end

  # ---------------------------------------------------------
  # Internal: reject invalid candidate
  # ---------------------------------------------------------

  defp reject_candidate(runtime_state, path, reason) do
    %{
      runtime_state
      | rejected_candidates: runtime_state.rejected_candidates + 1,
        validation_errors:
          Enum.take(
            [{:candidate_rejected, path, reason} |
             runtime_state.validation_errors],
            20
          )
    }
  end

  # ---------------------------------------------------------
  # Internal: synchronize public progress
  # ---------------------------------------------------------

  defp sync_public_progress(runtime_state) do
    bfs = runtime_state.bfs

    pending_candidates =
      :queue.len(bfs.candidate_paths)

    public_state =
      cond do
        not is_nil(runtime_state.best_path) ->
          :found

        bfs.state == :exhausted and
            map_size(runtime_state.validators) == 0 and
            pending_candidates == 0 ->
          :exhausted

        true ->
          :running
      end

    updates = %{
      current_url: bfs.current_url,
      frontier_size: bfs.frontier_size,
      visited_count: bfs.visited_count,
      path: runtime_state.best_path || [],
      state: public_state
    }

    apply_progress_updates(runtime_state, updates)
  end

  defp apply_progress_updates(runtime_state, updates) do
    checkpoint = Map.merge(runtime_state.checkpoint, updates)

    new_state = %{
      runtime_state
      | checkpoint: checkpoint,
        pending_updates: runtime_state.pending_updates + 1,
        checkpoint_dirty?: true
    }

    if new_state.pending_updates >= @checkpoint_batch_size do
      case persist_checkpoint(new_state) do
        {:ok, saved_state} -> saved_state
        {:error, _reason, unsaved_state} -> unsaved_state
      end
    else
      new_state
    end
  end

  # ---------------------------------------------------------
  # Internal: checkpoint persistence
  # ---------------------------------------------------------

  defp persist_checkpoint(%{checkpoint_dirty?: false} = runtime_state) do
    {:ok, runtime_state}
  end

  defp persist_checkpoint(runtime_state) do
    checkpoint = runtime_state.checkpoint

    case InfrastructureState.save_checkpoint(
           checkpoint.race_id,
           checkpoint
         ) do
      :ok ->
        saved_state = %{
          runtime_state
          | pending_updates: 0,
            checkpoint_dirty?: false
        }

        {:ok, saved_state}

      error ->
        {:error, error, runtime_state}
    end
  end

  # ---------------------------------------------------------
  # Child specification
  # ---------------------------------------------------------

  def child_spec(args) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [args]},
      restart: :transient,
      shutdown: 5000,
      type: :worker
    }
  end
end
