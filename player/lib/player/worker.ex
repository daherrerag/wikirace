defmodule Player.Worker do
  @moduledoc """
  One fetch. Downloads a page, extracts its article links and sends them to
  the coordinator with the outcome (`:ok` or `:error`). A failed fetch is a
  dead end (no links), but it is always reported so `in_flight` goes back down.
  """

  @doc "Started under the race's DynamicSupervisor as a plain linked process."
  def start_link(coordinator, canon, url, prefix) do
    {:ok, spawn_link(fn -> run(coordinator, canon, url, prefix) end)}
  end

  defp run(coordinator, canon, url, prefix) do
    {links, outcome} =
      try do
        case fetch(url) do
          {:ok, body} ->
            links = Player.Links.extract(body, url, prefix)
            {links, :ok}

          :error ->
            {[], :error}
        end
      rescue
        _ -> {[], :error}
      end

    send(coordinator, {:expanded, canon, links, outcome})
  end

  defp fetch(url) do
    options = [
      finch: [name: Player.Finch],
      retry: :transient,
      max_retries: 1,
      retry_delay: 200,
      receive_timeout: 5_000,
      decode_body: false
    ]

    case Req.get(url, options) do
      {:ok, %{status: 200, body: body}} when is_binary(body) -> {:ok, body}
      _ -> :error
    end
  rescue
    _ -> :error
  end
end
