defmodule Player.Worker do
  @moduledoc """
  One fetch. Downloads a page, extracts its article links
  and sends them to the coordinator.
  Failed fetch is reported as a dead end.
  """

  @doc "Started under the race's DynamicSupervisor. Plain linked process."
  def start_link(coordinator, canon, url, prefix) do
    {:ok, spawn_link(fn -> run(coordinator, canon, url, prefix) end)}
  end

  defp run(coordinator, canon, url, prefix) do
    links =
      case fetch(url) do
        {:ok, body} -> Player.Links.extract(body, url, prefix)
        :error -> []
      end

    send(coordinator, {:expanded, canon, links})
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
