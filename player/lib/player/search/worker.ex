defmodule Player.Search.Worker do

  alias Player.Search.Parser
  alias Player.Infrastructure.State

  @request_timeout 10_000

  def run(url) when is_binary(url) do
    case State.get_page(url) do
      {:ok, links} ->
        {:ok, url, links}

      :error ->
        fetch_and_parse(url)
    end
  end

  def run(_url) do
    {:error, nil, :invalid_url}
  end

  defp fetch_and_parse(url) do
    try do
      case fetch_article(url) do
        {:ok, html} ->
          case Parser.extract_link_pairs(html, url) do
            {:ok, links} ->
              case State.save_page(url, links) do
                :ok -> {:ok, url, links}
                error -> {:error, url, {:cache_save_failed, error}}
              end

            {:error, reason} ->
              {:error, url, {:parser_error, reason}}
          end

        {:error, reason} ->
          {:error, url, reason}
      end
    rescue
      error -> {:error, url, {:worker_exception, error}}
    catch
      kind, reason -> {:error, url, {:worker_failure, kind, reason}}
    end
  end

  defp fetch_article(url) do
    case Req.get(url,
           finch: [name: Player.Finch],
           receive_timeout: @request_timeout,
           retry: :transient,
           max_retries: 1,
           retry_delay: 200,
           decode_body: false
         ) do
      {:ok, %Req.Response{status: status, body: body}}
      when status in 200..299 and is_binary(body) ->
        {:ok, body}

      {:ok, %Req.Response{status: status}} ->
        {:error, {:http_status, status}}

      {:error, reason} ->
        {:error, {:http_request_failed, reason}}
    end
  end
end
