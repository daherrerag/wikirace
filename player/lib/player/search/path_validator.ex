defmodule Player.Search.PathValidator do
  alias Player.Search.Worker
  alias Player.Infrastructure.State
  alias Player.Search.Parser

  def validate([url]) when is_binary(url) do
    {:ok, [url]}
  end

  def validate([first_url, second_url | _rest] = path)
      when is_binary(first_url) and is_binary(second_url) do
    case validate_edges(path, 0) do
      :ok -> {:ok, path}
      error -> error
    end
  end

  def validate(_path) do
    {:error, :invalid_path}
  end

  defp validate_edges([_last_url], _index) do
    :ok
  end

  defp validate_edges([current_url, next_url | rest], index) do
    case get_article_links(current_url) do
      {:ok, links} ->
        next_key = Parser.canonical(next_url)

        if Enum.any?(links, &link_matches?(&1, next_key)) do
          validate_edges([next_url | rest], index + 1)
        else
          {:error,
           {:missing_link,
            %{
              from: current_url,
              to: next_url,
              edge_index: index
            }}}
        end

      {:error, reason} ->
        {:error,
         {:article_validation_failed,
          %{
            url: current_url,
            reason: reason,
            edge_index: index
          }}}
    end
  end

  defp validate_edges([], _index) do
    {:error, :invalid_path}
  end

  defp link_matches?({key, _url}, next_key), do: key == next_key
  defp link_matches?(url, next_key) when is_binary(url), do: Parser.canonical(url) == next_key

  defp get_article_links(url) do
    case State.get_page(url) do
      {:ok, links} ->
        {:ok, links}

      :error ->
        fetch_and_cache(url)
    end
  end

  defp fetch_and_cache(url) do
    case Worker.run(url) do
      {:ok, ^url, links} ->
        {:ok, links}

      {:error, ^url, reason} ->
        {:error, reason}

      unexpected_result ->
        {:error, {:unexpected_worker_result, unexpected_result}}
    end
  end
end
