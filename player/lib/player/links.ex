defmodule Player.Links do
  @moduledoc """
  Link handling.
  Mirrors `WikiRacePlatform.Race.Verifier`,
  so a link we follow is a link the verifier will find.

  Verifier parses with Floki, reads only `<a href>`,
  resolves with `URI.merge(page_url, href)` and compares `canonical/1` strings.
  """

  @doc "Decoded path, no query, no fragment, no trailing slash."
  @spec canonical(String.t()) :: String.t()
  def canonical(url) do
    uri = URI.parse(url)
    path = uri.path |> to_string() |> URI.decode() |> String.trim_trailing("/")
    "#{uri.scheme}://#{uri.host}#{port(uri)}#{path}"
  end

  defp port(%URI{scheme: "http", port: 80}), do: ""
  defp port(%URI{scheme: "https", port: 443}), do: ""
  defp port(%URI{port: nil}), do: ""
  defp port(%URI{port: port}), do: ":#{port}"

  @doc "Canonical article prefix, derived from the start URL."
  @spec prefix(String.t()) :: String.t()
  def prefix(start_url), do: start_url |> canonical() |> Path.dirname()

  @doc """
  Article links of a page as `[{canonical, url}]`.

  Only `<a href>` links under the article prefix.
  Links with a query are skipped,
  fragments are dropped.
  """
  @spec extract(String.t(), String.t(), String.t()) :: [{String.t(), String.t()}]
  def extract(body, page_url, prefix) do
    case Floki.parse_document(body) do
      {:ok, document} ->
        document
        |> Floki.attribute("a", "href")
        |> Enum.flat_map(&resolve(&1, page_url, prefix))
        |> Enum.uniq_by(&elem(&1, 0))

      _ ->
        []
    end
  end

  defp resolve("#" <> _rest, _page, _prefix), do: []
  defp resolve("javascript:" <> _rest, _page, _prefix), do: []
  defp resolve("mailto:" <> _rest, _page, _prefix), do: []

  defp resolve(href, page_url, prefix) do
    uri = URI.merge(page_url, href)

    if uri.query == nil do
      url = URI.to_string(%{uri | fragment: nil})
      canon = canonical(url)
      if article?(canon, prefix), do: [{canon, url}], else: []
    else
      []
    end
  rescue
    _ -> []
  end

  defp article?(canon, prefix) do
    String.starts_with?(canon, prefix <> "/") and byte_size(canon) > byte_size(prefix) + 1
  end
end
