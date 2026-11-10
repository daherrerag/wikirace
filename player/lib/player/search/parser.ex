defmodule Player.Search.Parser do
  @excluded_namespaces ~w(
    File Category Special Talk User Wikipedia Help Portal
    Template MediaWiki Module Draft Book TimedText
    Gadget Gadget_definition
  )

  @excluded_extensions ~w(
    .jpg .jpeg .png .gif .svg .webp .ico
    .css .js .json .xml .pdf .ogg .mp3 .mp4
    .woff .woff2 .ttf .zip
  )

  def extract_links(html, base_url)
      when is_binary(html) and is_binary(base_url) do
    with {:ok, pairs} <- extract_link_pairs(html, base_url) do
      {:ok, Enum.map(pairs, &elem(&1, 1))}
    end
  end

  def extract_links(_html, _base_url), do: {:error, :invalid_input}

  def extract_link_pairs(html, base_url)
      when is_binary(html) and is_binary(base_url) do
    with {:ok, document} <- Floki.parse_document(html),
         {:ok, base_uri} <- parse_base_url(base_url),
         {:ok, content_prefix} <- content_prefix(base_uri.path) do
      {links, _seen} =
        document
        |> Floki.attribute("a", "href")
        |> Enum.reduce({[], MapSet.new()}, fn href, {links, seen} ->
          case normalize_link(href, base_uri, content_prefix) do
            {:ok, url} ->
              key = canonical(url)

              if MapSet.member?(seen, key) do
                {links, seen}
              else
                {[{key, url} | links], MapSet.put(seen, key)}
              end

            _ ->
              {links, seen}
          end
        end)

      links = Enum.reverse(links)

      {:ok, links}
    end
  end

  def extract_link_pairs(_html, _base_url), do: {:error, :invalid_input}

  def canonical(url) when is_binary(url) do
    uri = URI.parse(url)

    if is_binary(uri.scheme) and is_binary(uri.host) do
      path = uri.path |> to_string() |> URI.decode() |> String.trim_trailing("/")

      port =
        if uri.port in [nil, 80] and uri.scheme == "http",
          do: "",
          else:
            if(uri.port == 443 and uri.scheme == "https",
              do: "",
              else: if(uri.port, do: ":#{uri.port}", else: "")
            )

      "#{uri.scheme}://#{uri.host}#{port}#{path}"
    else
      url
    end
  rescue
    _ -> url
  end

  defp parse_base_url(base_url) do
    uri = URI.parse(base_url)

    if uri.scheme in ["http", "https"] and
         is_binary(uri.host) do
      {:ok, uri}
    else
      {:error, :invalid_base_url}
    end
  end

  defp content_prefix(path) when is_binary(path) do
    case String.split(path, "/", trim: true) do
      ["content", snapshot | _] ->
        {:ok, "/content/#{snapshot}/"}

      _ ->
        {:error, :invalid_kiwix_path}
    end
  end

  defp normalize_link(href, base_uri, content_prefix)
       when is_binary(href) do
    href = String.trim(href)

    cond do
      href == "" ->
        :skip

      String.starts_with?(href, ["#", "mailto:", "javascript:", "tel:"]) ->
        :skip

      true ->
        resolve_and_validate(href, base_uri, content_prefix)
    end
  end

  defp normalize_link(_href, _base_uri, _content_prefix), do: :skip

  defp resolve_and_validate(href, base_uri, content_prefix) do
    try do
      uri = URI.merge(base_uri, href)

      normalized_uri = %{uri | fragment: nil}

      cond do
        not same_origin?(normalized_uri, base_uri) ->
          :skip

        not article_path?(normalized_uri.path, content_prefix) ->
          :skip

        normalized_uri.query != nil ->
          :skip

        excluded_namespace?(normalized_uri.path, content_prefix) ->
          :skip

        true ->
          {:ok, URI.to_string(normalized_uri)}
      end
    rescue
      _error -> :skip
    end
  end

  defp same_origin?(uri, base_uri) do
    uri.scheme == base_uri.scheme and
      uri.host == base_uri.host and
      effective_port(uri) == effective_port(base_uri)
  end

  defp effective_port(%URI{port: nil, scheme: "http"}), do: 80
  defp effective_port(%URI{port: nil, scheme: "https"}), do: 443
  defp effective_port(%URI{port: port}), do: port

  defp article_path?(path, content_prefix)
       when is_binary(path) do
    String.starts_with?(path, content_prefix) and
      String.length(path) > String.length(content_prefix) and
      not excluded_extension?(path)
  end

  defp article_path?(_path, _content_prefix), do: false

  defp excluded_extension?(path) do
    path
    |> String.downcase()
    |> then(fn path ->
      Enum.any?(@excluded_extensions, &String.ends_with?(path, &1))
    end)
  end

  defp excluded_namespace?(path, content_prefix) do
    title =
      String.replace_prefix(path, content_prefix, "")
      |> URI.decode()
      |> String.split("/", parts: 2)
      |> List.first()

    case String.split(title, ":", parts: 2) do
      [namespace, _article] ->
        namespace in @excluded_namespaces

      _ ->
        false
    end
  end
end
