defmodule Player.LinksTest do
  use ExUnit.Case, async: true

  alias Player.Links

  @base "http://127.0.0.1:4589/content/wiki"

  test "canonical drops fragment, trailing slash and decodes" do
    assert Links.canonical("#{@base}/Ada%20Lovelace#life") == "#{@base}/Ada Lovelace"
    assert Links.canonical("#{@base}/Elixir/") == "#{@base}/Elixir"
  end

  test "extract keeps only article links" do
    html = """
    <html><head><link rel="stylesheet" href="style.css"></head><body>
      <a href="BEAM">b</a><a href="BEAM#x">b again</a><a href="Ruby">r</a>
      <a href="../-/search?q=Elixir">search</a><a href="#top">top</a>
      <a href="http://example.com/x">out</a>
    </body></html>
    """

    prefix = Links.prefix("#{@base}/Elixir")
    links = Links.extract(html, "#{@base}/Elixir", prefix)
    assert Enum.map(links, &elem(&1, 0)) == ["#{@base}/BEAM", "#{@base}/Ruby"]
  end
end
