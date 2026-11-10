defmodule Player.Search.ParserTest do
  use ExUnit.Case, async: true

  alias Player.Search.Parser

  test "returns unique article links in page order" do
    html = """
    <a href="First">first</a>
    <a href="%46irst">escaped duplicate</a>
    <a href="Second#section">second</a>
    <a href="First#again">duplicate</a>
    """

    assert {:ok, links} =
             Parser.extract_links(
               html,
               "http://kiwix.test/content/book/Start"
             )

    assert links == [
             "http://kiwix.test/content/book/First",
             "http://kiwix.test/content/book/Second"
           ]
  end

  test "canonicalizes escaped paths for target comparison" do
    assert Parser.canonical("http://kiwix.test/content/book/T%61rget#section") ==
             Parser.canonical("http://kiwix.test/content/book/Target")
  end

  test "returns precomputed canonical keys with fetchable URLs" do
    html = ~s(<a href="T%61rget">target</a>)
    target_url = "http://kiwix.test/content/book/T%61rget"

    assert {:ok, [{key, ^target_url}]} =
             Parser.extract_link_pairs(html, "http://kiwix.test/content/book/Start")

    assert key == Parser.canonical("http://kiwix.test/content/book/Target")
  end
end
