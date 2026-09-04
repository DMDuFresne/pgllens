from pgllens.design import brand, css, tokens


def test_favicon_is_an_inline_data_uri():
    assert brand.FAVICON_DATA_URI.startswith("data:image/svg+xml;base64,")
    assert "http" not in brand.FAVICON_DATA_URI.split(",", 1)[1]


def test_palette_matches_the_brand_book():
    assert tokens.BG == "#252525"
    assert tokens.BLUE == "#b3e6e1"
    assert tokens.GREEN == "#d4fdb1"
    assert tokens.YELLOW == "#ffffa9"
    assert tokens.RED == "#f5602b"


def test_css_variables_declares_every_token():
    v = tokens.css_variables()
    for name in ("--bg", "--panel", "--ink", "--muted", "--edge", "--blue",
                 "--green", "--yellow", "--red", "--grad-mark", "--font-brand",
                 "--font-mono", "--r-sm", "--r-md", "--r-lg"):
        assert name in v
    assert v.startswith(":root{") and v.endswith("}")


def test_widget_css_loads_nothing_external():
    sheet = css.widget_css()
    assert "@import" not in sheet
    assert "url(http" not in sheet and "//fonts." not in sheet
    assert "var(--bg)" in sheet
