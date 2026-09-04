import re

from pgllens.design import widgetbuild


def test_build_substitutes_every_marker():
    html = widgetbuild.build_widget_html()
    for marker in ("/*==TOKENS==*/", "/*==CSS==*/", "<!--==FAVICON==-->"):
        assert marker not in html          # all replaced
    assert ":root{" in html                 # tokens landed
    assert "data:image/svg+xml;base64," in html


def test_widget_does_not_place_the_brand_mark_in_the_canvas():
    # The Abelara mark/wordmark was tried inline (a floating bottom-left overlay, then
    # inside the top toolbar) and rejected both times -- it kept colliding with or
    # crowding the advisory banner in a widget this small. Inverted from an earlier
    # assertion that required the mark's presence, so a future edit can't silently
    # reintroduce a colliding logo. The favicon (tab/window icon, never on-canvas) stays.
    html = widgetbuild.build_widget_html()
    assert "ablMarkGrad" not in html
    assert "data:image/svg+xml;base64," in html


def test_build_is_deterministic():
    assert widgetbuild.build_widget_html() == widgetbuild.build_widget_html()


def test_hidden_attribute_actually_hides_flex_elements():
    # Regression: .erd-empty/.erd-banner set display:flex in the shared component sheet.
    # An author rule beats the UA's `[hidden] { display: none }` regardless of selector
    # specificity, so toggling `.hidden = true` in JS silently did nothing and the
    # "waiting for data" panel stayed visible over a fully-rendered diagram.
    # The template must restate `[hidden]{display:none}` with enough force
    # (!important) to win back.
    html = widgetbuild.build_widget_html()
    assert re.search(r"\[hidden\]\s*\{[^}]*display\s*:\s*none\s*!important", html)
