# tests/test_widget_bridge.py — static assertions; the behavioural proof is the
# Playwright run documented in the task report, which drives tests/fixtures/mock_host.html.
import re
from pathlib import Path

WIDGET = Path("src/pgllens/widgets/erd_view.html")


def widget() -> str:
    return WIDGET.read_text(encoding="utf-8")


def test_uses_only_specified_method_names():
    html = widget()
    for method in ("ui/initialize", "ui/notifications/initialized",
                   "ui/notifications/tool-result", "ui/notifications/size-changed",
                   "ui/request-display-mode", "ui/download-file", "tools/call"):
        assert method in html, f"missing spec method {method}"


def test_invents_no_unspecified_methods():
    """Every ui/* or tools/* string in the widget must be a method the spec defines."""
    allowed = {
        "ui/initialize", "ui/notifications/initialized", "ui/notifications/tool-input",
        "ui/notifications/tool-input-partial", "ui/notifications/tool-result",
        "ui/notifications/tool-cancelled", "ui/notifications/host-context-changed",
        "ui/notifications/size-changed", "ui/resource-teardown",
        "ui/request-display-mode", "ui/download-file", "tools/call",
        "notifications/message",
    }
    found = set(re.findall(r'"((?:ui|tools|notifications)/[a-z\-/]+)"', widget()))
    assert found <= allowed, f"unspecified methods invented: {sorted(found - allowed)}"


def test_no_callservertool_or_downloadfile_shorthand():
    """Earlier drafts assumed callServerTool()/downloadFile() helpers that the
    spec does not define; the real methods are tools/call and ui/download-file."""
    html = widget()
    assert "callServerTool" not in html
    assert re.search(r"\bdownloadFile\s*\(", html) is None


def test_declares_protocol_version_and_client_info():
    html = widget()
    assert "protocolVersion" in html and "clientInfo" in html


def test_still_self_contained():
    html = widget()
    assert "fetch(" not in html and "eval(" not in html
    assert not re.search(r'src=["\']https?://', html)


def test_never_sends_ui_download_file():
    # ui/download-file is draft-only, absent from the 2026-01-26 stable revision.
    # It's fine for the string to appear in a comment explaining that; it must
    # never appear as a message actually sent (a quoted JSON-RPC method literal
    # inside a call()/post() site). Belt-and-braces alongside the sync test's
    # narrower `'"ui/download-file"' not in html` check.
    html = widget()
    assert 'method: "ui/download-file"' not in html
    assert "\"ui/download-file\", {" not in html


def test_handshake_completes_the_view_initiates_sequence():
    # The widget must send ui/initialize, then only after a result arrives send
    # ui/notifications/initialized -- never the other way around, and never
    # skipped. (Behavioural proof that the host actually receives them in order
    # is the Playwright run; this pins the source-level contract.)
    html = widget()
    assert 'call("ui/initialize"' in html
    assert 'notify("ui/notifications/initialized"' in html
    init_idx = html.index('call("ui/initialize"')
    notified_idx = html.index('notify("ui/notifications/initialized"')
    assert init_idx < notified_idx


def test_bounded_wait_on_initialize():
    # No timeout at all was the original bug this task fixes: a host that never
    # answers ui/initialize must not hang the widget forever.
    html = widget()
    assert "HOST_INIT_TIMEOUT_MS" in html
    assert re.search(r"setTimeout\(\s*\(\)\s*=>\s*finish\(null\)", html)


def test_host_context_changed_reacts_to_theme_dimensions_and_display_mode():
    html = widget()
    assert "ui/notifications/host-context-changed" in html
    assert "function applyHostContext" in html
    assert 'dataset.theme' in html
    assert "scheduleRefit()" in html
    assert "function updateDisplayModeChrome" in html


def test_resource_teardown_is_answered_not_silent():
    # ui/resource-teardown is a REQUEST (carries an id); it must get a JSON-RPC
    # response, and must stop timers/observers rather than leaving them running
    # against a view the host is discarding.
    html = widget()
    assert "ui/resource-teardown" in html
    assert "function teardown()" in html
    assert "respond(msg.id" in html
    assert "_resizeObserver.disconnect()" in html


def test_size_changed_is_fire_and_forget():
    # Emitted via notify() (no id, no reply expected) -- never bridge.call(), which
    # would wait on a response draft-only hosts will never send.
    html = widget()
    assert 'bridge.notify("ui/notifications/size-changed"' in html
    assert 'bridge.call("ui/notifications/size-changed"' not in html


def test_fullscreen_control_hidden_until_advertised():
    html = widget()
    assert "function fullscreenAdvertised" in html
    assert "$(\"fullscreen\").hidden = !fullscreenAdvertised" in html


def test_display_mode_request_restores_state_on_error():
    html = widget()
    assert 'bridge.call("ui/request-display-mode"' in html
    # the catch branch must restore aria-pressed rather than leave the button
    # stuck mid-request or throw uncaught
    assert "prevPressed" in html


def test_bridge_exposes_the_documented_interface():
    html = widget()
    for member in ("connect", "call", "notify", "on"):
        assert f"{member}" in html
    assert "get capabilities()" in html
    assert "get hostContext()" in html
