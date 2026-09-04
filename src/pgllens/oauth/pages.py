"""Self-contained login/consent page for the OAuth authorize step.

No branding/shell, just a plain password form. Every interpolated value is
HTML-escaped (attribute contexts included) so the page is safe even if an
attacker controls client_id/redirect_uri/state via a malicious registration.
"""

from __future__ import annotations

from html import escape


def _h(value: str | None) -> str:
    """HTML-escape for both text and attribute contexts (quotes included)."""
    return escape(value or "", quote=True)


def login_page(
    *,
    client_id: str,
    redirect_uri: str,
    code_challenge: str,
    code_challenge_method: str,
    state: str,
    csrf_token: str = "",
    error: str | None = None,
) -> str:
    error_html = f'<p class="error">{_h(error)}</p>' if error else ""
    return f"""<!doctype html>
<html>
<head><meta charset="utf-8"><title>Authorize</title></head>
<body>
  <h1>Authorize access</h1>
  <p>A client is requesting access to this MCP server. Enter the access
  password to continue.</p>
  {error_html}
  <form method="POST" action="/oauth/authorize">
    <input type="hidden" name="client_id" value="{_h(client_id)}" />
    <input type="hidden" name="redirect_uri" value="{_h(redirect_uri)}" />
    <input type="hidden" name="state" value="{_h(state)}" />
    <input type="hidden" name="code_challenge" value="{_h(code_challenge)}" />
    <input type="hidden" name="code_challenge_method" value="{_h(code_challenge_method)}" />
    <input type="hidden" name="csrf_token" value="{_h(csrf_token)}" />
    <label for="password">Password</label>
    <input type="password" id="password" name="password" autofocus required
           autocomplete="current-password" />
    <button type="submit">Authorize</button>
  </form>
</body>
</html>"""
