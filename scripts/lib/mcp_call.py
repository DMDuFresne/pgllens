#!/usr/bin/env python3
"""One MCP tools/call over Streamable HTTP, stdlib only. Used by verify-stack.sh
to generate real signal (a rejected write, an unknown schema) without an MCP client.

  python3 scripts/lib/mcp_call.py http://localhost:3000 query '{"sql":"DELETE FROM x"}'

Set MCP_BEARER to send an Authorization header (needed when MCP_AUTH_MODE != none).

Exit codes -- verify-stack.sh's fire_signal depends on this split:
  0  the call returned a JSON-RPC *result*, printed as text to stdout. This
     INCLUDES tool-level error envelopes (a read-only-gate rejection is a
     successful call that returned an error payload); the caller inspects the
     text to tell those apart.
  2  a JSON-RPC error object came back (bad method, bad params, auth); the error
     object is printed to stderr.
  3  transport failure: unreachable, timeout, unparseable body, or a response
     that is neither a result nor an error.
"""
import json
import os
import sys
import urllib.error
import urllib.request

ACCEPT = "application/json, text/event-stream"


def post(url: str, payload: dict, session: str | None) -> tuple[dict | None, str | None]:
    headers = {"Content-Type": "application/json", "Accept": ACCEPT}
    if os.environ.get("MCP_BEARER"):
        headers["Authorization"] = f"Bearer {os.environ['MCP_BEARER']}"
    if session:
        headers["Mcp-Session-Id"] = session
    req = urllib.request.Request(url, data=json.dumps(payload).encode(), headers=headers, method="POST")
    with urllib.request.urlopen(req, timeout=30) as resp:
        sid = resp.headers.get("Mcp-Session-Id", session)
        body = resp.read().decode()
        ctype = resp.headers.get("Content-Type", "")
    if not body.strip():
        return None, sid
    if "text/event-stream" in ctype:
        # First `data:` line wins: pgllens answers a single request per POST, so
        # the stream carries exactly one event. A multi-event stream would need
        # the id matched against the request instead.
        for line in body.splitlines():
            if line.startswith("data:"):
                return json.loads(line[5:].strip()), sid
        return None, sid
    return json.loads(body), sid


def main() -> int:
    base, tool, args = sys.argv[1], sys.argv[2], json.loads(sys.argv[3] if len(sys.argv) > 3 else "{}")
    url = base.rstrip("/") + "/mcp"
    try:
        _init, sid = post(url, {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
            "protocolVersion": "2025-06-18", "capabilities": {},
            "clientInfo": {"name": "verify-stack", "version": "1"}}}, None)
        post(url, {"jsonrpc": "2.0", "method": "notifications/initialized"}, sid)
        result, _ = post(url, {"jsonrpc": "2.0", "id": 2, "method": "tools/call",
                               "params": {"name": tool, "arguments": args}}, sid)
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as e:
        print(f"transport error: {e}", file=sys.stderr)
        return 3
    if result is None:
        print("no response body", file=sys.stderr)
        return 3
    if "error" in result:
        print(json.dumps(result["error"]), file=sys.stderr)
        return 2
    payload = result.get("result")
    if not isinstance(payload, dict):
        # Neither result nor error: not a JSON-RPC response at all. Exit 3 rather
        # than KeyError, so the caller sees "transport" and not a traceback.
        print(f"malformed JSON-RPC response (no result, no error): {json.dumps(result)}", file=sys.stderr)
        return 3
    for item in payload.get("content", []):
        if item.get("type") == "text":
            print(item["text"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
