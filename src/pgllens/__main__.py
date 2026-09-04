"""CLI entrypoint. `python -m pgllens` or the `pgllens` console script."""

from __future__ import annotations

import argparse
import sys

# psycopg's async mode refuses Windows' default ProactorEventLoop ("Psycopg
# cannot use the 'ProactorEventLoop' to run in async mode"), so with it every
# pooled connection fails and /health reports the database unreachable while the
# HTTP surface looks healthy. uvicorn builds its loop from a factory via
# asyncio.Runner, which never consults the event-loop policy --
# set_event_loop_policy() was a no-op -- so the factory is named explicitly.
# The string form needs uvicorn >= 0.36 (get_loop_factory). "auto" already
# yields a selector loop everywhere else.
LOOP = "asyncio:SelectorEventLoop" if sys.platform == "win32" else "auto"


def main(argv: list[str] | None = None) -> None:
    import uvicorn

    from pgllens.config import get_settings
    from pgllens.obs.logconfig import configure_logging
    from pgllens.server import build_app

    parser = argparse.ArgumentParser(prog="pgllens")
    parser.add_argument("--host", default=None)
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument("--oauth", action="store_true",
                        help="Enable OAuth 2.1 (authorization code + PKCE) on /mcp.")
    args = parser.parse_args(argv)

    settings = get_settings()
    configure_logging(settings)
    # --host/--port must land on `settings` itself, not just uvicorn.run's
    # kwargs (Fix round 1): build_app derives the OAuth discovery base URL
    # from settings.host/mcp_port, so a CLI-only override was invisible to
    # discovery and it kept advertising the config-file/default port.
    if args.host:
        settings.host = args.host
    if args.port:
        settings.mcp_port = args.port
    uvicorn.run(
        build_app(settings, oauth=args.oauth),
        host=settings.host,
        port=settings.mcp_port,
        loop=LOOP,
        # Audit L2: direct-exposure mode has no proxy to shed slow clients.
        # Deliberately simple: fixed values, not settings. limit_concurrency=100 is
        # far above the 5-connection pool's useful parallelism; make them settings
        # only if a real deployment hits them.
        limit_concurrency=100,
        timeout_keep_alive=5,
    )


if __name__ == "__main__":
    main()
