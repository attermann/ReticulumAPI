"""aiohttp middleware that gates REST endpoints on the configured auth policy."""
from __future__ import annotations

from aiohttp import web


PUBLIC_PATHS = {"/health", "/version", "/auth/login"}
WS_PATHS = {"/ws"}


def _extract_token(request: web.Request) -> str | None:
    header = request.headers.get("Authorization", "")
    if header.startswith("Bearer "):
        return header[len("Bearer ") :].strip() or None
    return None


@web.middleware
async def auth_middleware(request: web.Request, handler):
    app = request.app
    config = app["config"]
    registry = app["sessions"]

    if not config.auth.enabled:
        request["session"] = registry.anonymous()
        return await handler(request)

    if request.path in WS_PATHS:
        # WS does its own first-frame auth
        return await handler(request)

    if request.path in PUBLIC_PATHS:
        return await handler(request)

    token = _extract_token(request)
    if token is None:
        return web.json_response({"error": "unauthorized"}, status=401)

    session = registry.get_by_token(token)
    if session is None:
        return web.json_response({"error": "unauthorized"}, status=401)

    request["session"] = session
    return await handler(request)
