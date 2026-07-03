"""REST and WS handlers for RNS Resource send/receive.

REST:
- POST   /links/{id}/resources          Send: raw request body streamed to
                                        a temp file then handed to RNS.Resource.
                                        Query params:
                                          - `await_complete` (bool, default false)
                                          - `timeout` (float, seconds)
                                          - `auto_compress` (bool, default true)
                                          - `metadata` (url-encoded JSON)
- POST   /links/{id}/resources/policy   Body {"accept": true|false}
- GET    /links/{id}/resources          List transfers on this link
- GET    /resources                     List all session transfers
- GET    /resources/{transfer_id}       Metadata
- GET    /resources/{transfer_id}/data  Streamed download of the temp file
- DELETE /resources/{transfer_id}       Cancel/delete

WS:
- resource.send          — small-file send via data_b64
- resource.list          — list transfers
- resource.status        — one transfer
- resource.cancel        — cancel/delete
- resource.policy        — per-link accept flag

Server-emitted events (session-scoped): resource.started,
resource.progress, resource.completed, resource.sent, resource.failed.
"""
from __future__ import annotations

import base64
import json
import logging
import secrets
from pathlib import Path

from aiohttp import web

from ..rns.resources import ResourceError, ResourcesService


log = logging.getLogger(__name__)


# ---------- REST ----------


async def rest_send_resource(request: web.Request) -> web.Response:
    svc: ResourcesService = request.app["resources"]
    session = request["session"]
    link_id = request.match_info["id"]

    # Query params
    await_complete = request.rel_url.query.get("await_complete", "false").lower() in ("1", "true", "yes")
    timeout_raw = request.rel_url.query.get("timeout")
    try:
        timeout = float(timeout_raw) if timeout_raw else None
    except ValueError:
        return web.json_response({"error": "timeout must be a number"}, status=400)
    auto_compress = request.rel_url.query.get("auto_compress", "true").lower() in ("1", "true", "yes")
    metadata_raw = request.rel_url.query.get("metadata")
    metadata = None
    if metadata_raw:
        try:
            metadata = json.loads(metadata_raw)
        except Exception:
            return web.json_response({"error": "metadata must be JSON"}, status=400)

    # Stream the request body into a temp file
    upload_id = secrets.token_hex(8)
    upload_path = request.app["storage"].resources_dir / f"upload_{upload_id}"
    upload_path.parent.mkdir(parents=True, exist_ok=True)
    total_written = 0
    try:
        with open(upload_path, "wb") as out:
            async for chunk in request.content.iter_chunked(64 * 1024):
                out.write(chunk)
                total_written += len(chunk)
        try:
            upload_path.chmod(0o600)
        except Exception:
            pass
    except Exception as e:
        upload_path.unlink(missing_ok=True)
        return web.json_response({"error": f"upload failed: {e}"}, status=500)

    if total_written == 0:
        upload_path.unlink(missing_ok=True)
        return web.json_response({"error": "empty body"}, status=400)

    try:
        result = await svc.send(
            session,
            link_id,
            upload_path,
            metadata=metadata,
            auto_compress=auto_compress,
            await_complete=await_complete,
            timeout=timeout,
        )
    except ResourceError as e:
        upload_path.unlink(missing_ok=True)
        msg = str(e)
        status = 404 if "unknown link" in msg else (409 if "not ACTIVE" in msg else 400)
        return web.json_response({"error": msg}, status=status)

    if await_complete:
        kind = result.get("kind")
        if kind == "timeout":
            return web.json_response(result, status=408)
        if kind not in ("complete",):
            return web.json_response(result, status=502)
        return web.json_response(result, status=200)
    return web.json_response(result, status=201)


async def rest_link_policy(request: web.Request) -> web.Response:
    svc: ResourcesService = request.app["resources"]
    session = request["session"]
    link_id = request.match_info["id"]
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"error": "invalid_json"}, status=400)
    if "accept" not in body or not isinstance(body["accept"], bool):
        return web.json_response({"error": "'accept' boolean required"}, status=400)
    try:
        result = svc.set_link_policy(session, link_id, body["accept"])
    except ResourceError as e:
        return web.json_response({"error": str(e)}, status=404)
    return web.json_response(result)


async def rest_list_link_resources(request: web.Request) -> web.Response:
    svc: ResourcesService = request.app["resources"]
    session = request["session"]
    link_id = request.match_info["id"]
    return web.json_response({"resources": svc.list_transfers(session, link_id_hex=link_id)})


async def rest_list_resources(request: web.Request) -> web.Response:
    svc: ResourcesService = request.app["resources"]
    session = request["session"]
    return web.json_response({"resources": svc.list_transfers(session)})


async def rest_get_resource(request: web.Request) -> web.Response:
    svc: ResourcesService = request.app["resources"]
    session = request["session"]
    try:
        state = svc.get_state(session, request.match_info["transfer_id"])
    except ResourceError as e:
        return web.json_response({"error": str(e)}, status=404)
    download_url = f"/resources/{state.transfer_id}/data" if state.direction == "in" and state.status == "COMPLETE" else None
    return web.json_response(state.to_dict(download_url=download_url))


async def rest_download_resource(request: web.Request) -> web.StreamResponse:
    svc: ResourcesService = request.app["resources"]
    session = request["session"]
    transfer_id = request.match_info["transfer_id"]
    try:
        path = svc.open_stream(session, transfer_id)
    except ResourceError as e:
        msg = str(e)
        if "expired" in msg:
            return web.json_response({"error": msg}, status=410)
        return web.json_response({"error": msg}, status=404)
    return web.FileResponse(
        path=path,
        headers={
            "Content-Type": "application/octet-stream",
            "Content-Disposition": f"attachment; filename=\"{transfer_id}\"",
        },
    )


async def rest_delete_resource(request: web.Request) -> web.Response:
    svc: ResourcesService = request.app["resources"]
    session = request["session"]
    try:
        result = svc.delete(session, request.match_info["transfer_id"])
    except ResourceError as e:
        return web.json_response({"error": str(e)}, status=404)
    return web.json_response(result)


# ---------- WS ----------


async def ws_send(conn, msg: dict) -> None:
    if conn.session is None:
        return
    svc: ResourcesService = conn.app["resources"]
    link_id = msg.get("link_id")
    data_b64 = msg.get("data_b64")
    metadata = msg.get("metadata")
    auto_compress = bool(msg.get("auto_compress", True))
    if not isinstance(link_id, str) or not isinstance(data_b64, str):
        await conn.send_json({"type": "error", "error": "link_id and data_b64 required", "id": msg.get("id")})
        return
    try:
        payload = base64.b64decode(data_b64, validate=True)
    except Exception as e:
        await conn.send_json({"type": "error", "error": f"data_b64 invalid base64: {e}", "id": msg.get("id")})
        return
    try:
        result = await svc.send(
            conn.session,
            link_id,
            payload,
            metadata=metadata,
            auto_compress=auto_compress,
            await_complete=False,
        )
    except ResourceError as e:
        await conn.send_json({"type": "error", "error": str(e), "id": msg.get("id")})
        return
    await conn.send_json({"type": "resource.send.result", "id": msg.get("id"), **result})


async def ws_list(conn, msg: dict) -> None:
    if conn.session is None:
        return
    svc: ResourcesService = conn.app["resources"]
    await conn.send_json(
        {"type": "resource.list.result", "id": msg.get("id"), "resources": svc.list_transfers(conn.session)}
    )


async def ws_status(conn, msg: dict) -> None:
    if conn.session is None:
        return
    svc: ResourcesService = conn.app["resources"]
    tid = msg.get("transfer_id")
    if not isinstance(tid, str):
        await conn.send_json({"type": "error", "error": "transfer_id required", "id": msg.get("id")})
        return
    try:
        state = svc.get_state(conn.session, tid)
    except ResourceError as e:
        await conn.send_json({"type": "error", "error": str(e), "id": msg.get("id")})
        return
    await conn.send_json({"type": "resource.status.result", "id": msg.get("id"), **state.to_dict()})


async def ws_cancel(conn, msg: dict) -> None:
    if conn.session is None:
        return
    svc: ResourcesService = conn.app["resources"]
    tid = msg.get("transfer_id")
    if not isinstance(tid, str):
        await conn.send_json({"type": "error", "error": "transfer_id required", "id": msg.get("id")})
        return
    try:
        result = svc.delete(conn.session, tid)
    except ResourceError as e:
        await conn.send_json({"type": "error", "error": str(e), "id": msg.get("id")})
        return
    await conn.send_json({"type": "resource.cancel.result", "id": msg.get("id"), **result})


async def ws_policy(conn, msg: dict) -> None:
    if conn.session is None:
        return
    svc: ResourcesService = conn.app["resources"]
    link_id = msg.get("link_id")
    accept = msg.get("accept")
    if not isinstance(link_id, str) or not isinstance(accept, bool):
        await conn.send_json({"type": "error", "error": "link_id and accept required", "id": msg.get("id")})
        return
    try:
        result = svc.set_link_policy(conn.session, link_id, accept)
    except ResourceError as e:
        await conn.send_json({"type": "error", "error": str(e), "id": msg.get("id")})
        return
    await conn.send_json({"type": "resource.policy.result", "id": msg.get("id"), **result})


def register(app: web.Application) -> None:
    app.router.add_post("/links/{id}/resources", rest_send_resource)
    app.router.add_post("/links/{id}/resources/policy", rest_link_policy)
    app.router.add_get("/links/{id}/resources", rest_list_link_resources)
    app.router.add_get("/resources", rest_list_resources)
    app.router.add_get("/resources/{transfer_id}", rest_get_resource)
    app.router.add_get("/resources/{transfer_id}/data", rest_download_resource)
    app.router.add_delete("/resources/{transfer_id}", rest_delete_resource)

    router = app["ws_router"]
    router.register("resource.send", ws_send)
    router.register("resource.list", ws_list)
    router.register("resource.status", ws_status)
    router.register("resource.cancel", ws_cancel)
    router.register("resource.policy", ws_policy)
