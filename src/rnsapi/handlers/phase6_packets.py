"""REST and WS handlers for packet listeners and packet sends.

REST:
- POST   /packets/listen                  body: {destination_hash}
- DELETE /packets/listen/{hash}
- GET    /packets/listen                  — list current listeners
- POST   /packets/send                    body: {identity_hash, app_name, aspects, data_b64, proof_timeout?}

WS:
- packets.listen        params: {destination_hash}
- packets.unlisten      params: {destination_hash}
- packets.listeners     — reply with list
- packets.send          params: {identity_hash, app_name, aspects, data_b64, proof_timeout?}

Server-emitted events (session-scoped):
- packet.received            — a packet arrived on a listened destination
- packet.sent                — a packet was sent from this session
- packet.receipt.delivered   — a proof of delivery arrived
- packet.receipt.failed      — the receipt timed out or failed
"""
from __future__ import annotations

from aiohttp import web

from ..rns.packets import PacketError, PacketsService


async def rest_listen(request: web.Request) -> web.Response:
    svc: PacketsService = request.app["packets"]
    session = request["session"]
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"error": "invalid_json"}, status=400)
    dest = body.get("destination_hash")
    if not isinstance(dest, str):
        return web.json_response({"error": "destination_hash required"}, status=400)
    try:
        result = svc.listen(session, dest)
    except PacketError as e:
        status = 404 if "not owned" in str(e) else 400
        return web.json_response({"error": str(e)}, status=status)
    return web.json_response(result, status=201)


async def rest_unlisten(request: web.Request) -> web.Response:
    svc: PacketsService = request.app["packets"]
    session = request["session"]
    hash_hex = request.match_info["hash"]
    try:
        result = svc.unlisten(session, hash_hex)
    except PacketError as e:
        return web.json_response({"error": str(e)}, status=404)
    return web.json_response(result)


async def rest_list_listeners(request: web.Request) -> web.Response:
    svc: PacketsService = request.app["packets"]
    session = request["session"]
    return web.json_response({"destination_hashes": svc.list_listeners(session)})


async def rest_send(request: web.Request) -> web.Response:
    svc: PacketsService = request.app["packets"]
    session = request["session"]
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"error": "invalid_json"}, status=400)
    identity_hash = body.get("identity_hash")
    app_name = body.get("app_name")
    aspects = body.get("aspects", [])
    data_b64 = body.get("data_b64")
    proof_timeout = body.get("proof_timeout")
    if not isinstance(identity_hash, str):
        return web.json_response({"error": "identity_hash required"}, status=400)
    if not isinstance(app_name, str):
        return web.json_response({"error": "app_name required"}, status=400)
    if not isinstance(aspects, list):
        return web.json_response({"error": "aspects must be a list"}, status=400)
    if not isinstance(data_b64, str):
        return web.json_response({"error": "data_b64 required"}, status=400)
    if proof_timeout is not None and not isinstance(proof_timeout, (int, float)):
        return web.json_response({"error": "proof_timeout must be a number"}, status=400)
    try:
        result = await svc.send(session, identity_hash, app_name, aspects, data_b64, proof_timeout=proof_timeout)
    except PacketError as e:
        # 404 when the identity isn't recallable, 400 for validation errors
        status = 404 if "no known identity" in str(e) else 400
        return web.json_response({"error": str(e)}, status=status)
    return web.json_response(result)


async def ws_listen(conn, msg: dict) -> None:
    if conn.session is None:
        return
    svc: PacketsService = conn.app["packets"]
    dest = msg.get("destination_hash")
    if not isinstance(dest, str):
        await conn.send_json({"type": "error", "error": "destination_hash required", "id": msg.get("id")})
        return
    try:
        result = svc.listen(conn.session, dest)
    except PacketError as e:
        await conn.send_json({"type": "error", "error": str(e), "id": msg.get("id")})
        return
    await conn.send_json({"type": "packets.listen.result", "id": msg.get("id"), **result})


async def ws_unlisten(conn, msg: dict) -> None:
    if conn.session is None:
        return
    svc: PacketsService = conn.app["packets"]
    dest = msg.get("destination_hash")
    if not isinstance(dest, str):
        await conn.send_json({"type": "error", "error": "destination_hash required", "id": msg.get("id")})
        return
    try:
        result = svc.unlisten(conn.session, dest)
    except PacketError as e:
        await conn.send_json({"type": "error", "error": str(e), "id": msg.get("id")})
        return
    await conn.send_json({"type": "packets.unlisten.result", "id": msg.get("id"), **result})


async def ws_listeners(conn, msg: dict) -> None:
    if conn.session is None:
        return
    svc: PacketsService = conn.app["packets"]
    await conn.send_json(
        {
            "type": "packets.listeners.result",
            "id": msg.get("id"),
            "destination_hashes": svc.list_listeners(conn.session),
        }
    )


async def ws_send(conn, msg: dict) -> None:
    if conn.session is None:
        return
    svc: PacketsService = conn.app["packets"]
    identity_hash = msg.get("identity_hash")
    app_name = msg.get("app_name")
    aspects = msg.get("aspects", [])
    data_b64 = msg.get("data_b64")
    proof_timeout = msg.get("proof_timeout")
    if not isinstance(identity_hash, str):
        await conn.send_json({"type": "error", "error": "identity_hash required", "id": msg.get("id")})
        return
    if not isinstance(app_name, str):
        await conn.send_json({"type": "error", "error": "app_name required", "id": msg.get("id")})
        return
    if not isinstance(aspects, list):
        await conn.send_json({"type": "error", "error": "aspects must be a list", "id": msg.get("id")})
        return
    if not isinstance(data_b64, str):
        await conn.send_json({"type": "error", "error": "data_b64 required", "id": msg.get("id")})
        return
    try:
        result = await svc.send(conn.session, identity_hash, app_name, aspects, data_b64, proof_timeout=proof_timeout)
    except PacketError as e:
        await conn.send_json({"type": "error", "error": str(e), "id": msg.get("id")})
        return
    await conn.send_json({"type": "packets.send.result", "id": msg.get("id"), **result})


def register(app: web.Application) -> None:
    app.router.add_post("/packets/listen", rest_listen)
    app.router.add_delete("/packets/listen/{hash}", rest_unlisten)
    app.router.add_get("/packets/listen", rest_list_listeners)
    app.router.add_post("/packets/send", rest_send)

    router = app["ws_router"]
    router.register("packets.listen", ws_listen)
    router.register("packets.unlisten", ws_unlisten)
    router.register("packets.listeners", ws_listeners)
    router.register("packets.send", ws_send)
