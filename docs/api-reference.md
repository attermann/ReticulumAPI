# API Reference

`rnsapid` exposes REST and WebSocket endpoints on a shared port. Both
protocols share the same authentication, session model, and permission set.

## Contents

- [Common conventions](#common-conventions)
- [Authentication](#authentication)
- [REST endpoints](#rest-endpoints) — `/health`, `/version`, `/auth/*`, `/session`
- [WebSocket endpoints](#websocket-endpoints) — `/ws` lifecycle + `ping`/`session.info`/`auth.logout`
- [Identities](#identities) — `/identities`, `/identities/{hash}`
- [Active identity](#active-identity) — `/session/active-identity`
- [Destinations](#destinations) — `/destinations`
- [Announces](#announces) — `/announce` + global `announce.*` events
- [Paths](#paths) — `/paths`, `/paths/request`
- [Packets](#packets) — `/packets/listen`, `/packets/send`, receipt events
- [Links](#links) — `/links/*`, all Link lifecycle events
- [Resources](#resources) — `/links/{id}/resources`, `/resources/*`, progress + download

### Endpoint index

| Method | Path                                | Feature area           |
| ------ | ----------------------------------- | ---------------------- |
| GET    | `/health`                           | Network                |
| GET    | `/version`                          | Network                |
| GET    | `/ws`                               | Network / Sessions     |
| POST   | `/auth/login`                       | Sessions               |
| POST   | `/auth/logout`                      | Sessions               |
| GET    | `/session`                          | Sessions               |
| POST   | `/identities`                       | Identities             |
| GET    | `/identities`                       | Identities             |
| GET    | `/identities/{hash}`                | Identities             |
| GET    | `/session/active-identity`          | Identities             |
| PUT    | `/session/active-identity`          | Identities             |
| DELETE | `/session/active-identity`          | Identities             |
| GET    | `/destinations`                     | Destinations           |
| POST   | `/destinations`                     | Destinations           |
| DELETE | `/destinations/{hash}`              | Destinations           |
| POST   | `/announce`                         | Announces              |
| GET    | `/paths`                            | Paths                  |
| POST   | `/paths/request`                    | Paths                  |
| POST   | `/packets/listen`                   | Packets                |
| DELETE | `/packets/listen/{hash}`            | Packets                |
| GET    | `/packets/listen`                   | Packets                |
| POST   | `/packets/send`                     | Packets                |
| POST   | `/links`                            | Links                  |
| GET    | `/links`                            | Links                  |
| GET    | `/links/{id}`                       | Links                  |
| DELETE | `/links/{id}`                       | Links                  |
| POST   | `/links/{id}/identify`              | Links                  |
| POST   | `/links/{id}/data`                  | Links                  |
| POST   | `/links/{id}/request`               | Links                  |
| POST   | `/links/{id}/resources`             | Resources              |
| POST   | `/links/{id}/resources/policy`      | Resources              |
| GET    | `/links/{id}/resources`             | Resources              |
| GET    | `/resources`                        | Resources              |
| GET    | `/resources/{transfer_id}`          | Resources              |
| GET    | `/resources/{transfer_id}/data`     | Resources              |
| DELETE | `/resources/{transfer_id}`          | Resources              |

## Common conventions

- **Binary data** is encoded as base64 in JSON fields named with a `_b64`
  suffix (e.g. `data_b64`, `payload_b64`).
- **Hashes** (destination hashes, identity hashes, link IDs) are lowercase
  hex strings unless noted otherwise.
- **WS messages** are JSON objects with a `type` field. Requests may include
  a client-generated `id` field, which is echoed on the response frame.
- **All timestamps** are Unix epoch seconds (float).

## Authentication

Authentication is off by default (`[auth] enabled = false`); every endpoint
is reachable without credentials and a shared *anonymous* session is used.

When `[auth] enabled = true`:

- REST clients call `POST /auth/login` with `{"username":"...", "password":"..."}`,
  receive an opaque bearer token, and send it on all subsequent calls as
  `Authorization: Bearer <token>`.
- WebSocket clients open `/ws` and must send `{"type":"auth","token":"..."}`
  as their **first frame** within `[auth] ws_auth_frame_timeout` seconds. On
  success the server replies with `auth.session.attached`. On failure the
  connection is closed with WebSocket status code 4001.

To generate the password hash to paste into config, run:

```
rnsapid --hash-password
```

## REST endpoints

### `GET /health`

Liveness probe.

Response `200 OK`:

```json
{"status": "ok"}
```

### `GET /version`

Reports the daemon version and protocol revisions.

Response `200 OK`:

```json
{
  "name": "rnsapid",
  "version": "0.1.0",
  "protocol": {"rest": 1, "ws": 1}
}
```

### `POST /auth/login`

With auth **enabled**:

Request:

```json
{"username": "admin", "password": "..."}
```

Response `200 OK`:

```json
{
  "token": "...",
  "session_id": "...",
  "auth_required": true,
  "is_anonymous": false
}
```

`401 Unauthorized` on bad credentials. `400 Bad Request` on malformed JSON.

With auth **disabled**, the request body is ignored and the shared anonymous
session's token is returned with `auth_required: false, is_anonymous: true`.

### `POST /auth/logout`

Revokes the current session, closes its WebSocket connections, and tears
down its owned destinations, links, packet listeners, and in-flight
resources. Requires a valid bearer token unless auth is disabled.

Response `200 OK`:

```json
{"ok": true}
```

### `GET /session`

Returns metadata about the current session.

Response `200 OK`:

```json
{
  "session_id": "...",
  "created_at": 0.0,
  "last_seen_at": 0.0,
  "is_anonymous": false,
  "ws_connections": 1
}
```

## WebSocket endpoints

### `GET /ws`

WebSocket entrypoint. The connection lifecycle is:

1. Client opens the socket.
2. If auth is enabled, the client sends `{"type":"auth","token":"..."}` as the
   first frame (within `ws_auth_frame_timeout` seconds). If auth is disabled,
   the client is attached to the anonymous session automatically.
3. Server sends `{"type":"auth.session.attached","session_id":"...", "is_anonymous":...}`.
4. Server broadcasts `{"type":"auth.session.connected","session_id":"...","connection_id":"..."}`
   to all connections belonging to the same session.
5. Subsequent frames are JSON objects with a `type` field; unknown types get
   an `{"type":"error","error":"unknown_type"}` reply.

Rejection close codes:

- `4001 auth_timeout` — no auth frame received in time.
- `4001 auth_required` — first frame was not an auth message.
- `4001 invalid_json` — first frame was not valid JSON.
- `4001 invalid_token` — token unknown or expired.

### WS message `ping`

Refreshes the session's `last_seen_at` and replies with a `pong`.

Request:

```json
{"type": "ping", "id": "optional-client-id"}
```

Reply:

```json
{"type": "pong", "id": "optional-client-id", "t": 1720000000.0}
```

### WS message `session.info`

Reply with the same JSON body as `GET /session`, wrapped as an event of type
`session.info`.

### WS message `auth.logout`

Same effect as `POST /auth/logout`. The server replies with
`{"type":"auth.logout.result","ok":true,"id":...}` and then closes the
connection with code 4001.

## Identities

Identities are stored as `.rid` files under
`~/.config/rnsapi/identities/<hash>.rid`. The file contains the private key
material and is loaded lazily when the identity is referenced.

### `POST /identities`

Generate a new identity, persist it, and return its (non-secret) fields.

Response `201 Created`:

```json
{
  "hash": "0123456789abcdef...",
  "public_key": "0123abc...",
  "path": "/Users/.../identities/0123....rid"
}
```

Also emits `{"type":"identity.created", ...}` to the session's WS
connections.

### `GET /identities`

```json
{"identities": [ { "hash": "...", "public_key": "...", "path": "..." } ]}
```

### `GET /identities/{hash}`

Same shape as one entry from `GET /identities`. `404 Not Found` if the
identity does not exist.

## Active identity

Every session has at most one active identity. It is used implicitly by
`POST /destinations`, announces, path requests, packet sends, and link
opens.

### `GET /session/active-identity`

```json
{"active": true, "hash": "...", "public_key": "...", "path": "..."}
```

Or `{"active": false}` when no identity is set.

### `PUT /session/active-identity`

Request:

```json
{"hash": "0123abc..."}
```

Response `200 OK` with the same shape as `GET`.

**`409 Conflict`** if the session already owns any destinations or open
links. The response body includes the current counts:

```json
{"error": "session_dirty", "owned_destinations": 2, "open_links": 0}
```

To switch identities, tear down all destinations and links first (or
logout + relogin).

### `DELETE /session/active-identity`

Clear the active identity. Same 409 rule as `PUT`.

## Destinations

Destinations are session-scoped: they belong to the session that created
them and are automatically deregistered from RNS when that session ends.

### `POST /destinations`

Request:

```json
{
  "direction": "in",
  "type": "single",
  "app_name": "myapp",
  "aspects": ["messaging", "v1"]
}
```

- `direction`: `in` or `out`
- `type`: `single` · `group` · `plain`
- `app_name` and `aspects`: RNS naming conventions — letters, digits,
  underscores only; no dots.

Requires an active identity on the session. Responds `409 Conflict` with
`{"error":"no_active_identity"}` otherwise.

Response `201 Created`:

```json
{
  "hash": "abcdef...",
  "identity_hash": "0123abc...",
  "direction": "in",
  "type": "single",
  "app_name": "myapp",
  "aspects": ["messaging", "v1"]
}
```

Emits `{"type":"destination.added", "destination": {...}}` to the session's
WS connections.

### `GET /destinations`

```json
{"destinations": [ { ... same shape as POST response ... } ]}
```

### `DELETE /destinations/{hash}`

Deregisters the destination from RNS. `404 Not Found` if the destination
isn't owned by this session. Emits `{"type":"destination.removed",
"destination": {...}}` to the session's WS.

## WS message types (identities & destinations)

| Inbound `type`                         | Reply / event                                         |
| -------------------------------------- | ----------------------------------------------------- |
| `identity.create`                      | server broadcasts `identity.created` to the session   |
| `identity.list`                        | reply: `identity.list.result`                         |
| `session.active_identity.get`          | reply: `session.active_identity.info`                 |
| `session.active_identity.set`          | server broadcasts `session.active_identity.changed`   |
| `session.active_identity.clear`        | server broadcasts `session.active_identity.changed`   |
| `destination.list`                     | reply: `destination.list.result`                      |
| `destination.add`                      | server broadcasts `destination.added`                 |
| `destination.remove`                   | server broadcasts `destination.removed`               |

Server-emitted events (session-scoped):

- `identity.created`                    — a new identity was created
- `session.active_identity.changed`     — active identity set or cleared
- `destination.added`                   — a destination was registered
- `destination.removed`                 — a destination was deregistered

## Announces

`rnsapid` registers a **global** announce listener with RNS at startup (with
no aspect filter), so every announce that reaches this node is fanned out
to *every* connected WebSocket regardless of session. Announces sent by
this daemon are also broadcast globally so all connected clients can
observe local activity.

### `POST /announce`

Broadcast an announce for a destination the current session owns.

Request:

```json
{
  "destination_hash": "abcdef...",
  "app_data_b64": "base64 of optional app data (omit or null for none)"
}
```

Response `200 OK`:

```json
{
  "ok": true,
  "destination_hash": "abcdef...",
  "app_data_bytes": 15
}
```

`404 Not Found` if the destination is not owned by this session.
`400 Bad Request` for malformed input.

### WS message `announce.send`

Same as the REST endpoint. Params:

```json
{
  "type": "announce.send",
  "id": "optional-client-id",
  "destination_hash": "abcdef...",
  "app_data_b64": null
}
```

The server replies with a targeted `announce.send.result` frame and, in
parallel, broadcasts `announce.sent` to every connected WS.

### Server-emitted announce events (global)

- `announce.received` — RNS delivered an announce to this node:

  ```json
  {
    "type": "announce.received",
    "destination_hash": "abcdef...",
    "identity_hash": "0123abc...",
    "app_data_b64": "base64 or null",
    "packet_hash": "0123abc... (if available)",
    "is_path_response": false
  }
  ```

- `announce.sent` — this daemon just sent an announce:

  ```json
  {
    "type": "announce.sent",
    "destination_hash": "abcdef...",
    "identity_hash": "0123abc...",
    "session_id": "id of the session that initiated the send",
    "app_data_b64": null
  }
  ```

## Paths

RNS routing paths (destination → next-hop-interface, hop count, expiry) can
be inspected via `/paths`, and new paths can be discovered by sending a
path-request over the network.

> **Limitation:** RNS exposes no public listener for *incoming* path-request
> packets, and `rnsapid` refuses to monkey-patch RNS internals. There is
> therefore no `path.request.received` event.

### `GET /paths`

Query the routing table. Query params:

| Param         | Meaning                                                    |
| ------------- | ---------------------------------------------------------- |
| `destination` | Return only the entry for this destination hash (32 hex).  |
| `interface`   | Return only entries reached via this interface name.       |
| `max_hops`    | Return only entries within this hop count.                 |

Response `200 OK`:

```json
{
  "paths": [
    {
      "hash":      "abcdef...",
      "via":       "0123ab...",
      "hops":      2,
      "interface": "AutoInterface[default]",
      "timestamp": 1720000000.0,
      "expires":   1720003600.0
    }
  ]
}
```

`400 Bad Request` on invalid parameters.

### `POST /paths/request`

Send a path-request for a destination and *await* the response (or timeout).

Request:

```json
{
  "destination_hash": "abcdef...",
  "timeout": 15
}
```

`timeout` is optional and falls back to `[limits] path_request_timeout`
from config.

Response `200 OK` when the path is discovered before the timeout:

```json
{
  "found": true,
  "destination_hash": "abcdef...",
  "hops": 3,
  "next_hop": "0123ab...",
  "interface": "AutoInterface[default]"
}
```

Response `408 Request Timeout` when no path was found before the deadline:

```json
{"found": false, "destination_hash": "abcdef..."}
```

While the request is outstanding, the server emits `path.request.sent` to
**the current session's** WS connections only:

```json
{
  "type": "path.request.sent",
  "session_id": "...",
  "destination_hash": "abcdef..."
}
```

### WS message `paths.query`

Same as `GET /paths`. Params match the query-string names.

Reply (targeted): `paths.query.result` with the same body as the REST reply.

### WS message `paths.request`

Same as `POST /paths/request` but asynchronous — the WS reply comes when
the request completes (found or timed out), and the session-only
`path.request.sent` event fires as with REST.

Reply (targeted): `paths.request.result` with `found` + path fields.

## Packets

Packet operations are session-scoped: listeners only fire on destinations
that belong to the current session, and receipt callbacks route back to
the session that initiated the send.

### `POST /packets/listen`

Attach a packet callback to one of your session's owned destinations. Every
subsequent packet delivered to that destination fires a session-only
`packet.received` event.

Request:

```json
{"destination_hash": "abcdef..."}
```

Response `201 Created`:

```json
{"ok": true, "destination_hash": "abcdef..."}
```

`404 Not Found` if the destination is not owned by this session.

### `DELETE /packets/listen/{hash}`

Detach the callback. `404 Not Found` if not currently listening.

### `GET /packets/listen`

```json
{"destination_hashes": ["abcdef...", ...]}
```

### `POST /packets/send`

Encrypt and send a Packet to a target identity. The target destination is
constructed at send time from the given identity hash + app_name + aspects
(same three fields that determine a destination's hash on the other side).

Request:

```json
{
  "identity_hash": "0123abc...",
  "app_name": "myapp",
  "aspects": ["messaging", "v1"],
  "data_b64": "base64 of the payload",
  "proof_timeout": 15
}
```

`proof_timeout` is optional; when set, adjusts how long the daemon holds
the `PacketReceipt` before firing `packet.receipt.failed`. The target
identity is resolved by:

1. Looking it up in the local identity store
   (`~/.config/rnsapi/identities/`), and if not found,
2. Calling `RNS.Identity.recall(...)` (which succeeds once an announce
   carrying this identity's public key has been received).

If neither succeeds, the endpoint returns `404 Not Found`.

Response `200 OK`:

```json
{
  "ok": true,
  "destination_hash": "computed hex hash of the OUT destination",
  "identity_hash": "0123abc...",
  "packet_hash": "hex of the packet hash, if RNS assigned one",
  "size": 42,
  "has_receipt": true
}
```

### WS message types (packets)

| Inbound `type`         | Reply / event                                                     |
| ---------------------- | ----------------------------------------------------------------- |
| `packets.listen`       | reply: `packets.listen.result`                                    |
| `packets.unlisten`     | reply: `packets.unlisten.result`                                  |
| `packets.listeners`    | reply: `packets.listeners.result`                                 |
| `packets.send`         | reply: `packets.send.result` + server broadcasts `packet.sent`    |

### Server-emitted packet events (session-scoped)

- `packet.received` — a packet arrived on a listened destination:

  ```json
  {
    "type": "packet.received",
    "session_id": "...",
    "destination_hash": "abcdef...",
    "data_b64": "base64 of plaintext",
    "size": 42,
    "packet_hash": "hex or null",
    "hops": 2,
    "rssi": null,
    "snr": null
  }
  ```

- `packet.sent` — this daemon just dispatched a packet:

  ```json
  {
    "type": "packet.sent",
    "session_id": "...",
    "destination_hash": "computed OUT dest hash",
    "identity_hash": "0123abc...",
    "packet_hash": "hex",
    "size": 42,
    "has_receipt": true
  }
  ```

- `packet.receipt.delivered` — a proof arrived for a packet we sent:

  ```json
  {
    "type": "packet.receipt.delivered",
    "session_id": "...",
    "destination_hash": "computed OUT dest hash",
    "packet_hash": "hex",
    "rtt": 0.427,
    "status": "DELIVERED"
  }
  ```

- `packet.receipt.failed` — the receipt timed out or otherwise failed:

  ```json
  {
    "type": "packet.receipt.failed",
    "session_id": "...",
    "destination_hash": "computed OUT dest hash",
    "packet_hash": "hex",
    "status": "FAILED"
  }
  ```

## Links

RNS Links are long-lived encrypted channels between two identities with
forward secrecy. `rnsapid` maintains a **per-session** Link cache keyed on
the target destination hash: within one session, subsequent `link.open`
calls to the same destination reuse the existing Link and every WS
connection currently attached to the session sees every Link event.

The client-facing `link_id` **is** the destination hash (hex).

### `POST /links`

Open a Link (or reuse an existing one). Request:

```json
{
  "identity_hash": "0123abc...",
  "app_name": "myapp",
  "aspects": ["messaging", "v1"],
  "auto_identify": false,
  "await_established": true,
  "establishment_timeout": 15.0,
  "path_lookup_timeout": 15.0
}
```

- `auto_identify` — if `true`, the daemon calls `link.identify(session's
  active identity)` after the link becomes ACTIVE.
- `await_established` — if `true` (default), the response is returned only
  after the link reaches ACTIVE (or the establishment timeout elapses).
  If `false`, the response comes back immediately with the link in PENDING
  and the client watches for the `link.established` event on WS.

Response `201 Created` when a new link was opened (or `200 OK` when an
existing link was reused):

```json
{
  "reused": false,
  "awaited": true,
  "link_id": "abcdef...",
  "destination_hash": "abcdef...",
  "aspect": "myapp.messaging.v1",
  "status": "ACTIVE",
  "mtu": 500,
  "mdu": 396,
  "remote_identity_hash": "0123abc... or null",
  "teardown_reason": null
}
```

Errors:

- `404 Not Found` — the target identity is unknown (announce it or path-request first).
- `408 Request Timeout` — `await_established=true` and the link never became ACTIVE.
- `400 Bad Request` — malformed input.

### `GET /links`

```json
{"links": [ { link_snapshot }, ... ]}
```

### `GET /links/{id}`

Return the current link snapshot. `404 Not Found` if the session doesn't own this link.

### `DELETE /links/{id}`

Tear down the link.

### `POST /links/{id}/identify`

Send the session's active identity to the remote side. `400 Bad Request` if
the session has no active identity.

### `POST /links/{id}/data`

Send a raw data packet over the link.

```json
{"data_b64": "..."}
```

Emits `link.data.sent` to the session.

### `POST /links/{id}/request`

Send an RPC-style request over the link and **await** the response.

```json
{
  "path": "/echo",
  "data_b64": "optional base64 payload",
  "timeout": 30
}
```

Response `200 OK`:

```json
{
  "ok": true,
  "link_id": "...",
  "path": "/echo",
  "kind": "response",
  "response_b64": "base64 of the response",
  "size": 42
}
```

Errors:

- `408 Request Timeout` — the request awaited but no response arrived.
- `502 Bad Gateway` — the remote reported the request failed.

### WS message types (links)

| Inbound `type`     | Reply / event                                        |
| ------------------ | ---------------------------------------------------- |
| `link.open`        | reply `link.open.result`                             |
| `link.close`       | reply `link.close.result`                            |
| `link.identify`    | reply `link.identify.result`                         |
| `link.send`        | reply `link.send.result` + event `link.data.sent`    |
| `link.request`     | reply `link.request.result` (acknowledgement) + later `link.request.response` / `link.request.failed` |
| `link.status`      | reply `link.status.result`                           |
| `link.list`        | reply `link.list.result`                             |

### Server-emitted link events (session-scoped)

- `link.established`      — link reached ACTIVE
- `link.closed`           — link torn down (any reason)
- `link.disconnected`     — link closed by remote (fires alongside `link.closed` when the teardown reason is not `initiator_closed`)
- `link.remote_identified` — remote sent us their identity via `link.identify`
- `link.data.received`    — an inbound packet arrived on the link
- `link.data.sent`        — we sent an outbound packet on the link
- `link.proof`            — reserved for future proof-tracking events (see `packet.receipt.*` for the current shape)
- `link.request.response` — an RPC response arrived
- `link.request.failed`   — an RPC request failed

Each event payload includes `session_id`, `link_id`, `destination_hash`,
and `aspect`. See `src/rnsapi/rns/links.py` for the exact schema.

## Resources

RNS Resources are the reliable-transport primitive on top of a Link:
segmentation, compression, request-window flow control, and integrity
checks for payloads larger than a single Packet's MDU.

**Direction:** both send and receive, over both REST and WebSocket.

**Streaming:** RNS accumulates all parts in memory then writes to disk
once assembly is complete — the daemon **cannot** expose partial received
bytes during transfer. What it does expose:

- **Live progress** via `resource.progress` events on WS (throttled).
- **Streamed download** via `GET /resources/{id}/data` once complete
  (aiohttp's `web.FileResponse` chunks the reply).
- **Optional inline `data_b64`** in the `resource.completed` event when
  the received bytes are smaller than `[resources] max_inline_bytes`
  (default 64 KB).

**Auto-accept:** every session-owned Link accepts incoming resources by
default (`RNS.Link.ACCEPT_ALL`). Opt out per-link with
`POST /links/{id}/resources/policy`.

**Retention:** completed transfer temp files live under
`~/.config/rnsapi/resources/` and are automatically deleted after
`[resources] retention_seconds` (default 3600). `DELETE
/resources/{transfer_id}` removes one immediately. Session teardown
removes them all.

### `POST /links/{id}/resources`

Send a resource. The request body is streamed to a temp file, then
`RNS.Resource(open(temp),link)` starts the transfer. **The Link must be
ACTIVE** — 409 Conflict otherwise.

Query parameters:

| Param            | Default   | Meaning                                                                 |
| ---------------- | --------- | ----------------------------------------------------------------------- |
| `await_complete` | `false`   | If `true`, block until the transfer completes.                          |
| `timeout`        | 300       | Await timeout in seconds.                                               |
| `auto_compress`  | `true`    | Passed through to `RNS.Resource`.                                       |
| `metadata`       |           | URL-encoded JSON to attach to the resource.                             |

Request:

```
POST /links/abcd.../resources?await_complete=false
Content-Type: application/octet-stream
<raw bytes ...>
```

Response `201 Created` (fire-and-forget):

```json
{
  "awaited": false,
  "transfer_id": "abc123...",
  "session_id": "...",
  "direction": "out",
  "link_id": "abcd...",
  "status": "QUEUED",
  "total_size": 5000,
  "bytes_transferred": 0,
  "progress": 0.0,
  "created_at": 1720000000.0,
  "metadata": null
}
```

Or `200 OK` when `await_complete=true` and the transfer reached COMPLETE.
`408 Request Timeout` on await timeout. `502 Bad Gateway` on FAILED /
CORRUPT. `409 Conflict` when the link isn't ACTIVE. `404 Not Found` when
the link isn't owned by this session.

### `POST /links/{id}/resources/policy`

Toggle whether the link accepts incoming resource advertisements.

```json
{"accept": true}
```

Response `200 OK` mirrors the request plus `link_id`.

### `GET /links/{id}/resources` — list transfers on this link

```json
{"resources": [ { transfer_snapshot }, ... ]}
```

### `GET /resources` — list all session transfers

Same shape.

### `GET /resources/{transfer_id}` — one transfer's metadata

Includes `download_url` when the transfer is inbound and COMPLETE.

### `GET /resources/{transfer_id}/data`

**Streamed download** of the assembled bytes via aiohttp `FileResponse`.

- `200 OK` with `Content-Type: application/octet-stream`,
  `Content-Disposition: attachment; filename="<transfer_id>"`.
- `404 Not Found` if the transfer doesn't exist, isn't owned by this
  session, is outbound, or isn't yet COMPLETE.
- `410 Gone` if the temp file has been swept.

### `DELETE /resources/{transfer_id}`

Cancels the transfer if in flight, deletes the temp file if complete, and
removes the transfer from session state. Fires `resource.failed` on WS.

### WS message types (resources)

| Inbound `type`         | Reply                                                            |
| ---------------------- | ---------------------------------------------------------------- |
| `resource.send`        | `resource.send.result`. Small-file send via `data_b64`.          |
| `resource.list`        | `resource.list.result`                                           |
| `resource.status`      | `resource.status.result`                                         |
| `resource.cancel`      | `resource.cancel.result`                                         |
| `resource.policy`      | `resource.policy.result`                                         |

### Server-emitted resource events (session-scoped)

- `resource.started` — a transfer began (inbound or outbound)
- `resource.progress` — periodic progress update (throttled to
  `[resources] progress_throttle_ms` and `progress_throttle_pct`)
- `resource.completed` — inbound transfer finished successfully. Includes
  `download_url`; also includes `data_b64` if the received size is
  ≤ `max_inline_bytes`.
- `resource.sent` — outbound transfer finished successfully
- `resource.failed` — status FAILED / CORRUPT / cancelled

Every event payload includes `session_id`, `transfer_id`, `link_id`,
`direction`, `status`, `total_size`, `bytes_transferred`, `progress`,
`metadata`, and (for terminal events) `completed_at`.
