# API Reference

`rnsapid` exposes REST and WebSocket endpoints on a shared port. Both
protocols share the same authentication, session model, and permission set.

The endpoints listed below reflect the current phase of implementation. Later
phases add more endpoints; the file grows as those land.

## Common conventions

- **Binary data** is encoded as base64 in JSON fields named with a `_b64`
  suffix (e.g. `data_b64`, `payload_b64`).
- **Hashes** (destination hashes, identity hashes, link IDs) are lowercase
  hex strings unless noted otherwise.
- **WS messages** are JSON objects with a `type` field. Requests may include
  a client-generated `id` field, which is echoed on the response frame.

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

Revokes the current session, closes its WebSocket connections, and (in later
phases) tears down its owned destinations and links. Requires a valid bearer
token unless auth is disabled.

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
`POST /destinations` and later phases (announces, path requests, packets,
links).

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

## WS message types (Phase 3)

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
