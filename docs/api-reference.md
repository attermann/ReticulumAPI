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
