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

## WebSocket endpoints

### `GET /ws`

WebSocket entrypoint. Phase 1 echoes every text/binary frame back to the
client. Phase 2 replaces this with the session-authenticated router that
dispatches inbound frames by `type` and fans out server-initiated events.
