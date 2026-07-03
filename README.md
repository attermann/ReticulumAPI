# ReticulumAPI

A REST + WebSocket API daemon that exposes the full [Reticulum Network
Stack](https://reticulum.network/) (RNS) service surface — identities,
destinations, announces, paths, packets, and links — over HTTP(S) and WS(S).

`rnsapid` runs as a normal RNS client application: it imports the installed
`RNS` package and calls `RNS.Reticulum(...)`. By default it shares the user's
existing Reticulum configuration under `~/.config/reticulum/`, so the daemon
participates in the same mesh as any other RNS app on the machine.

## Why

Reticulum is a Python-native cryptography-based mesh protocol. Its only
programmatic surface is the `RNS` Python API, which locks out non-Python
clients — web apps, Electron shells, mobile front-ends, and services
written in other languages. `rnsapid` bridges the gap: any HTTP or
WebSocket client can now use every RNS service.

## Design philosophy

- **One process, one port.** REST and WebSocket share the same TCP port and
  the same aiohttp application. Every REST endpoint has a WS counterpart
  where a request/response pattern makes sense; the two paths never
  duplicate logic — they call the same underlying service class.
- **Secure by default.** TLS is on by default with an auto-generated
  self-signed cert. User-provided certs and plaintext (development only) are
  opt-in.
- **Localhost by default.** The daemon binds to `127.0.0.1`; bind to a
  different address (or `0.0.0.0`) via config.
- **Session-scoped.** Clients authenticate once and hold a session that
  spans REST and WS calls. Long-lived resources (destinations, links,
  packet listeners) belong to the session that created them and are
  cleaned up automatically when the session ends.
- **RNS is treated as read-only.** No monkey-patching, no runtime mutation
  of RNS internals, no source edits. `rnsapid` is *just another RNS client
  app*.

## Feature surface

| Area                       | Endpoints                                                                       |
| -------------------------- | ------------------------------------------------------------------------------- |
| Network interface          | `/health`, `/version`, `/ws`                                                    |
| Sessions                   | `/auth/login`, `/auth/logout`, `/session`, WS first-frame auth                  |
| Identities & destinations  | `/identities`, `/session/active-identity`, `/destinations`                      |
| Announces                  | `/announce`, global announce listener → `announce.received` broadcast           |
| Paths                      | `/paths` (queries), `/paths/request` (awaited)                                  |
| Packets                    | `/packets/listen`, `/packets/send` with receipt tracking                        |
| Links                      | `/links` (open/close/status), `/links/{id}/{data,request,identify}`             |
| Resources                  | `/links/{id}/resources` (send), `/resources/{id}/data` (streamed download)      |

## Quickstart

```bash
git clone <this repo>
cd ReticulumAPI
python3 -m venv .venv
.venv/bin/pip install -e .[test]

.venv/bin/rnsapid --init          # writes ~/.config/rnsapi/config
.venv/bin/rnsapid                  # starts on https://127.0.0.1:8000
```

Verify:

```bash
curl -k https://127.0.0.1:8000/health
# {"status": "ok"}
```

To print the self-signed cert's SHA-256 fingerprint (useful for verifying
it in a browser prompt):

```bash
.venv/bin/rnsapid --print-cert-fingerprint
```

To enable authentication:

```bash
# Generate a password hash and paste it into config
.venv/bin/rnsapid --hash-password
# Then set [auth] enabled = true and password_hash = <the hash>
```

For a plaintext development listener alongside TLS, set `[network]
allow_http = true` in the config.

## Documentation

- **[Getting Started](docs/getting-started.md)** — a hands-on walkthrough
  from `rnsapid --init` to opening a Link over the API.
- **[API Reference](docs/api-reference.md)** — every REST endpoint and WS
  message type, with request/response schemas.
- **[Configuration](docs/config.md)** — every INI key.
- **[CLAUDE.md](CLAUDE.md)** — architecture, invariants, and contributor
  guidance (also read by Claude Code when working on the project).

## Development

```bash
.venv/bin/pytest -q
```

The test suite (~120 tests) covers unit tests for every service and one
integration smoke test per feature area against a real in-process
`RNS.Reticulum` instance.

## License

Apache-2.0
