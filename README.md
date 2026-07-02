# ReticulumAPI

A REST + WebSocket API daemon that exposes the full [Reticulum Network
Stack](https://reticulum.network/) (RNS) service surface — identities,
destinations, announces, paths, packets, and links — over HTTP(S) and WSS.

`rnsapid` runs as a normal RNS client application: it imports the installed
`RNS` package and calls `RNS.Reticulum(...)`. By default it shares the user's
existing Reticulum configuration under `~/.config/reticulum/`, so the daemon
participates in the same mesh as any other RNS app on the machine.

**Status:** in development. See `docs/api-reference.md` for the current
endpoint surface.

## Design

- **One process, one port** — REST and WebSocket share the same TCP port and
  the same aiohttp application; every REST endpoint has a WS counterpart
  where a request/response pattern doesn't fit.
- **Secure by default** — TLS is on by default with an auto-generated
  self-signed cert; user-provided certs and plaintext (development only) are
  opt-in.
- **Localhost by default** — the daemon binds to `127.0.0.1`; bind to a
  different address (or `0.0.0.0`) via config.
- **Session-scoped** — clients authenticate once and hold a session that
  spans REST and WS calls. Long-lived resources (destinations, links)
  belong to the session that created them and are cleaned up automatically
  when the session ends.

## Quickstart

```bash
pip install -e .[test]
rnsapid --init        # writes ~/.config/rnsapi/config
rnsapid               # starts on https://127.0.0.1:8000
```

To print the self-signed cert's SHA-256 fingerprint (useful for verifying it
in a browser prompt):

```bash
rnsapid --print-cert-fingerprint
```

For a plaintext development listener alongside TLS, set `[network]
allow_http = true` in the config.

## Configuration

See `docs/config.md`.

## Testing

```bash
pip install -e .[test]
pytest -q
```

## License

MIT.
