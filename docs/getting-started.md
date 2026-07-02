# Getting Started

This walkthrough takes you from an empty Reticulum installation to
exchanging traffic over an RNS Link — all via the `rnsapid` REST + WS API.

We assume:

- Python 3.10+ is available.
- You already have `RNS` installed (`pip install rns`).
- You are on a mesh — that is, you have at least one working RNS
  interface configured in `~/.config/reticulum/config`. If you don't yet,
  the [Reticulum installation guide](https://reticulum.network/manual/gettingstartedfast.html)
  walks you through it.

Every command in this guide uses plain `curl` and (for the WebSocket
sections) `wscat`. Install `wscat` with `npm install -g wscat` if you
don't have it.

## 1. Install and bootstrap

```bash
git clone <this repo>
cd ReticulumAPI
python3 -m venv .venv
.venv/bin/pip install -e .[test]

.venv/bin/rnsapid --init
```

`rnsapid --init` writes `~/.config/rnsapi/config` with sensible defaults
and creates `identities/`, `certs/`, `logs/` under `~/.config/rnsapi/`.

## 2. Run the daemon

```bash
.venv/bin/rnsapid
```

You'll see:

```
rnsapid 0.1.0 starting
starting RNS.Reticulum (configdir=<default>, log_level=3)
RNS.Reticulum started
self-signed cert SHA-256 fingerprint: <hex>
listening on https://127.0.0.1:8000
```

Leave that terminal running.

## 3. Talk to the daemon

In another terminal:

```bash
curl -k https://127.0.0.1:8000/health
# {"status": "ok"}

curl -k https://127.0.0.1:8000/version
# {"name": "rnsapid", "version": "0.1.0", "protocol": {"rest": 1, "ws": 1}}
```

The `-k` flag tells curl to skip cert verification — the default cert is
self-signed. To view the fingerprint (so you can pin it in a browser or
verify it out-of-band):

```bash
.venv/bin/rnsapid --print-cert-fingerprint
```

## 4. Create an identity

```bash
curl -k -X POST https://127.0.0.1:8000/identities
# {"hash": "...", "public_key": "...", "path": "/Users/.../identities/....rid"}
```

The `hash` field is your new identity's 16-byte truncated hash (as hex).
Save it in a shell variable:

```bash
IDENT=$(curl -sk -X POST https://127.0.0.1:8000/identities \
        | python3 -c "import json,sys; print(json.load(sys.stdin)['hash'])")
echo "$IDENT"
```

## 5. Set it active on your session

```bash
curl -k -X PUT -H 'Content-Type: application/json' \
     -d "{\"hash\":\"$IDENT\"}" \
     https://127.0.0.1:8000/session/active-identity
# {"active": true, "hash": "...", "public_key": "...", ...}
```

Every subsequent destination and link you register belongs to this
identity until you clear it (`DELETE /session/active-identity`) or your
session ends.

## 6. Register a destination

```bash
curl -k -X POST -H 'Content-Type: application/json' \
     -d '{"direction":"in","type":"single","app_name":"myapp","aspects":["hello"]}' \
     https://127.0.0.1:8000/destinations
# {"hash": "...", "identity_hash": "...", "direction": "in", "type": "single",
#  "app_name": "myapp", "aspects": ["hello"]}
```

## 7. Send an announce

```bash
DEST=$(curl -sk https://127.0.0.1:8000/destinations \
       | python3 -c "import json,sys; print(json.load(sys.stdin)['destinations'][0]['hash'])")

curl -k -X POST -H 'Content-Type: application/json' \
     -d "{\"destination_hash\":\"$DEST\",\"app_data_b64\":\"$(printf 'hi from rnsapid' | base64)\"}" \
     https://127.0.0.1:8000/announce
# {"ok": true, "destination_hash": "...", "app_data_bytes": 15}
```

Any RNS peer on your mesh listening for announces on this app_name will
see it arrive.

## 8. Watch server events on WebSocket

Open a WS listener in another terminal:

```bash
wscat -n -c wss://127.0.0.1:8000/ws
```

You'll receive:

```
{"type":"auth.session.attached","session_id":"...","is_anonymous":true}
{"type":"auth.session.connected","session_id":"...","connection_id":"..."}
```

Now re-run the `POST /announce` in the first terminal — the WS terminal
will print an `announce.sent` event:

```json
{
  "type": "announce.sent",
  "destination_hash": "...",
  "identity_hash": "...",
  "session_id": "...",
  "app_data_b64": "aGkgZnJvbSBybnNhcGlk"
}
```

If any peer on your mesh announces itself while `wscat` is open, you'll
also see `announce.received` events fanned out globally.

## 9. Query the path table

Once you've seen announces from other peers, RNS knows how to route to
them:

```bash
curl -k https://127.0.0.1:8000/paths
# {"paths": [{"hash":"...","via":"...","hops":3,"interface":"..."}, ...]}
```

Filter by destination or interface:

```bash
curl -k "https://127.0.0.1:8000/paths?destination=abc123..."
curl -k "https://127.0.0.1:8000/paths?interface=AutoInterface[default]"
```

## 10. Request a path synchronously

If you know a destination hash but don't have a path yet, block until one
arrives (or time out):

```bash
curl -k -X POST -H 'Content-Type: application/json' \
     -d '{"destination_hash":"abc123...","timeout":30}' \
     https://127.0.0.1:8000/paths/request
# 200: {"found": true, "destination_hash":"...", "hops":..., "next_hop":"...", "interface":"..."}
# 408: {"found": false, "destination_hash":"..."}
```

While the request is outstanding, your session's WS receives a
`path.request.sent` event.

## 11. Send a packet

Given a known target identity + app_name + aspects, encrypt and dispatch a
Packet:

```bash
curl -k -X POST -H 'Content-Type: application/json' \
     -d "{
       \"identity_hash\":\"$IDENT\",
       \"app_name\":\"myapp\",
       \"aspects\":[\"messaging\"],
       \"data_b64\":\"$(printf 'hello over packet' | base64)\"
     }" \
     https://127.0.0.1:8000/packets/send
```

Your WS receives `packet.sent`. If the target replies with a delivery
proof, you'll also see `packet.receipt.delivered`.

## 12. Listen for incoming packets

On a destination you own:

```bash
curl -k -X POST -H 'Content-Type: application/json' \
     -d "{\"destination_hash\":\"$DEST\"}" \
     https://127.0.0.1:8000/packets/listen
# {"ok": true, "destination_hash": "..."}
```

Every packet delivered to that destination now fires a `packet.received`
event on your session's WS connections.

## 13. Open a Link

Links are long-lived encrypted channels with forward secrecy.

```bash
curl -k -X POST -H 'Content-Type: application/json' \
     -d "{
       \"identity_hash\":\"$IDENT\",
       \"app_name\":\"myapp\",
       \"aspects\":[\"messaging\"],
       \"await_established\":true,
       \"establishment_timeout\":15
     }" \
     https://127.0.0.1:8000/links
# 201: {"reused":false, "awaited":true, "link_id":"...", "status":"ACTIVE", ...}
```

If you set `"await_established": false`, the response returns immediately
with the link in `PENDING` state. Watch the WS for `link.established`.

## 14. Send data over a Link

```bash
LID=<link_id from step 13>

curl -k -X POST -H 'Content-Type: application/json' \
     -d "{\"data_b64\":\"$(printf 'ping' | base64)\"}" \
     https://127.0.0.1:8000/links/$LID/data
```

Every data frame in and out fires `link.data.sent` or `link.data.received`
on the WS.

## 15. Make an RPC-style request over the Link

If the remote side has registered a request handler (via
`destination.register_request_handler`), you can send an awaited request:

```bash
curl -k -X POST -H 'Content-Type: application/json' \
     -d '{"path":"/echo","data_b64":"aGVsbG8=","timeout":10}' \
     https://127.0.0.1:8000/links/$LID/request
# 200: {"ok": true, "kind": "response", "response_b64": "...", "size": ...}
# 408: request timed out
# 502: remote reported failure
```

## 16. Close the Link

```bash
curl -k -X DELETE https://127.0.0.1:8000/links/$LID
```

You'll see `link.closed` on the WS.

## 17. Enabling authentication

For production or multi-tenant deployments, turn on bearer-token auth:

```bash
.venv/bin/rnsapid --hash-password
# password: ***
# confirm:  ***
# scrypt$16384$8$1$...$...
```

Copy that hash into `~/.config/rnsapi/config`:

```ini
[auth]
enabled = true
username = admin
password_hash = scrypt$16384$8$1$...$...
```

Restart `rnsapid`, then login:

```bash
curl -k -X POST -H 'Content-Type: application/json' \
     -d '{"username":"admin","password":"the-password"}' \
     https://127.0.0.1:8000/auth/login
# {"token":"...", "session_id":"...", "auth_required":true, "is_anonymous":false}
```

Every subsequent REST call must include `Authorization: Bearer <token>`.
WS clients send `{"type":"auth","token":"..."}` as their first frame
within 5 seconds of connecting.

## Where to next

- [API Reference](api-reference.md) — the full endpoint and event catalog.
- [Configuration](config.md) — every INI key.
- [CLAUDE.md](../CLAUDE.md) — architecture notes for contributors.
