# Configuration Reference

`rnsapid` reads its configuration from an INI file. The default location is:

```
~/.config/rnsapi/config
```

The path can be overridden with `--config /path/to/config`, and the entire
storage root (identities, certs, logs) can be relocated with `--home
/path/to/home` or by setting the `RNSAPI_HOME` environment variable.

To generate a starter config, run:

```
rnsapid --init
```

## Sections

### `[network]`

| Key         | Default     | Meaning                                                        |
| ----------- | ----------- | -------------------------------------------------------------- |
| `bind_host` | `127.0.0.1` | Address to bind. Use `0.0.0.0` to accept remote connections.   |
| `bind_port` | `8000`      | Primary port. Serves TLS when `tls=true`, plaintext otherwise. |
| `tls`       | `true`      | Whether the primary port uses TLS.                             |
| `allow_http`| `false`     | Also expose a plaintext port when TLS is enabled (dev only).   |
| `http_port` | `8001`      | Plaintext port when `allow_http=true`.                         |

### `[tls]`

| Key                | Default        | Meaning                                                       |
| ------------------ | -------------- | ------------------------------------------------------------- |
| `mode`             | `self_signed`  | `self_signed` · `user_provided` · `disabled`.                 |
| `cert_file`        |                | PEM cert path (required when `mode=user_provided`).           |
| `key_file`         |                | PEM key path (required when `mode=user_provided`).            |
| `self_signed_cn`   | `localhost`    | Common name embedded in the generated self-signed cert.       |

Self-signed certs are generated under `~/.config/rnsapi/certs/` on first run
and are reused thereafter. To print the SHA-256 fingerprint (useful for
verifying the cert in browser prompts), run:

```
rnsapid --print-cert-fingerprint
```

### `[auth]`

| Key                          | Default | Meaning                                                        |
| ---------------------------- | ------- | -------------------------------------------------------------- |
| `enabled`                    | `false` | When `true`, all endpoints require a bearer token from login.  |
| `username`                   | `admin` | Login username.                                                |
| `password_hash`              |         | Argon2 or bcrypt hash of the password (never store plaintext). |
| `session_inactivity_timeout` | `1800`  | Seconds of inactivity before a session is reaped.              |
| `session_max_lifetime`       | `86400` | Maximum session lifetime regardless of activity.               |
| `ws_auth_frame_timeout`      | `5`     | Seconds a WS client has to send its first-frame auth message.  |

Session management is implemented in Phase 2. In Phase 1, auth is a no-op
regardless of this setting.

### `[rns]`

| Key          | Default | Meaning                                                             |
| ------------ | ------- | ------------------------------------------------------------------- |
| `config_dir` |         | Empty = share the user's RNS config (`~/.config/reticulum`).        |
| `log_level`  | `3`     | RNS log verbosity (0 = critical, 7 = extreme).                      |

### `[storage]`

| Key              | Default            | Meaning                                            |
| ---------------- | ------------------ | -------------------------------------------------- |
| `root`           | `~/.config/rnsapi` | Storage root.                                      |
| `identities_dir` | `identities`       | Relative or absolute directory for `.rid` files.   |
| `certs_dir`      | `certs`            | Directory for self-signed certs.                   |
| `logs_dir`       | `logs`             | Directory for rotated log files.                   |

### `[logging]`

| Key                    | Default              | Meaning                              |
| ---------------------- | -------------------- | ------------------------------------ |
| `level`                | `INFO`               | Python log level.                    |
| `file`                 | `logs/rnsapid.log`   | Log file (relative to storage root). |
| `rotate_max_bytes`     | `10485760`           | Rotate at this size.                 |
| `rotate_backup_count`  | `5`                  | Keep this many rotated files.        |

### `[limits]`

| Key                       | Default    | Meaning                                        |
| ------------------------- | ---------- | ---------------------------------------------- |
| `path_request_timeout`    | `15`       | Seconds to wait for path establishment.        |
| `link_establish_timeout`  | `15`       | Seconds to wait for a Link to reach ACTIVE.    |
| `request_default_timeout` | `30`       | Default timeout for `link.request`.            |
| `max_ws_message_bytes`    | `1048576`  | Reject WS frames larger than this.             |
| `max_packet_bytes`        | `65535`    | Reject Packet payloads larger than this.       |
