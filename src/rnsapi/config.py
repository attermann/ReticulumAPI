"""INI configuration for rnsapid.

Loaded from ~/.config/rnsapi/config by default. `rnsapid --init` writes a
default file if none exists; the daemon otherwise refuses to silently create
one so that operators explicitly bootstrap their install.
"""
from __future__ import annotations

import configparser
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal


TlsMode = Literal["self_signed", "user_provided", "disabled"]


@dataclass
class NetworkConfig:
    bind_host: str = "127.0.0.1"
    bind_port: int = 8000
    tls: bool = True
    allow_http: bool = False
    http_port: int = 8001


@dataclass
class TlsConfig:
    mode: TlsMode = "self_signed"
    cert_file: str = ""
    key_file: str = ""
    self_signed_cn: str = "localhost"


@dataclass
class AuthConfig:
    enabled: bool = False
    username: str = "admin"
    password_hash: str = ""
    session_inactivity_timeout: int = 1800
    session_max_lifetime: int = 86400
    ws_auth_frame_timeout: int = 5


@dataclass
class RnsConfig:
    config_dir: str = ""  # empty = use RNS default (~/.config/reticulum)
    log_level: int = 3


@dataclass
class StorageConfig:
    root: str = "~/.config/rnsapi"
    identities_dir: str = "identities"
    certs_dir: str = "certs"
    logs_dir: str = "logs"


@dataclass
class LoggingConfig:
    level: str = "INFO"
    file: str = "logs/rnsapid.log"
    rotate_max_bytes: int = 10 * 1024 * 1024
    rotate_backup_count: int = 5


@dataclass
class LimitsConfig:
    path_request_timeout: int = 15
    link_establish_timeout: int = 15
    request_default_timeout: int = 30
    max_ws_message_bytes: int = 1024 * 1024
    max_packet_bytes: int = 65535


@dataclass
class Config:
    network: NetworkConfig = field(default_factory=NetworkConfig)
    tls: TlsConfig = field(default_factory=TlsConfig)
    auth: AuthConfig = field(default_factory=AuthConfig)
    rns: RnsConfig = field(default_factory=RnsConfig)
    storage: StorageConfig = field(default_factory=StorageConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    limits: LimitsConfig = field(default_factory=LimitsConfig)


DEFAULT_CONFIG_TEXT = """\
# rnsapid configuration
# See docs/config.md for the full reference.

[network]
bind_host = 127.0.0.1
bind_port = 8000
tls = true
# When true, also expose a plaintext HTTP+WS port (useful for development)
allow_http = false
http_port = 8001

[tls]
# Options: self_signed | user_provided | disabled
mode = self_signed
cert_file =
key_file =
self_signed_cn = localhost

[auth]
enabled = false
username = admin
# argon2 or bcrypt hash of the password; never store plaintext here.
password_hash =
session_inactivity_timeout = 1800
session_max_lifetime = 86400
ws_auth_frame_timeout = 5

[rns]
# Empty = use RNS's default (~/.config/reticulum). Overriding lets rnsapid run
# against its own private Reticulum config, isolated from other apps.
config_dir =
log_level = 3

[storage]
root = ~/.config/rnsapi
identities_dir = identities
certs_dir = certs
logs_dir = logs

[logging]
level = INFO
file = logs/rnsapid.log
rotate_max_bytes = 10485760
rotate_backup_count = 5

[limits]
path_request_timeout = 15
link_establish_timeout = 15
request_default_timeout = 30
max_ws_message_bytes = 1048576
max_packet_bytes = 65535
"""


def _get_bool(section, key: str, default: bool) -> bool:
    raw = section.get(key, "").strip().lower()
    if raw == "":
        return default
    if raw in ("true", "yes", "on", "1"):
        return True
    if raw in ("false", "no", "off", "0"):
        return False
    raise ValueError(f"[{section.name}] {key} must be a boolean, got {raw!r}")


def _get_int(section, key: str, default: int) -> int:
    raw = section.get(key, "").strip()
    if raw == "":
        return default
    return int(raw)


def _get_str(section, key: str, default: str) -> str:
    raw = section.get(key, "").strip()
    return raw if raw != "" else default


def load(path: Path) -> Config:
    """Parse the INI at *path* into a Config. Missing keys use dataclass defaults."""
    parser = configparser.ConfigParser()
    parser.read(path, encoding="utf-8")

    cfg = Config()

    if parser.has_section("network"):
        s = parser["network"]
        cfg.network = NetworkConfig(
            bind_host=_get_str(s, "bind_host", cfg.network.bind_host),
            bind_port=_get_int(s, "bind_port", cfg.network.bind_port),
            tls=_get_bool(s, "tls", cfg.network.tls),
            allow_http=_get_bool(s, "allow_http", cfg.network.allow_http),
            http_port=_get_int(s, "http_port", cfg.network.http_port),
        )

    if parser.has_section("tls"):
        s = parser["tls"]
        mode = _get_str(s, "mode", cfg.tls.mode)
        if mode not in ("self_signed", "user_provided", "disabled"):
            raise ValueError(f"[tls] mode must be self_signed|user_provided|disabled, got {mode!r}")
        cfg.tls = TlsConfig(
            mode=mode,  # type: ignore[arg-type]
            cert_file=_get_str(s, "cert_file", cfg.tls.cert_file),
            key_file=_get_str(s, "key_file", cfg.tls.key_file),
            self_signed_cn=_get_str(s, "self_signed_cn", cfg.tls.self_signed_cn),
        )

    if parser.has_section("auth"):
        s = parser["auth"]
        cfg.auth = AuthConfig(
            enabled=_get_bool(s, "enabled", cfg.auth.enabled),
            username=_get_str(s, "username", cfg.auth.username),
            password_hash=_get_str(s, "password_hash", cfg.auth.password_hash),
            session_inactivity_timeout=_get_int(s, "session_inactivity_timeout", cfg.auth.session_inactivity_timeout),
            session_max_lifetime=_get_int(s, "session_max_lifetime", cfg.auth.session_max_lifetime),
            ws_auth_frame_timeout=_get_int(s, "ws_auth_frame_timeout", cfg.auth.ws_auth_frame_timeout),
        )

    if parser.has_section("rns"):
        s = parser["rns"]
        cfg.rns = RnsConfig(
            config_dir=_get_str(s, "config_dir", cfg.rns.config_dir),
            log_level=_get_int(s, "log_level", cfg.rns.log_level),
        )

    if parser.has_section("storage"):
        s = parser["storage"]
        cfg.storage = StorageConfig(
            root=_get_str(s, "root", cfg.storage.root),
            identities_dir=_get_str(s, "identities_dir", cfg.storage.identities_dir),
            certs_dir=_get_str(s, "certs_dir", cfg.storage.certs_dir),
            logs_dir=_get_str(s, "logs_dir", cfg.storage.logs_dir),
        )

    if parser.has_section("logging"):
        s = parser["logging"]
        cfg.logging = LoggingConfig(
            level=_get_str(s, "level", cfg.logging.level),
            file=_get_str(s, "file", cfg.logging.file),
            rotate_max_bytes=_get_int(s, "rotate_max_bytes", cfg.logging.rotate_max_bytes),
            rotate_backup_count=_get_int(s, "rotate_backup_count", cfg.logging.rotate_backup_count),
        )

    if parser.has_section("limits"):
        s = parser["limits"]
        cfg.limits = LimitsConfig(
            path_request_timeout=_get_int(s, "path_request_timeout", cfg.limits.path_request_timeout),
            link_establish_timeout=_get_int(s, "link_establish_timeout", cfg.limits.link_establish_timeout),
            request_default_timeout=_get_int(s, "request_default_timeout", cfg.limits.request_default_timeout),
            max_ws_message_bytes=_get_int(s, "max_ws_message_bytes", cfg.limits.max_ws_message_bytes),
            max_packet_bytes=_get_int(s, "max_packet_bytes", cfg.limits.max_packet_bytes),
        )

    validate(cfg)
    return cfg


def validate(cfg: Config) -> None:
    if not (0 < cfg.network.bind_port < 65536):
        raise ValueError(f"[network] bind_port out of range: {cfg.network.bind_port}")
    if cfg.network.allow_http and not (0 < cfg.network.http_port < 65536):
        raise ValueError(f"[network] http_port out of range: {cfg.network.http_port}")
    if cfg.network.allow_http and cfg.network.http_port == cfg.network.bind_port:
        raise ValueError("[network] http_port must differ from bind_port when allow_http=true")
    if cfg.network.tls and cfg.tls.mode == "user_provided":
        if not cfg.tls.cert_file or not cfg.tls.key_file:
            raise ValueError("[tls] user_provided mode requires cert_file and key_file")
    if cfg.network.tls and cfg.tls.mode == "disabled":
        raise ValueError("[network] tls=true but [tls] mode=disabled — inconsistent")
    if cfg.auth.enabled and not cfg.auth.password_hash:
        raise ValueError("[auth] enabled=true but password_hash is empty")


def write_default(path: Path) -> None:
    """Write the default config to *path*. Refuses to overwrite an existing file."""
    if path.exists():
        raise FileExistsError(str(path))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(DEFAULT_CONFIG_TEXT, encoding="utf-8")
