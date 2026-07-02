from pathlib import Path

import pytest

from rnsapi import config as config_mod


def _write(tmp_path: Path, text: str) -> Path:
    p = tmp_path / "config"
    p.write_text(text, encoding="utf-8")
    return p


def test_load_defaults_when_file_empty(tmp_path):
    cfg = config_mod.load(_write(tmp_path, ""))
    assert cfg.network.bind_host == "127.0.0.1"
    assert cfg.network.bind_port == 8000
    assert cfg.network.tls is True
    assert cfg.auth.enabled is False
    assert cfg.tls.mode == "self_signed"


def test_load_overrides(tmp_path):
    cfg = config_mod.load(
        _write(
            tmp_path,
            """
[network]
bind_host = 0.0.0.0
bind_port = 9000
tls = false
[auth]
enabled = false
[tls]
mode = disabled
""",
        )
    )
    assert cfg.network.bind_host == "0.0.0.0"
    assert cfg.network.bind_port == 9000
    assert cfg.network.tls is False
    assert cfg.tls.mode == "disabled"


def test_invalid_port_rejected(tmp_path):
    with pytest.raises(ValueError, match="bind_port"):
        config_mod.load(_write(tmp_path, "[network]\nbind_port = 70000\n"))


def test_tls_user_provided_requires_cert_key(tmp_path):
    with pytest.raises(ValueError, match="cert_file"):
        config_mod.load(_write(tmp_path, "[tls]\nmode = user_provided\n"))


def test_tls_disabled_but_network_tls_true_inconsistent(tmp_path):
    with pytest.raises(ValueError, match="inconsistent"):
        config_mod.load(
            _write(tmp_path, "[network]\ntls = true\n[tls]\nmode = disabled\n")
        )


def test_auth_enabled_requires_password_hash(tmp_path):
    with pytest.raises(ValueError, match="password_hash"):
        config_mod.load(_write(tmp_path, "[auth]\nenabled = true\n"))


def test_allow_http_requires_different_port(tmp_path):
    with pytest.raises(ValueError, match="http_port"):
        config_mod.load(
            _write(
                tmp_path,
                "[network]\nbind_port = 8000\nallow_http = true\nhttp_port = 8000\n",
            )
        )


def test_write_default_refuses_overwrite(tmp_path):
    dst = tmp_path / "config"
    config_mod.write_default(dst)
    assert dst.exists()
    with pytest.raises(FileExistsError):
        config_mod.write_default(dst)


def test_default_config_text_is_valid(tmp_path):
    dst = tmp_path / "config"
    config_mod.write_default(dst)
    cfg = config_mod.load(dst)
    assert cfg.network.bind_port == 8000
    assert cfg.tls.mode == "self_signed"


def test_boolean_variants(tmp_path):
    cfg = config_mod.load(
        _write(tmp_path, "[network]\ntls = false\nallow_http = yes\nhttp_port = 8001\n")
    )
    assert cfg.network.tls is False
    assert cfg.network.allow_http is True


def test_invalid_boolean_rejected(tmp_path):
    with pytest.raises(ValueError, match="boolean"):
        config_mod.load(_write(tmp_path, "[network]\ntls = maybe\n"))
