from pathlib import Path

from rnsapi import paths


def test_resolve_default_root(monkeypatch, tmp_path):
    monkeypatch.delenv("RNSAPI_HOME", raising=False)
    p = paths.resolve()
    assert p.root == Path("~/.config/rnsapi").expanduser().resolve()


def test_resolve_env_override(monkeypatch, tmp_path):
    monkeypatch.setenv("RNSAPI_HOME", str(tmp_path / "custom"))
    p = paths.resolve()
    assert p.root == (tmp_path / "custom").resolve()


def test_resolve_arg_override(tmp_path):
    p = paths.resolve(tmp_path / "explicit")
    assert p.root == (tmp_path / "explicit").resolve()
    assert p.identities_dir == p.root / "identities"
    assert p.certs_dir == p.root / "certs"
    assert p.logs_dir == p.root / "logs"
    assert p.config_file == p.root / "config"


def test_ensure_creates_all_dirs(tmp_path):
    p = paths.resolve(tmp_path / "home")
    p.ensure()
    assert p.root.is_dir()
    assert p.identities_dir.is_dir()
    assert p.certs_dir.is_dir()
    assert p.logs_dir.is_dir()
