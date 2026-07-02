"""Shared pytest fixtures for rnsapid tests."""
from __future__ import annotations

import shutil
import socket
from pathlib import Path

import pytest

from rnsapi import paths
from rnsapi.config import Config, NetworkConfig, TlsConfig


def _free_port() -> int:
    s = socket.socket()
    try:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]
    finally:
        s.close()


@pytest.fixture
def rnsapi_home(tmp_path):
    home = tmp_path / "rnsapi"
    p = paths.resolve(home)
    p.ensure()
    return p


@pytest.fixture
def plain_config(rnsapi_home) -> Config:
    """Config with TLS off, auth off, ephemeral ports — safe for integration tests."""
    cfg = Config()
    cfg.network = NetworkConfig(bind_host="127.0.0.1", bind_port=0, tls=False, allow_http=False)
    cfg.tls = TlsConfig(mode="disabled")
    return cfg


@pytest.fixture
def tls_config(rnsapi_home) -> Config:
    cfg = Config()
    cfg.network = NetworkConfig(bind_host="127.0.0.1", bind_port=0, tls=True, allow_http=False)
    cfg.tls = TlsConfig(mode="self_signed", self_signed_cn="localhost")
    return cfg


# ---------- Session-scoped RNS fixture ----------
#
# RNS.Reticulum is a singleton — one instance per process. So we boot it once
# per test session against a private, no-interfaces config that lives in the
# tmp dir. Tests that need RNS pull the `rns_instance` fixture; each test
# creates its own identities/destinations so there's no cross-test coupling.


_RNS_TEST_CONFIG_TEMPLATE = """\
[reticulum]
  enable_transport = No
  share_instance = Yes
  instance_name = rnsapid_testrunner
  shared_instance_port = {shared_port}
  instance_control_port = {control_port}
  panic_on_interface_error = No

[logging]
  loglevel = 1

[interfaces]
  # No interfaces — all traffic stays in-process.
"""


@pytest.fixture(scope="session")
def rns_config_dir(tmp_path_factory) -> Path:
    root = tmp_path_factory.mktemp("rns")
    (root / "config").write_text(
        _RNS_TEST_CONFIG_TEMPLATE.format(shared_port=_free_port(), control_port=_free_port())
    )
    return root


@pytest.fixture(scope="session")
def rns_instance(rns_config_dir):
    import RNS

    inst = RNS.Reticulum(configdir=str(rns_config_dir), loglevel=1, logdest=RNS.LOG_STDOUT)
    yield inst
    try:
        RNS.Reticulum.exit_handler()
    except Exception:
        pass
