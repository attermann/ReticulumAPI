"""Shared pytest fixtures for rnsapid tests."""
from __future__ import annotations

import pytest

from rnsapi import paths
from rnsapi.config import Config, NetworkConfig, TlsConfig


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
