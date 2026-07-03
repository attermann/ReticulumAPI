"""Unit tests for ResourcesService.

Full RNS Resource transfer requires a working two-endpoint Link handshake,
which needs interfaces we don't have in the test environment. These tests
focus on:

- config parsing for [resources]
- session state additions
- input validation and error mapping
- direct-invocation of the receive-side callbacks (started + concluded)
  which exercises the fanout, temp-file copy, and inline-b64 logic
- cancel / delete
- cleanup semantics
- temp-file sweep
"""
from __future__ import annotations

import asyncio
import base64
import time
from pathlib import Path

import pytest

from rnsapi.async_bridge import AsyncBridge
from rnsapi.auth.session import Session
from rnsapi.config import Config, ResourcesConfig
from rnsapi.rns.resources import ResourceError, ResourcesService, TransferState, _status_str
from rnsapi.ws.hub import WSHub


class _FakeConn:
    def __init__(self, session):
        self.session = session
        self.sent: list[dict] = []

    async def send_json(self, data):
        self.sent.append(data)


def _new_session():
    now = time.time()
    return Session(id="sess-res", token="t", created_at=now, last_seen_at=now)


def _make_svc(rnsapi_home, hub: WSHub | None = None) -> ResourcesService:
    cfg = Config()
    cfg.resources = ResourcesConfig(
        temp_dir="resources",
        retention_seconds=3600,
        sweep_interval_seconds=300,
        max_inline_bytes=64,
        progress_throttle_ms=50,
        progress_throttle_pct=1.0,
        default_auto_accept=True,
    )
    return ResourcesService(hub or WSHub(), cfg, rnsapi_home)


# ---------- config ----------


def test_config_parses_resources_section(tmp_path):
    from rnsapi import config as config_mod
    p = tmp_path / "config"
    p.write_text(
        "[resources]\n"
        "retention_seconds = 60\n"
        "max_inline_bytes = 128\n"
        "default_auto_accept = false\n",
        encoding="utf-8",
    )
    cfg = config_mod.load(p)
    assert cfg.resources.retention_seconds == 60
    assert cfg.resources.max_inline_bytes == 128
    assert cfg.resources.default_auto_accept is False
    # Defaults preserved
    assert cfg.resources.sweep_interval_seconds == 300


def test_session_has_transfer_fields():
    s = _new_session()
    assert s.active_transfers == {}
    assert s.link_resource_policy == {}


# ---------- errors ----------


@pytest.mark.asyncio
async def test_send_rejects_unknown_link(rnsapi_home, rns_instance):
    session = _new_session()
    svc = _make_svc(rnsapi_home)
    with pytest.raises(ResourceError, match="unknown link"):
        await svc.send(session, "aa" * 16, b"hi")


@pytest.mark.asyncio
async def test_send_rejects_invalid_link_id(rnsapi_home, rns_instance):
    session = _new_session()
    svc = _make_svc(rnsapi_home)
    with pytest.raises(ResourceError, match="invalid link id"):
        await svc.send(session, "nothex", b"hi")


def test_get_state_unknown(rnsapi_home, rns_instance):
    session = _new_session()
    svc = _make_svc(rnsapi_home)
    with pytest.raises(ResourceError, match="unknown transfer"):
        svc.get_state(session, "notreal")


def test_open_stream_requires_complete_inbound(rnsapi_home, rns_instance):
    session = _new_session()
    svc = _make_svc(rnsapi_home)
    session.active_transfers["t1"] = TransferState(
        transfer_id="t1", session_id=session.id, direction="out", link_id_hex="aa" * 16
    )
    with pytest.raises(ResourceError, match="outbound"):
        svc.open_stream(session, "t1")


# ---------- receive callback flow ----------


class _FakeReceivedResource:
    """Minimal stand-in for RNS.Resource on the receive side."""

    def __init__(self, data: bytes, status_value):
        import RNS
        self._data = data
        self.status = status_value
        # Emulate resource.data as a file-like object with a `.name`
        # attribute pointing at a real temp file on disk.
        import tempfile
        self._tmp = tempfile.NamedTemporaryFile(delete=False)
        self._tmp.write(data)
        self._tmp.flush()
        self._tmp.seek(0)
        self.data = self._tmp
        self.metadata = {"filename": "test.bin"}

    def get_data_size(self):
        return len(self._data)

    def get_progress(self):
        return 1.0 if self.status else 0.0

    def cancel(self):
        pass


@pytest.mark.asyncio
async def test_receive_concluded_emits_inline_for_small(rnsapi_home, rns_instance):
    import RNS
    session = _new_session()
    hub = WSHub()
    conn = _FakeConn(session)
    hub.register(conn)
    cfg = Config()
    cfg.resources = ResourcesConfig(max_inline_bytes=1024, progress_throttle_ms=50)
    svc = ResourcesService(hub, cfg, rnsapi_home)
    await svc.start()
    try:
        AsyncBridge.set_main_loop(asyncio.get_running_loop())
        resource = _FakeReceivedResource(b"payload-small", RNS.Resource.COMPLETE)
        link_hash = bytes.fromhex("aa" * 16)
        await svc._on_receive_started(session, link_hash, "app.aspect", resource)
        await svc._on_receive_concluded(session, link_hash, "app.aspect", resource)

        completed = [e for e in conn.sent if e.get("type") == "resource.completed"]
        assert completed
        ev = completed[0]
        assert ev["status"] == "COMPLETE"
        assert ev["data_b64"] == base64.b64encode(b"payload-small").decode()
        assert ev["download_url"].startswith("/resources/")
        assert ev["metadata"] == {"filename": "test.bin"}
    finally:
        AsyncBridge.clear_main_loop()
        await svc.stop()


@pytest.mark.asyncio
async def test_receive_concluded_omits_inline_for_large(rnsapi_home, rns_instance):
    import RNS
    session = _new_session()
    hub = WSHub()
    conn = _FakeConn(session)
    hub.register(conn)
    cfg = Config()
    cfg.resources = ResourcesConfig(max_inline_bytes=32, progress_throttle_ms=50)
    svc = ResourcesService(hub, cfg, rnsapi_home)
    await svc.start()
    try:
        AsyncBridge.set_main_loop(asyncio.get_running_loop())
        payload = b"x" * 128  # > max_inline_bytes
        resource = _FakeReceivedResource(payload, RNS.Resource.COMPLETE)
        link_hash = bytes.fromhex("bb" * 16)
        await svc._on_receive_started(session, link_hash, "app.big", resource)
        await svc._on_receive_concluded(session, link_hash, "app.big", resource)

        completed = [e for e in conn.sent if e.get("type") == "resource.completed"]
        assert completed
        ev = completed[0]
        assert "data_b64" not in ev
        assert ev["download_url"].startswith("/resources/")
        # Follow the download URL through open_stream
        transfer_id = ev["transfer_id"]
        path = svc.open_stream(session, transfer_id)
        assert path.read_bytes() == payload
    finally:
        AsyncBridge.clear_main_loop()
        await svc.stop()


@pytest.mark.asyncio
async def test_receive_failed_emits_failed(rnsapi_home, rns_instance):
    import RNS
    session = _new_session()
    hub = WSHub()
    conn = _FakeConn(session)
    hub.register(conn)
    svc = _make_svc(rnsapi_home, hub=hub)
    await svc.start()
    try:
        AsyncBridge.set_main_loop(asyncio.get_running_loop())
        resource = _FakeReceivedResource(b"x", RNS.Resource.FAILED)
        link_hash = bytes.fromhex("cc" * 16)
        await svc._on_receive_started(session, link_hash, "app.fail", resource)
        await svc._on_receive_concluded(session, link_hash, "app.fail", resource)

        failed = [e for e in conn.sent if e.get("type") == "resource.failed"]
        assert failed
        assert failed[0]["status"] == "FAILED"
    finally:
        AsyncBridge.clear_main_loop()
        await svc.stop()


# ---------- link policy ----------


@pytest.mark.asyncio
async def test_set_link_policy_requires_owned_link(rnsapi_home, rns_instance):
    session = _new_session()
    svc = _make_svc(rnsapi_home)
    with pytest.raises(ResourceError, match="unknown link"):
        svc.set_link_policy(session, "aa" * 16, False)


# ---------- sweep ----------


def test_sweep_deletes_old_files(rnsapi_home):
    svc = _make_svc(rnsapi_home)
    old_file = rnsapi_home.resources_dir / "expired"
    fresh_file = rnsapi_home.resources_dir / "fresh"
    rnsapi_home.resources_dir.mkdir(parents=True, exist_ok=True)
    old_file.write_bytes(b"old")
    fresh_file.write_bytes(b"fresh")
    old_time = time.time() - 10_000
    import os
    os.utime(old_file, (old_time, old_time))

    removed = svc._sweep_once(retention_seconds=3600)
    assert removed >= 1
    assert not old_file.exists()
    assert fresh_file.exists()


# ---------- cleanup ----------


@pytest.mark.asyncio
async def test_cleanup_removes_transfers_and_files(rnsapi_home, rns_instance):
    session = _new_session()
    svc = _make_svc(rnsapi_home)
    await svc.start()
    try:
        # Simulate a completed inbound with a temp file
        rnsapi_home.resources_dir.mkdir(parents=True, exist_ok=True)
        tmp = rnsapi_home.resources_dir / "t1"
        tmp.write_bytes(b"leftover")
        session.active_transfers["t1"] = TransferState(
            transfer_id="t1",
            session_id=session.id,
            direction="in",
            link_id_hex="dd" * 16,
            status="COMPLETE",
            temp_path=tmp,
        )
        await svc.cleanup_session(session)
        assert session.active_transfers == {}
        assert not tmp.exists()
    finally:
        await svc.stop()
