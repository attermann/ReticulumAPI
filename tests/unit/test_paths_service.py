"""Unit tests for rnsapi.rns.paths.PathsService."""
import asyncio
import time

import pytest

from rnsapi.config import Config
from rnsapi.rns.paths import PathsError, PathsService
from rnsapi.ws.hub import WSHub


class _FakeReticulum:
    def __init__(self, table):
        self._table = table

    def get_path_table(self, max_hops=None):
        if max_hops is None:
            return self._table
        return [e for e in self._table if e["hops"] <= max_hops]


class _FakeConn:
    def __init__(self):
        from rnsapi.auth.session import Session
        self.session = Session(id="sess1", token="t", created_at=time.time(), last_seen_at=time.time())
        self.sent: list[dict] = []

    async def send_json(self, data):
        self.sent.append(data)


def _sample_table():
    return [
        {"hash": b"\x11" * 16, "via": b"\x22" * 16, "hops": 1, "interface": "AutoIf[0]", "timestamp": 1.0, "expires": 2.0},
        {"hash": b"\x33" * 16, "via": b"\x44" * 16, "hops": 5, "interface": "TCPIf[1]", "timestamp": 3.0, "expires": 4.0},
    ]


def test_list_all_paths_serialises_bytes_to_hex():
    svc = PathsService(Config(), WSHub())
    svc.attach(_FakeReticulum(_sample_table()))
    entries = svc.list_paths()
    assert len(entries) == 2
    assert entries[0]["hash"] == "11" * 16
    assert entries[0]["via"] == "22" * 16
    assert entries[0]["interface"] == "AutoIf[0]"


def test_filter_by_destination():
    svc = PathsService(Config(), WSHub())
    svc.attach(_FakeReticulum(_sample_table()))
    entries = svc.list_paths(destination="33" * 16)
    assert len(entries) == 1
    assert entries[0]["hash"] == "33" * 16


def test_filter_by_interface():
    svc = PathsService(Config(), WSHub())
    svc.attach(_FakeReticulum(_sample_table()))
    entries = svc.list_paths(interface="TCPIf[1]")
    assert len(entries) == 1
    assert entries[0]["hash"] == "33" * 16


def test_filter_by_max_hops():
    svc = PathsService(Config(), WSHub())
    svc.attach(_FakeReticulum(_sample_table()))
    entries = svc.list_paths(max_hops=2)
    assert len(entries) == 1
    assert entries[0]["hash"] == "11" * 16


def test_invalid_destination_hash_rejected():
    svc = PathsService(Config(), WSHub())
    svc.attach(_FakeReticulum(_sample_table()))
    with pytest.raises(PathsError, match="invalid destination"):
        svc.list_paths(destination="not-hex")


def test_list_paths_returns_empty_when_no_reticulum():
    svc = PathsService(Config(), WSHub())
    assert svc.list_paths() == []


@pytest.mark.asyncio
async def test_request_path_returns_not_found_on_timeout(rns_instance):
    cfg = Config()
    cfg.limits.path_request_timeout = 0  # instant timeout
    hub = WSHub()
    conn = _FakeConn()
    hub.register(conn)
    svc = PathsService(cfg, hub)
    svc.attach(rns_instance)
    # Random destination that we don't have a path to
    result = await svc.request_path(conn.session, "cd" * 16, timeout=0.05)
    assert result["found"] is False
    assert result["destination_hash"] == "cd" * 16
    # WS should have received the path.request.sent event
    assert any(e["type"] == "path.request.sent" for e in conn.sent)
    sent = next(e for e in conn.sent if e["type"] == "path.request.sent")
    assert sent["destination_hash"] == "cd" * 16


@pytest.mark.asyncio
async def test_request_path_returns_found_if_path_already_known(rns_instance):
    """If the path is already in the table, request_path returns quickly."""
    import RNS

    cfg = Config()
    hub = WSHub()
    conn = _FakeConn()
    svc = PathsService(cfg, hub)
    svc.attach(rns_instance)

    # Inject a synthetic path into RNS.Transport.path_table so has_path() succeeds
    dest_hash = bytes.fromhex("ab" * 16)
    with RNS.Transport.path_table_lock if hasattr(RNS.Transport, "path_table_lock") else _null_ctx():
        # tuple layout: [timestamp, via, hops, expires, packet_hash, interface]
        RNS.Transport.path_table[dest_hash] = [
            time.time(),
            b"\xff" * 16,
            2,
            time.time() + 3600,
            None,
            None,
        ]
    try:
        result = await asyncio.wait_for(
            svc.request_path(conn.session, "ab" * 16, timeout=1.0),
            timeout=3,
        )
        assert result["found"] is True
        assert result["destination_hash"] == "ab" * 16
        assert result["hops"] == 2
    finally:
        RNS.Transport.path_table.pop(dest_hash, None)


class _null_ctx:
    def __enter__(self): return self
    def __exit__(self, *a): return False
