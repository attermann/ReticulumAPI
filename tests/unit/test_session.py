import asyncio
import time

import pytest

from rnsapi.auth.session import SessionRegistry
from rnsapi.config import Config
from rnsapi.ws.hub import WSHub


def _registry(inactivity=1800, lifetime=86400):
    cfg = Config()
    cfg.auth.session_inactivity_timeout = inactivity
    cfg.auth.session_max_lifetime = lifetime
    hub = WSHub()
    return SessionRegistry(cfg, hub), hub


def test_create_returns_unique_ids_and_tokens():
    reg, _ = _registry()
    a = reg.create()
    b = reg.create()
    assert a.id != b.id
    assert a.token != b.token
    assert not a.is_anonymous
    assert reg.get_by_token(a.token) is a
    assert reg.get_by_id(a.id) is a


def test_anonymous_is_singleton():
    reg, _ = _registry()
    a = reg.anonymous()
    b = reg.anonymous()
    assert a is b
    assert a.is_anonymous


def test_get_by_token_touches_last_seen():
    reg, _ = _registry()
    s = reg.create()
    original = s.last_seen_at
    time.sleep(0.01)
    reg.get_by_token(s.token)
    assert s.last_seen_at > original


@pytest.mark.asyncio
async def test_revoke_removes_from_lookup():
    reg, _ = _registry()
    s = reg.create()
    token = s.token
    await reg.revoke(token, reason="logout")
    assert reg.get_by_token(token) is None
    assert reg.get_by_id(s.id) is None


@pytest.mark.asyncio
async def test_sweep_expires_stale_sessions():
    reg, _ = _registry(inactivity=0)  # anything past 0s inactive is expired
    s = reg.create()
    # Move last_seen back into the past
    s.last_seen_at = time.time() - 5
    expired = await reg.sweep_once()
    assert s in expired
    assert reg.get_by_id(s.id) is None


@pytest.mark.asyncio
async def test_sweep_leaves_anonymous_alone():
    reg, _ = _registry(inactivity=0)
    anon = reg.anonymous()
    expired = await reg.sweep_once()
    assert expired == []
    assert reg.anonymous() is anon


@pytest.mark.asyncio
async def test_cleanup_hooks_are_invoked_on_expiry():
    reg, _ = _registry(inactivity=0)
    called = []

    async def hook(session):
        called.append(session.id)

    reg.register_cleanup(hook)
    s = reg.create()
    s.last_seen_at = time.time() - 5
    await reg.sweep_once()
    assert called == [s.id]


@pytest.mark.asyncio
async def test_reaper_runs_and_stops():
    reg, _ = _registry(inactivity=0)
    reg.start_reaper(interval=0.05)
    s = reg.create()
    s.last_seen_at = time.time() - 5
    for _ in range(20):
        await asyncio.sleep(0.05)
        if reg.get_by_id(s.id) is None:
            break
    assert reg.get_by_id(s.id) is None
    await reg.stop_reaper()


@pytest.mark.asyncio
async def test_max_lifetime_expires_even_when_active():
    reg, _ = _registry(inactivity=86400, lifetime=0)
    s = reg.create()
    s.created_at = time.time() - 5
    s.last_seen_at = time.time()
    expired = await reg.sweep_once()
    assert s in expired
