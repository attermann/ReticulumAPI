from dataclasses import dataclass, field

import pytest

from rnsapi.rns.destinations import DestinationError, DestinationService
from rnsapi.rns.identities import IdentityService


@dataclass
class _FakeSession:
    id: str = "sess1"
    owned_destinations: dict = field(default_factory=dict)
    open_links: dict = field(default_factory=dict)


def test_create_registers_destination(rnsapi_home, rns_instance):
    identity_svc = IdentityService(rnsapi_home)
    identity, _ = identity_svc.create()
    svc = DestinationService()
    session = _FakeSession()
    destination, info = svc.create(session, identity, "in", "single", "rnsapi_test", ["unit", "one"])
    assert destination.hash in session.owned_destinations
    assert info.app_name == "rnsapi_test"
    assert info.aspects == ("unit", "one")
    assert info.direction == "in"
    assert info.type == "single"
    # cleanup
    svc.remove(session, info.hash_hex)


def test_remove_deregisters(rnsapi_home, rns_instance):
    identity_svc = IdentityService(rnsapi_home)
    identity, _ = identity_svc.create()
    svc = DestinationService()
    session = _FakeSession()
    _, info = svc.create(session, identity, "in", "single", "rnsapi_test", ["unit", "two"])
    svc.remove(session, info.hash_hex)
    assert info.hash_hex not in {h.hex() for h in session.owned_destinations.keys()}


def test_remove_unknown_raises(rnsapi_home, rns_instance):
    svc = DestinationService()
    session = _FakeSession()
    with pytest.raises(DestinationError, match="not owned"):
        svc.remove(session, "aa" * 16)


def test_invalid_direction_rejected(rnsapi_home, rns_instance):
    identity_svc = IdentityService(rnsapi_home)
    identity, _ = identity_svc.create()
    svc = DestinationService()
    session = _FakeSession()
    with pytest.raises(DestinationError, match="direction"):
        svc.create(session, identity, "sideways", "single", "rnsapi_test", ["x"])


def test_invalid_type_rejected(rnsapi_home, rns_instance):
    identity_svc = IdentityService(rnsapi_home)
    identity, _ = identity_svc.create()
    svc = DestinationService()
    session = _FakeSession()
    with pytest.raises(DestinationError, match="type"):
        svc.create(session, identity, "in", "flat", "rnsapi_test", ["x"])


def test_invalid_aspect_rejected(rnsapi_home, rns_instance):
    identity_svc = IdentityService(rnsapi_home)
    identity, _ = identity_svc.create()
    svc = DestinationService()
    session = _FakeSession()
    with pytest.raises(DestinationError, match="aspect"):
        svc.create(session, identity, "in", "single", "rnsapi_test", ["has spaces"])


@pytest.mark.asyncio
async def test_cleanup_session_deregisters_all(rnsapi_home, rns_instance):
    identity_svc = IdentityService(rnsapi_home)
    identity, _ = identity_svc.create()
    svc = DestinationService()
    session = _FakeSession()
    svc.create(session, identity, "in", "single", "rnsapi_test", ["cleanup", "one"])
    svc.create(session, identity, "in", "single", "rnsapi_test", ["cleanup", "two"])
    assert len(session.owned_destinations) == 2
    await svc.cleanup_session(session)
    assert session.owned_destinations == {}
