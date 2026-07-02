import re

import pytest

from rnsapi.rns.identities import IdentityError, IdentityService


def test_create_persists_rid_file(rnsapi_home, rns_instance):
    svc = IdentityService(rnsapi_home)
    identity, info = svc.create()
    assert re.fullmatch(r"[0-9a-f]{32}", info.hash_hex)
    assert info.hash_hex == identity.hexhash
    assert (rnsapi_home.identities_dir / f"{info.hash_hex}.rid").exists()


def test_list_returns_all_persisted(rnsapi_home, rns_instance):
    svc = IdentityService(rnsapi_home)
    a, _ = svc.create()
    b, _ = svc.create()
    hashes = {i.hash_hex for i in svc.list()}
    assert a.hexhash in hashes
    assert b.hexhash in hashes


def test_load_by_hash(rnsapi_home, rns_instance):
    svc = IdentityService(rnsapi_home)
    a, _ = svc.create()
    loaded = svc.load(a.hexhash)
    assert loaded.hexhash == a.hexhash


def test_load_unknown_raises(rnsapi_home, rns_instance):
    svc = IdentityService(rnsapi_home)
    with pytest.raises(IdentityError, match="not found"):
        svc.load("aa" * 16)


def test_load_invalid_hash_raises(rnsapi_home, rns_instance):
    svc = IdentityService(rnsapi_home)
    with pytest.raises(IdentityError, match="invalid"):
        svc.load("nothex")
