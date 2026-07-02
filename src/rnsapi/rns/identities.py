"""Identity management: create, list, load, and mark session-active.

Identities are stored as `.rid` files under `~/.config/rnsapi/identities/`.
`RNS.Identity.to_file(path)` and `RNS.Identity.from_file(path)` are the
canonical serialization; we lay them out one file per identity keyed on the
identity's hex hash.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

import RNS

from ..paths import StoragePaths


log = logging.getLogger(__name__)

_HEX_HASH = re.compile(r"^[0-9a-f]{32}$")


class IdentityError(Exception):
    """Raised for identity-related errors that map to 4xx responses."""


@dataclass
class IdentityInfo:
    hash_hex: str
    path: str
    public_key_hex: str

    def to_dict(self) -> dict:
        return {"hash": self.hash_hex, "public_key": self.public_key_hex, "path": self.path}


class IdentityService:
    def __init__(self, storage: StoragePaths):
        self._storage = storage
        self._storage.identities_dir.mkdir(parents=True, exist_ok=True)

    def _path_for(self, identity: RNS.Identity) -> Path:
        return self._storage.identities_dir / f"{identity.hexhash}.rid"

    def _info(self, identity: RNS.Identity, path: Path) -> IdentityInfo:
        return IdentityInfo(
            hash_hex=identity.hexhash,
            path=str(path),
            public_key_hex=identity.get_public_key().hex(),
        )

    def create(self) -> tuple[RNS.Identity, IdentityInfo]:
        identity = RNS.Identity()
        path = self._path_for(identity)
        identity.to_file(str(path))
        info = self._info(identity, path)
        log.info("created identity %s at %s", info.hash_hex, path)
        return identity, info

    def list(self) -> list[IdentityInfo]:
        results: list[IdentityInfo] = []
        for path in sorted(self._storage.identities_dir.glob("*.rid")):
            try:
                identity = RNS.Identity.from_file(str(path))
            except Exception:
                log.warning("could not load identity from %s", path)
                continue
            if identity is None:
                continue
            results.append(self._info(identity, path))
        return results

    def load(self, hash_hex: str) -> RNS.Identity:
        h = hash_hex.lower()
        if not _HEX_HASH.match(h):
            raise IdentityError(f"invalid identity hash: {hash_hex!r}")
        path = self._storage.identities_dir / f"{h}.rid"
        if not path.exists():
            raise IdentityError(f"identity not found: {hash_hex}")
        identity = RNS.Identity.from_file(str(path))
        if identity is None:
            raise IdentityError(f"identity file corrupt: {path}")
        return identity

    def info_for(self, identity: RNS.Identity) -> IdentityInfo:
        return self._info(identity, self._path_for(identity))
