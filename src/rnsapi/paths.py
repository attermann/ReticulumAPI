"""Resolve rnsapid's storage layout under a single root."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


DEFAULT_ROOT = Path("~/.config/rnsapi").expanduser()


@dataclass(frozen=True)
class StoragePaths:
    root: Path
    config_file: Path
    identities_dir: Path
    default_identity_file: Path
    certs_dir: Path
    logs_dir: Path
    resources_dir: Path

    def ensure(self) -> None:
        for p in (self.root, self.identities_dir, self.certs_dir, self.logs_dir, self.resources_dir):
            p.mkdir(parents=True, exist_ok=True)


def resolve(root: str | os.PathLike | None = None) -> StoragePaths:
    if root is None:
        root = os.environ.get("RNSAPI_HOME", DEFAULT_ROOT)
    root = Path(root).expanduser().resolve()
    return StoragePaths(
        root=root,
        config_file=root / "config",
        identities_dir=root / "identities",
        # Flat file at the root — there's only one, and most apps supply
        # their own identity via PUT /session/active-identity anyway.
        default_identity_file=root / "default_identity",
        certs_dir=root / "certs",
        logs_dir=root / "logs",
        resources_dir=root / "resources",
    )
