"""Logging setup for rnsapid: rotating file + stdout."""
from __future__ import annotations

import logging
import logging.handlers
from pathlib import Path


def configure(level: str, log_file: Path | None, max_bytes: int, backup_count: int) -> None:
    root = logging.getLogger()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))

    for h in list(root.handlers):
        root.removeHandler(h)

    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    stream = logging.StreamHandler()
    stream.setFormatter(fmt)
    root.addHandler(stream)

    if log_file is not None:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        rot = logging.handlers.RotatingFileHandler(
            log_file, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8"
        )
        rot.setFormatter(fmt)
        root.addHandler(rot)
