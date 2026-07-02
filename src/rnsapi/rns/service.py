"""Own the RNS.Reticulum instance and gate its lifecycle.

`RNS.Reticulum` is a singleton: exactly one instance may exist per process.
Startup takes a few seconds because RNS spawns worker threads for its
transport, path table, and interfaces. Shutdown must call
`RNS.Reticulum.exit_handler()` before the process exits or state files may
be left inconsistent.

By default we boot RNS against the user's shared configuration at
`~/.config/reticulum/`, so `rnsapid` participates in the same mesh as any
other RNS app on the host. Override `[rns] config_dir` in the daemon config
to point at a private Reticulum config instead.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import RNS

from ..config import Config


log = logging.getLogger(__name__)


class RNSService:
    def __init__(self, config: Config):
        self._config = config
        self.reticulum: Optional[RNS.Reticulum] = None
        self._started = False

    def start(self) -> None:
        if self._started:
            return
        configdir_arg = None
        if self._config.rns.config_dir:
            configdir_arg = str(Path(self._config.rns.config_dir).expanduser())
        log.info(
            "starting RNS.Reticulum (configdir=%s, log_level=%s)",
            configdir_arg or "<default>",
            self._config.rns.log_level,
        )
        self.reticulum = RNS.Reticulum(
            configdir=configdir_arg,
            verbosity=self._config.rns.log_level,
            logdest=RNS.LOG_STDOUT,
        )
        self._started = True
        log.info("RNS.Reticulum started")

    def stop(self) -> None:
        if not self._started:
            return
        try:
            RNS.Reticulum.exit_handler()
        except Exception:
            log.exception("RNS exit_handler raised")
        self._started = False
        log.info("RNS.Reticulum stopped")
