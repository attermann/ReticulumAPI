"""Entry point for the `rnsapid` command."""
from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys
from pathlib import Path

from . import __version__
from . import config as config_mod
from . import logging_setup, paths, server
from .tls import cert_fingerprint_sha256, ensure_self_signed


log = logging.getLogger("rnsapid")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="rnsapid", description="Reticulum REST + WebSocket API daemon")
    p.add_argument("--config", type=Path, default=None, help="Path to config file")
    p.add_argument("--home", type=Path, default=None, help="Storage root (default: ~/.config/rnsapi)")
    p.add_argument("--init", action="store_true", help="Write a default config and exit")
    p.add_argument(
        "--print-cert-fingerprint",
        action="store_true",
        help="Print the self-signed TLS cert's SHA-256 fingerprint and exit",
    )
    p.add_argument("--version", action="version", version=f"rnsapid {__version__}")
    return p.parse_args(argv)


def _init_home(storage: paths.StoragePaths, config_path: Path) -> int:
    storage.ensure()
    if config_path.exists():
        print(f"config already exists at {config_path}", file=sys.stderr)
        return 1
    config_mod.write_default(config_path)
    print(f"wrote default config to {config_path}")
    print(f"storage root:      {storage.root}")
    print(f"identities dir:    {storage.identities_dir}")
    print(f"certs dir:         {storage.certs_dir}")
    print(f"logs dir:          {storage.logs_dir}")
    return 0


def _print_fingerprint(storage: paths.StoragePaths, cfg: config_mod.Config) -> int:
    cert_paths = ensure_self_signed(storage.certs_dir, cfg.tls.self_signed_cn)
    print(cert_fingerprint_sha256(cert_paths.cert))
    return 0


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)

    storage = paths.resolve(args.home)
    config_path = args.config or storage.config_file

    if args.init:
        return _init_home(storage, config_path)

    if not config_path.exists():
        print(
            f"config file not found: {config_path}\nRun `rnsapid --init` to create one.",
            file=sys.stderr,
        )
        return 2

    cfg = config_mod.load(config_path)
    storage.ensure()

    log_file = None
    if cfg.logging.file:
        log_file = (storage.root / cfg.logging.file) if not Path(cfg.logging.file).is_absolute() else Path(cfg.logging.file)
    logging_setup.configure(cfg.logging.level, log_file, cfg.logging.rotate_max_bytes, cfg.logging.rotate_backup_count)

    if args.print_cert_fingerprint:
        return _print_fingerprint(storage, cfg)

    log.info("rnsapid %s starting", __version__)

    loop = asyncio.new_event_loop()
    stop = asyncio.Event()

    def _shutdown(*_a):
        log.info("shutdown signal received")
        loop.call_soon_threadsafe(stop.set)

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _shutdown)
        except NotImplementedError:
            signal.signal(sig, _shutdown)

    async def _run():
        run_task = asyncio.create_task(server.run(cfg, storage))
        stop_task = asyncio.create_task(stop.wait())
        done, pending = await asyncio.wait(
            {run_task, stop_task}, return_when=asyncio.FIRST_COMPLETED
        )
        for t in pending:
            t.cancel()
        for t in pending:
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass
        for t in done:
            exc = t.exception()
            if exc is not None:
                raise exc

    try:
        loop.run_until_complete(_run())
    except KeyboardInterrupt:
        pass
    finally:
        loop.close()
    log.info("rnsapid exited cleanly")
    return 0


if __name__ == "__main__":
    sys.exit(main())
