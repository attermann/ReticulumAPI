"""Session-scoped RNS.Resource send/receive bridge.

`RNS.Resource` is the reliable-transport primitive on top of a Link: it
handles segmentation, compression, request-window flow control, and
integrity checks. This service surfaces both directions:

- **Send**: `send(session, link, file_or_bytes, metadata, auto_compress)`
  wraps `RNS.Resource(data, link, ...)`, wires progress/completion
  callbacks that throttle-fanout `resource.progress` and
  `resource.completed` events (session-scoped).
- **Receive**: `attach_link(session, link)` sets `ACCEPT_ALL` on the link
  and wires the started/concluded callbacks. Since RNS doesn't accept a
  progress_callback on the ACCEPT_ALL path, live progress on inbound
  transfers is produced by a small background asyncio task that polls
  `resource.get_progress()` at the configured throttle rate until the
  resource reaches ASSEMBLING or a terminal state.

All RNS-thread callbacks bridge to asyncio via `AsyncBridge.run_async`.
Received bytes are always copied into our own `resources_dir` (under a
per-transfer filename) so our download URL is decoupled from RNS's
internal file lifecycle.
"""
from __future__ import annotations

import asyncio
import base64
import contextlib
import json
import logging
import os
import secrets
import shutil
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

import RNS

from ..async_bridge import AsyncBridge


if TYPE_CHECKING:
    from ..auth.session import Session
    from ..config import Config
    from ..paths import StoragePaths
    from ..ws.hub import WSHub


log = logging.getLogger(__name__)


_RESOURCE_STATUS_MAP = {
    RNS.Resource.NONE: "NONE",
    RNS.Resource.QUEUED: "QUEUED",
    RNS.Resource.ADVERTISED: "ADVERTISED",
    RNS.Resource.TRANSFERRING: "TRANSFERRING",
    RNS.Resource.AWAITING_PROOF: "AWAITING_PROOF",
    RNS.Resource.ASSEMBLING: "ASSEMBLING",
    RNS.Resource.COMPLETE: "COMPLETE",
    RNS.Resource.FAILED: "FAILED",
    RNS.Resource.CORRUPT: "CORRUPT",
    RNS.Resource.REJECTED: "REJECTED",
}


def _status_str(status) -> str:
    return _RESOURCE_STATUS_MAP.get(status, "UNKNOWN")


class ResourceError(Exception):
    """Resource-related errors that map to 4xx REST responses."""


@dataclass
class TransferState:
    transfer_id: str
    session_id: str
    direction: str  # "in" or "out"
    link_id_hex: str
    status: str = "PENDING"
    total_size: int = 0
    bytes_transferred: int = 0
    progress: float = 0.0
    temp_path: Optional[Path] = None
    upload_temp_path: Optional[Path] = None  # for outbound: the file we streamed in
    metadata: Optional[dict] = None
    created_at: float = field(default_factory=time.time)
    completed_at: Optional[float] = None
    failure_reason: Optional[str] = None
    # Strong ref to the RNS Resource so its callbacks survive the send path.
    resource: Any = None
    # asyncio.Event set on completion so the REST await path can wait.
    done_event: Optional[asyncio.Event] = None
    # Progress-throttle bookkeeping
    _last_progress_emit_ts: float = 0.0
    _last_progress_emit_pct: float = -1.0
    # Poller task for inbound (RNS doesn't fire progress callbacks on receive).
    poller_task: Any = None

    def to_dict(self, download_url: Optional[str] = None) -> dict:
        d = {
            "transfer_id": self.transfer_id,
            "session_id": self.session_id,
            "direction": self.direction,
            "link_id": self.link_id_hex,
            "status": self.status,
            "total_size": self.total_size,
            "bytes_transferred": self.bytes_transferred,
            "progress": self.progress,
            "created_at": self.created_at,
            "completed_at": self.completed_at,
            "metadata": self.metadata,
            "failure_reason": self.failure_reason,
            "temp_path_exists": bool(self.temp_path and self.temp_path.exists()),
        }
        if download_url is not None:
            d["download_url"] = download_url
        return d


class ResourcesService:
    def __init__(
        self,
        hub: "WSHub",
        config: "Config",
        storage: "StoragePaths",
    ):
        self._hub = hub
        self._config = config
        self._storage = storage
        self._sweep_task: Optional[asyncio.Task] = None

    # ---------- lifecycle ----------

    async def start(self) -> None:
        self._storage.resources_dir.mkdir(parents=True, exist_ok=True)
        if self._sweep_task is None:
            self._sweep_task = asyncio.create_task(self._sweep_loop())

    async def stop(self) -> None:
        if self._sweep_task is not None:
            self._sweep_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._sweep_task
            self._sweep_task = None

    async def _sweep_loop(self) -> None:
        interval = max(30, self._config.resources.sweep_interval_seconds)
        retention = self._config.resources.retention_seconds
        try:
            while True:
                await asyncio.sleep(interval)
                try:
                    self._sweep_once(retention)
                except Exception:
                    log.exception("resource sweep failed")
        except asyncio.CancelledError:
            pass

    def _sweep_once(self, retention_seconds: int) -> int:
        now = time.time()
        removed = 0
        try:
            for entry in self._storage.resources_dir.iterdir():
                if not entry.is_file():
                    continue
                try:
                    if now - entry.stat().st_mtime > retention_seconds:
                        entry.unlink(missing_ok=True)
                        removed += 1
                except FileNotFoundError:
                    pass
        except FileNotFoundError:
            pass
        return removed

    # ---------- Link attach (called by LinksService) ----------

    def attach_link(self, session: "Session", link, link_dest_hash: bytes, aspect: str) -> None:
        """Wire resource callbacks on a session-owned Link.

        Called by `LinksService._wire_callbacks` right after the other
        Link callbacks are installed.
        """
        policy = session.link_resource_policy.get(link_dest_hash, {})
        accept = policy.get("accept", self._config.resources.default_auto_accept)

        try:
            if accept:
                link.set_resource_strategy(RNS.Link.ACCEPT_ALL)
            else:
                link.set_resource_strategy(RNS.Link.ACCEPT_NONE)
        except Exception:
            log.debug("set_resource_strategy not supported by this Link build")

        session_id = session.id
        link_id_hex = link_dest_hash.hex()

        def _on_resource_started(resource):
            AsyncBridge.run_async(
                self._on_receive_started(session, link_dest_hash, aspect, resource)
            )

        def _on_resource_concluded(resource):
            AsyncBridge.run_async(
                self._on_receive_concluded(session, link_dest_hash, aspect, resource)
            )

        try:
            link.set_resource_started_callback(_on_resource_started)
        except Exception:
            log.debug("set_resource_started_callback not supported")
        try:
            link.set_resource_concluded_callback(_on_resource_concluded)
        except Exception:
            log.debug("set_resource_concluded_callback not supported")

    def set_link_policy(self, session: "Session", link_id_hex: str, accept: bool) -> dict:
        try:
            link_hash = bytes.fromhex(link_id_hex.lower())
        except ValueError:
            raise ResourceError(f"invalid link id: {link_id_hex!r}") from None
        entry = session.open_links.get(link_hash)
        if entry is None:
            raise ResourceError(f"unknown link: {link_id_hex}")
        session.link_resource_policy[link_hash] = {"accept": bool(accept)}
        try:
            entry.link.set_resource_strategy(
                RNS.Link.ACCEPT_ALL if accept else RNS.Link.ACCEPT_NONE
            )
        except Exception:
            log.debug("set_resource_strategy not supported by this Link build")
        return {"ok": True, "link_id": link_id_hex, "accept": bool(accept)}

    # ---------- receive-side ----------

    async def _on_receive_started(self, session, link_dest_hash, aspect, resource):
        transfer_id = secrets.token_hex(8)
        total_size = 0
        try:
            total_size = int(resource.get_data_size())
        except Exception:
            try:
                total_size = int(getattr(resource, "total_size", 0) or 0)
            except Exception:
                total_size = 0
        state = TransferState(
            transfer_id=transfer_id,
            session_id=session.id,
            direction="in",
            link_id_hex=link_dest_hash.hex(),
            status=_status_str(getattr(resource, "status", RNS.Resource.TRANSFERRING)),
            total_size=total_size,
            resource=resource,
            metadata=getattr(resource, "metadata", None),
        )
        # Stash so the concluded callback can find it via resource identity.
        setattr(resource, "_rnsapi_transfer_id", transfer_id)
        session.active_transfers[transfer_id] = state

        await self._hub.send_session(
            session.id,
            {"type": "resource.started", **state.to_dict()},
        )

        # Start a polling task to emit progress events on inbound transfers
        # (RNS doesn't fire progress_callbacks on ACCEPT_ALL receipts).
        state.poller_task = asyncio.create_task(self._poll_receive_progress(session, state))

    async def _poll_receive_progress(self, session, state: TransferState):
        throttle = max(0.05, self._config.resources.progress_throttle_ms / 1000.0)
        try:
            while True:
                await asyncio.sleep(throttle)
                if state.resource is None:
                    return
                status = getattr(state.resource, "status", None)
                if status in (
                    RNS.Resource.COMPLETE,
                    RNS.Resource.ASSEMBLING,
                    RNS.Resource.FAILED,
                    RNS.Resource.CORRUPT,
                    RNS.Resource.REJECTED,
                ):
                    return
                try:
                    progress = float(state.resource.get_progress())
                except Exception:
                    progress = state.progress
                state.progress = progress
                state.bytes_transferred = int(progress * state.total_size) if state.total_size else 0
                state.status = _status_str(status)
                await self._emit_progress_if_due(session, state)
        except asyncio.CancelledError:
            pass

    async def _emit_progress_if_due(self, session, state: TransferState) -> None:
        now = time.monotonic()
        min_ms = self._config.resources.progress_throttle_ms / 1000.0
        min_pct = self._config.resources.progress_throttle_pct
        if now - state._last_progress_emit_ts < min_ms:
            return
        pct = state.progress * 100.0
        if state._last_progress_emit_pct >= 0 and abs(pct - state._last_progress_emit_pct) < min_pct:
            return
        state._last_progress_emit_ts = now
        state._last_progress_emit_pct = pct
        await self._hub.send_session(
            session.id,
            {"type": "resource.progress", **state.to_dict()},
        )

    async def _on_receive_concluded(self, session, link_dest_hash, aspect, resource):
        transfer_id = getattr(resource, "_rnsapi_transfer_id", None)
        if transfer_id is None:
            transfer_id = secrets.token_hex(8)
            state = TransferState(
                transfer_id=transfer_id,
                session_id=session.id,
                direction="in",
                link_id_hex=link_dest_hash.hex(),
                resource=resource,
            )
            session.active_transfers[transfer_id] = state
        else:
            state = session.active_transfers.get(transfer_id)
            if state is None:
                # Started callback wasn't wired (shouldn't happen). Synthesize.
                state = TransferState(
                    transfer_id=transfer_id,
                    session_id=session.id,
                    direction="in",
                    link_id_hex=link_dest_hash.hex(),
                    resource=resource,
                )
                session.active_transfers[transfer_id] = state

        # Stop the poller
        if state.poller_task is not None:
            state.poller_task.cancel()

        status = getattr(resource, "status", None)
        state.status = _status_str(status)
        state.completed_at = time.time()
        state.metadata = getattr(resource, "metadata", None)

        if status == RNS.Resource.COMPLETE:
            # Copy the assembled bytes into our resources_dir under transfer_id.
            try:
                data_handle = resource.data
                src_path = None
                if hasattr(data_handle, "name"):
                    src_path = Path(data_handle.name)
                dest_path = self._storage.resources_dir / transfer_id
                if src_path is not None and src_path.exists():
                    shutil.copyfile(src_path, dest_path)
                else:
                    # Fall back to reading through the handle
                    with open(dest_path, "wb") as out:
                        try:
                            data_handle.seek(0)
                        except Exception:
                            pass
                        while True:
                            chunk = data_handle.read(64 * 1024)
                            if not chunk:
                                break
                            out.write(chunk)
                try:
                    dest_path.chmod(0o600)
                except Exception:
                    pass
                state.temp_path = dest_path
                state.total_size = dest_path.stat().st_size
                state.bytes_transferred = state.total_size
                state.progress = 1.0
            except Exception as e:
                log.exception("copying received resource failed")
                state.status = "FAILED"
                state.failure_reason = f"local_copy_failed: {e}"

            payload = self._make_completed_event(state)
            await self._hub.send_session(session.id, payload)
        else:
            state.failure_reason = state.failure_reason or state.status
            await self._hub.send_session(
                session.id,
                {"type": "resource.failed", **state.to_dict()},
            )

        # Release strong ref to the RNS resource + null callbacks to break cycles
        state.resource = None

    def _make_completed_event(self, state: TransferState) -> dict:
        body = state.to_dict(download_url=f"/resources/{state.transfer_id}/data")
        body["type"] = "resource.completed"
        if state.temp_path is not None:
            try:
                size = state.temp_path.stat().st_size
                if size <= self._config.resources.max_inline_bytes:
                    body["data_b64"] = base64.b64encode(state.temp_path.read_bytes()).decode("ascii")
            except FileNotFoundError:
                pass
        return body

    # ---------- send-side ----------

    async def send(
        self,
        session: "Session",
        link_id_hex: str,
        source: Any,  # bytes OR a Path to an already-uploaded file
        *,
        metadata: Optional[dict] = None,
        auto_compress: bool = True,
        await_complete: bool = False,
        timeout: Optional[float] = None,
    ) -> dict:
        try:
            link_hash = bytes.fromhex(link_id_hex.lower())
        except ValueError:
            raise ResourceError(f"invalid link id: {link_id_hex!r}") from None
        entry = session.open_links.get(link_hash)
        if entry is None:
            raise ResourceError(f"unknown link: {link_id_hex}")
        if getattr(entry.link, "status", None) != RNS.Link.ACTIVE:
            raise ResourceError(
                "link is not ACTIVE — wait for the link.established event or "
                "reopen the link with await_established=true before sending a resource"
            )

        transfer_id = secrets.token_hex(8)
        state = TransferState(
            transfer_id=transfer_id,
            session_id=session.id,
            direction="out",
            link_id_hex=link_id_hex,
            metadata=metadata,
        )
        state.done_event = asyncio.Event()
        session.active_transfers[transfer_id] = state

        if isinstance(source, (bytes, bytearray)):
            data_input = bytes(source)
            state.total_size = len(data_input)
            resource_data = data_input
        elif isinstance(source, Path):
            state.total_size = source.stat().st_size
            state.upload_temp_path = source
            resource_data = open(source, "rb")  # closed after transfer
        else:
            raise ResourceError("source must be bytes or a Path")

        session_id = session.id

        def _on_progress(res):
            try:
                progress = float(res.get_progress())
            except Exception:
                progress = state.progress
            state.progress = progress
            state.bytes_transferred = int(progress * state.total_size) if state.total_size else 0
            state.status = _status_str(getattr(res, "status", None))
            AsyncBridge.run_async(self._emit_progress_if_due(session, state))

        def _on_complete(res):
            status = getattr(res, "status", None)
            state.status = _status_str(status)
            state.completed_at = time.time()
            state.progress = 1.0 if status == RNS.Resource.COMPLETE else state.progress
            state.bytes_transferred = state.total_size if status == RNS.Resource.COMPLETE else state.bytes_transferred
            state.resource = None
            # Close the file handle if we opened one
            if isinstance(resource_data, (bytes, bytearray)) is False:
                try:
                    resource_data.close()
                except Exception:
                    pass
            # Clean up the upload temp file — the send is done
            if state.upload_temp_path is not None:
                try:
                    state.upload_temp_path.unlink(missing_ok=True)
                except Exception:
                    pass
                state.upload_temp_path = None

            if status == RNS.Resource.COMPLETE:
                event = {"type": "resource.sent", **state.to_dict()}
            else:
                state.failure_reason = state.failure_reason or state.status
                event = {"type": "resource.failed", **state.to_dict()}
            AsyncBridge.run_async(self._hub.send_session(session_id, event))

            if state.done_event is not None:
                loop = AsyncBridge.main_loop
                if loop is not None and loop.is_running():
                    loop.call_soon_threadsafe(state.done_event.set)

        try:
            resource = RNS.Resource(
                resource_data,
                entry.link,
                metadata=metadata,
                auto_compress=auto_compress,
                callback=_on_complete,
                progress_callback=_on_progress,
            )
        except Exception as e:
            session.active_transfers.pop(transfer_id, None)
            raise ResourceError(f"RNS refused to create Resource: {e}") from None

        state.resource = resource
        state.status = _status_str(getattr(resource, "status", None))

        await self._hub.send_session(
            session_id,
            {"type": "resource.started", **state.to_dict()},
        )

        if not await_complete:
            return {"awaited": False, **state.to_dict()}

        try:
            await asyncio.wait_for(state.done_event.wait(), timeout=timeout or 300)
        except asyncio.TimeoutError:
            return {"awaited": True, "kind": "timeout", **state.to_dict()}
        return {"awaited": True, "kind": state.status.lower(), **state.to_dict()}

    # ---------- query / cancel ----------

    def list_transfers(self, session: "Session", link_id_hex: Optional[str] = None) -> list[dict]:
        results = []
        for state in session.active_transfers.values():
            if link_id_hex is not None and state.link_id_hex.lower() != link_id_hex.lower():
                continue
            results.append(state.to_dict())
        return results

    def get_state(self, session: "Session", transfer_id: str) -> TransferState:
        state = session.active_transfers.get(transfer_id)
        if state is None:
            raise ResourceError(f"unknown transfer: {transfer_id}")
        return state

    def cancel(self, session: "Session", transfer_id: str) -> dict:
        state = self.get_state(session, transfer_id)
        # Cancel the RNS resource if still in flight.
        if state.resource is not None:
            try:
                state.resource.cancel()
            except Exception:
                log.exception("resource.cancel raised")
        state.status = "CANCELLED"
        state.failure_reason = state.failure_reason or "cancelled"
        if state.poller_task is not None:
            state.poller_task.cancel()
        # Remove temp file if present
        if state.temp_path is not None:
            try:
                state.temp_path.unlink(missing_ok=True)
            except Exception:
                pass
            state.temp_path = None
        if state.upload_temp_path is not None:
            try:
                state.upload_temp_path.unlink(missing_ok=True)
            except Exception:
                pass
            state.upload_temp_path = None
        # Signal any awaiter
        if state.done_event is not None and AsyncBridge.main_loop is not None:
            AsyncBridge.main_loop.call_soon_threadsafe(state.done_event.set)
        # Fire failed event
        AsyncBridge.run_async(
            self._hub.send_session(session.id, {"type": "resource.failed", **state.to_dict()})
        )
        return {"ok": True, "transfer_id": transfer_id}

    def delete(self, session: "Session", transfer_id: str) -> dict:
        state = self.get_state(session, transfer_id)
        # If still active, cancel first
        if state.status in ("TRANSFERRING", "QUEUED", "ADVERTISED", "PENDING", "ASSEMBLING", "AWAITING_PROOF"):
            self.cancel(session, transfer_id)
        # Remove the temp file if present
        if state.temp_path is not None:
            try:
                state.temp_path.unlink(missing_ok=True)
            except Exception:
                pass
        session.active_transfers.pop(transfer_id, None)
        return {"ok": True, "transfer_id": transfer_id}

    def open_stream(self, session: "Session", transfer_id: str) -> Path:
        """Return the temp file path for a completed inbound transfer.

        Raises ResourceError if the transfer isn't COMPLETE or the file has
        already been swept.
        """
        state = self.get_state(session, transfer_id)
        if state.direction != "in":
            raise ResourceError("transfer is outbound; no downloadable file")
        if state.status != "COMPLETE" or state.temp_path is None:
            raise ResourceError("transfer not complete")
        if not state.temp_path.exists():
            raise ResourceError("temp file has expired")
        return state.temp_path

    # ---------- cleanup ----------

    async def cleanup_session(self, session: "Session") -> None:
        for transfer_id, state in list(session.active_transfers.items()):
            if state.resource is not None:
                try:
                    state.resource.cancel()
                except Exception:
                    pass
                state.resource = None
            if state.poller_task is not None:
                state.poller_task.cancel()
            if state.temp_path is not None:
                try:
                    state.temp_path.unlink(missing_ok=True)
                except Exception:
                    pass
            if state.upload_temp_path is not None:
                try:
                    state.upload_temp_path.unlink(missing_ok=True)
                except Exception:
                    pass
        session.active_transfers.clear()
