"""Bridge from RNS worker threads to the asyncio event loop.

RNS callbacks (announces, packet callbacks, receipts, link events) fire on
RNS-owned threads. Everything user-visible in ReticulumAPI runs on aiohttp's
asyncio loop, so we bounce those callbacks with `run_coroutine_threadsafe`.

Adapted from MeshChatX's async_utils.py.
"""
from __future__ import annotations

import asyncio
import logging
import threading
from collections.abc import Coroutine
from typing import Any


log = logging.getLogger(__name__)


class AsyncBridge:
    main_loop: asyncio.AbstractEventLoop | None = None
    _pending_futures: list[Any] = []
    _pending_coroutines: list[Coroutine] = []
    _futures_lock = threading.Lock()
    _coroutines_lock = threading.Lock()
    _FUTURES_SWEEP_THRESHOLD = 32
    _COROUTINES_MAX = 256

    @staticmethod
    def set_main_loop(loop: asyncio.AbstractEventLoop) -> None:
        AsyncBridge.main_loop = loop
        with AsyncBridge._coroutines_lock:
            pending = AsyncBridge._pending_coroutines
            AsyncBridge._pending_coroutines = []
        for coro in pending:
            AsyncBridge.run_async(coro)

    @staticmethod
    def clear_main_loop() -> None:
        AsyncBridge.main_loop = None

    @staticmethod
    def run_async(coroutine: Coroutine) -> None:
        """Schedule *coroutine* on the main event loop from any thread.

        Returned futures are tracked so they (and the closures they reference)
        can be garbage-collected promptly once finished.
        """
        loop = AsyncBridge.main_loop
        if loop is not None and loop.is_running():
            future = asyncio.run_coroutine_threadsafe(coroutine, loop)
            with AsyncBridge._futures_lock:
                AsyncBridge._pending_futures.append(future)
                if len(AsyncBridge._pending_futures) >= AsyncBridge._FUTURES_SWEEP_THRESHOLD:
                    AsyncBridge._pending_futures = [
                        f for f in AsyncBridge._pending_futures if not f.done()
                    ]
            return

        # Loop not up yet — buffer the coroutine but cap the backlog.
        with AsyncBridge._coroutines_lock:
            AsyncBridge._pending_coroutines.append(coroutine)
            if len(AsyncBridge._pending_coroutines) > AsyncBridge._COROUTINES_MAX:
                dropped = len(AsyncBridge._pending_coroutines) - AsyncBridge._COROUTINES_MAX
                AsyncBridge._pending_coroutines = AsyncBridge._pending_coroutines[
                    -AsyncBridge._COROUTINES_MAX :
                ]
                log.warning(
                    "dropped %d buffered coroutine(s) — event loop is not running",
                    dropped,
                )
