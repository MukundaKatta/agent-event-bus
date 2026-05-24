"""Core EventBus implementation."""

from __future__ import annotations

import asyncio
import inspect
import logging
import threading
import time
import uuid
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

# A handler can be sync or async. Both shapes take a single Event arg.
SyncHandler = Callable[["Event"], None]
AsyncHandler = Callable[["Event"], Awaitable[None]]
Handler = SyncHandler | AsyncHandler

# Called when a handler raises and the bus is not in strict mode.
ErrorCallback = Callable[["Event", Handler, BaseException], None]


@dataclass(frozen=True)
class Event:
    """An emitted event.

    Attributes:
        type: dotted event type, e.g. "llm.call.start".
        data: arbitrary payload.
        timestamp: monotonic timestamp at emit time (seconds).
        id: uuid4 string, unique per emission.
    """

    type: str
    data: Any = None
    timestamp: float = field(default_factory=time.monotonic)
    id: str = field(default_factory=lambda: str(uuid.uuid4()))


class EventBus:
    """In-process pub/sub for agent loop events.

    Subscribers register patterns; producers emit events. Wildcards `*`
    (single segment) and `**` (any depth at tail) are supported.

    The bus is thread-safe for subscribe/unsubscribe/emit. Subscribers
    run inline on the caller's thread (sync) or event loop (async). For
    cross-process delivery, persistence, or backpressure, use a real
    message queue.
    """

    def __init__(
        self,
        *,
        keep_history: bool = False,
        history_size: int = 100,
        handler_error_callback: ErrorCallback | None = None,
    ) -> None:
        if history_size < 1:
            raise ValueError("history_size must be >= 1")
        # List of (pattern, handler) in registration order. We keep this
        # ordered so dispatch is deterministic.
        self._subs: list[tuple[str, Handler]] = []
        self._lock = threading.RLock()
        self._strict = False
        self._keep_history = keep_history
        self._history: deque[Event] | None = (
            deque(maxlen=history_size) if keep_history else None
        )
        self._error_cb: ErrorCallback = (
            handler_error_callback if handler_error_callback is not None else _default_error_cb
        )

    # ---- subscription ----

    def on(
        self, pattern: str, handler: Handler | None = None
    ) -> Handler | Callable[[Handler], Handler]:
        """Register `handler` for `pattern`. Usable as a decorator:

            @bus.on("llm.*")
            def h(event): ...

        or as a direct call:

            bus.on("llm.*", h)
        """
        _validate_pattern(pattern)
        if handler is None:
            # decorator form
            def decorator(h: Handler) -> Handler:
                with self._lock:
                    self._subs.append((pattern, h))
                return h

            return decorator
        with self._lock:
            self._subs.append((pattern, handler))
        return handler

    def off(self, pattern: str, handler: Handler) -> bool:
        """Remove the (pattern, handler) pair. Returns True if a sub was
        removed, False if no matching pair was registered. Only removes
        the first match (registration order)."""
        with self._lock:
            for i, (p, h) in enumerate(self._subs):
                if p == pattern and h is handler:
                    del self._subs[i]
                    return True
        return False

    def clear(self) -> None:
        """Remove all subscribers."""
        with self._lock:
            self._subs.clear()

    def subscribers(self, pattern: str | None = None) -> list[tuple[str, Handler]]:
        """Return registered (pattern, handler) pairs.

        If `pattern` is given, returns only entries whose registered
        pattern is exactly equal to it.
        """
        with self._lock:
            if pattern is None:
                return list(self._subs)
            return [(p, h) for p, h in self._subs if p == pattern]

    # ---- emission ----

    def emit(self, type: str, data: Any = None) -> Event:
        """Emit `type` with `data` to all matching handlers synchronously.

        Async handlers are scheduled via `asyncio.run(handler(...))` if no
        loop is running; otherwise dispatched as fire-and-forget tasks.
        For predictable async handling, prefer `emit_async`.
        """
        event = Event(type=type, data=data)
        self._record_history(event)
        for handler in self._match(type):
            try:
                result = handler(event)
                if inspect.isawaitable(result):
                    # sync emit + async handler: best-effort dispatch
                    self._run_awaitable_inline(result)
            except BaseException as exc:  # noqa: BLE001
                if self._strict:
                    raise
                self._safe_error_cb(event, handler, exc)
        return event

    async def emit_async(self, type: str, data: Any = None) -> Event:
        """Emit `type` with `data` and await all matching handlers.

        Sync handlers run inline. Async handlers are gathered so they run
        concurrently. Exceptions are handled per the bus's strict-mode
        setting just like `emit`.
        """
        event = Event(type=type, data=data)
        self._record_history(event)
        async_tasks: list[Awaitable[Any]] = []
        async_handlers: list[Handler] = []
        for handler in self._match(type):
            try:
                result = handler(event)
            except BaseException as exc:  # noqa: BLE001
                if self._strict:
                    raise
                self._safe_error_cb(event, handler, exc)
                continue
            if inspect.isawaitable(result):
                async_tasks.append(result)
                async_handlers.append(handler)
        if async_tasks:
            results = await asyncio.gather(*async_tasks, return_exceptions=True)
            for handler, result in zip(async_handlers, results, strict=True):
                if isinstance(result, BaseException):
                    if self._strict:
                        raise result
                    self._safe_error_cb(event, handler, result)
        return event

    # ---- options ----

    def set_strict(self, strict: bool = True) -> None:
        """Toggle strict mode. In strict mode, the first handler error
        is re-raised and remaining handlers are not invoked."""
        self._strict = bool(strict)

    @property
    def strict(self) -> bool:
        return self._strict

    # ---- history ----

    def history(self, n: int = 100) -> list[Event]:
        """Return up to the last `n` events. Empty list if history is off."""
        if self._history is None:
            return []
        if n < 0:
            raise ValueError("n must be >= 0")
        with self._lock:
            buf = list(self._history)
        if n >= len(buf):
            return buf
        return buf[-n:]

    # ---- internals ----

    def _match(self, event_type: str) -> list[Handler]:
        """Snapshot of handlers whose pattern matches `event_type`, in
        registration order. Taken under the lock so we don't race with
        subscribe/unsubscribe during dispatch."""
        with self._lock:
            return [h for p, h in self._subs if _matches(p, event_type)]

    def _record_history(self, event: Event) -> None:
        if self._history is None:
            return
        with self._lock:
            self._history.append(event)

    def _safe_error_cb(self, event: Event, handler: Handler, exc: BaseException) -> None:
        try:
            self._error_cb(event, handler, exc)
        except BaseException:  # noqa: BLE001
            # error callback itself blew up; never let bus dispatch die because
            # of an observer.
            logger.exception("agent_event_bus: handler_error_callback raised")

    @staticmethod
    def _run_awaitable_inline(awaitable: Awaitable[Any]) -> None:
        """Best-effort run of an async handler called from sync `emit`.

        If a loop is already running on this thread (e.g. we are inside an
        async framework that called `emit` synchronously), we schedule the
        awaitable as a task and return; the caller asked for sync emit so
        we do not await. If no loop is running, we drive the awaitable to
        completion via `asyncio.run`.
        """
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        if loop is None:
            asyncio.run(_as_coro(awaitable))
            return
        loop.create_task(_as_coro(awaitable))


def _as_coro(awaitable: Awaitable[Any]) -> Any:
    """Wrap an arbitrary awaitable in a coroutine so it can pass to
    asyncio.run / loop.create_task."""

    async def _runner() -> Any:
        return await awaitable

    return _runner()


def _default_error_cb(event: Event, handler: Handler, exc: BaseException) -> None:
    logger.warning(
        "agent_event_bus: handler %r raised on event %r: %r",
        getattr(handler, "__name__", handler),
        event.type,
        exc,
    )


# ---- pattern matching ----


def _validate_pattern(pattern: str) -> None:
    if not isinstance(pattern, str) or not pattern:
        raise ValueError("pattern must be a non-empty string")
    segments = pattern.split(".")
    for i, seg in enumerate(segments):
        if not seg:
            raise ValueError(f"pattern has empty segment: {pattern!r}")
        if seg == "**" and i != len(segments) - 1:
            raise ValueError(
                f"`**` is only allowed as the final segment of a pattern: {pattern!r}"
            )


def _matches(pattern: str, event_type: str) -> bool:
    """Return True if `event_type` (dotted string) matches `pattern`.

    Rules:
      * exact: "llm.call.start" matches only itself.
      * "*"  : matches exactly one segment.
      * "**" : tail-only; matches one or more trailing segments.
    """
    if pattern == event_type:
        return True
    p_parts = pattern.split(".")
    e_parts = event_type.split(".")
    # Handle `**` as tail.
    if p_parts and p_parts[-1] == "**":
        head = p_parts[:-1]
        if len(e_parts) <= len(head):
            # `**` must consume at least one segment.
            return False
        for pp, ep in zip(head, e_parts, strict=False):
            if pp == "*":
                continue
            if pp != ep:
                return False
        return True
    # No `**`. Lengths must match exactly.
    if len(p_parts) != len(e_parts):
        return False
    for pp, ep in zip(p_parts, e_parts, strict=False):
        if pp == "*":
            continue
        if pp != ep:
            return False
    return True
