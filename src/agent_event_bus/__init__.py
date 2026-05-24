"""agent-event-bus - tiny in-process pub/sub for agent loop events.

Producers emit typed events; subscribers register for the event types
they care about. Wildcards `*` (one segment) and `**` (any tail depth)
are supported. Sync and async handlers both work, on both `emit` and
`emit_async`.

    from agent_event_bus import EventBus, Event

    bus = EventBus()

    @bus.on("llm.call.start")
    def log_start(event: Event):
        print(f"call started: {event.data}")

    bus.emit("llm.call.start", data={"model": "claude-opus-4-7"})

Not a real message queue: no persistence, no cross-process delivery.
Sibling to `agent-step-log` (per-step file writer) and `agenttrace`
(whole-run aggregator); both plug into the bus as ordinary subscribers.
"""

from agent_event_bus.bus import (
    AsyncHandler,
    ErrorCallback,
    Event,
    EventBus,
    Handler,
    SyncHandler,
)

__version__ = "0.1.0"

__all__ = [
    "AsyncHandler",
    "ErrorCallback",
    "Event",
    "EventBus",
    "Handler",
    "SyncHandler",
    "__version__",
]
