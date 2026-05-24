# agent-event-bus

[![PyPI](https://img.shields.io/pypi/v/agent-event-bus.svg)](https://pypi.org/project/agent-event-bus/)
[![Python](https://img.shields.io/pypi/pyversions/agent-event-bus.svg)](https://pypi.org/project/agent-event-bus/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

**Tiny in-process pub/sub for agent loop events.**

Agent code grows messy fast. The LLM call site ends up knowing about the
logger, the dashboard, the cost tracker, the alert hook, and the audit
writer all at once. This library is the small bus that decouples them:
producers `emit("llm.call.start", ...)`, subscribers register for the
event types they care about, and nobody has to import anyone else.

Not a real message queue. No persistence, no cross-process delivery. Use
Redis, NATS, or RabbitMQ if you need that. Use this if you just want
clean wiring inside one Python process.

## Install

```bash
pip install agent-event-bus
```

## Basic use (sync)

```python
from agent_event_bus import EventBus, Event

bus = EventBus()

@bus.on("llm.call.start")
def log_start(event: Event):
    print(f"call started: {event.data}")

@bus.on("llm.call.end")
def log_end(event: Event):
    print(f"call ended in {event.data['ms']} ms")

bus.emit("llm.call.start", data={"model": "claude-opus-4-7", "tokens_in": 1000})
bus.emit("llm.call.end", data={"ms": 842})
```

## Async

```python
import asyncio
from agent_event_bus import EventBus

bus = EventBus()

async def write_to_db(event):
    await db.insert(event.type, event.data)

bus.on("tool.*", write_to_db)

async def main():
    await bus.emit_async("tool.call.end", data={"tool": "search", "ms": 12})

asyncio.run(main())
```

Sync handlers also run on the async path (inline). Async handlers run
concurrently via `asyncio.gather`.

## Wildcard patterns

```python
bus.on("llm.call.start", a)       # exact match only
bus.on("llm.*", b)                # single segment: matches llm.call, llm.error
bus.on("llm.**", c)               # any depth: matches llm.call.start.detail
bus.on("**", everything)          # firehose
```

Segment splitter is `.`. A single `*` matches exactly one segment. A `**`
matches one or more segments at the tail of the pattern.

## Error handling

By default, an exception in one handler does not stop the others. The
error is forwarded to a callback:

```python
bus = EventBus(handler_error_callback=lambda event, handler, exc: log.warning(...))
```

Strict mode re-raises the first handler error and stops dispatch:

```python
bus.set_strict(True)
```

## Optional history

```python
bus = EventBus(keep_history=True, history_size=500)
bus.emit("llm.call.start", data={...})
last = bus.history()
```

History is off by default to keep the bus zero-overhead.

## What it does NOT do

- No persistence, no cross-process delivery. Single process only.
- No backpressure or queues. Subscribers run inline on the emitter's
  thread (sync) or in the emitter's event loop (async).
- No topics-as-objects, no priorities. Order of dispatch is registration
  order.
- No middleware chain. If you want one, wrap the handler.

## Siblings

- [`agent-step-log`](https://pypi.org/project/agent-step-log/) is the
  per-step file writer. Plug it into the bus as one subscriber and every
  emitted event becomes a line on disk.
- [`agenttrace`](https://pypi.org/project/agenttrace/) is the whole-run
  cost + latency aggregator. Plug it in as another subscriber.

The bus is the wiring; the siblings are two of the things you wire into
it.

## License

MIT
