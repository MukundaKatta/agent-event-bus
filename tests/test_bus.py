import asyncio
import threading

import pytest

from agent_event_bus import Event, EventBus

# ---------- registration + exact match ----------


def test_exact_name_match_fires_handler():
    bus = EventBus()
    seen = []
    bus.on("llm.call.start", lambda e: seen.append(e))
    bus.emit("llm.call.start", data={"x": 1})
    assert len(seen) == 1
    assert seen[0].type == "llm.call.start"
    assert seen[0].data == {"x": 1}


def test_decorator_form_registers():
    bus = EventBus()
    seen = []

    @bus.on("a.b")
    def h(event: Event) -> None:
        seen.append(event)

    bus.emit("a.b")
    assert len(seen) == 1
    # decorator returns the original function so it remains callable
    assert callable(h)


def test_non_matching_event_does_not_fire():
    bus = EventBus()
    seen = []
    bus.on("llm.call.start", lambda e: seen.append(e))
    bus.emit("llm.call.end")
    assert seen == []


# ---------- wildcard matching ----------


def test_single_segment_wildcard_matches_one_level():
    bus = EventBus()
    seen = []
    bus.on("llm.*", lambda e: seen.append(e.type))
    bus.emit("llm.call")  # matches
    bus.emit("llm.error")  # matches
    bus.emit("llm.call.start")  # does NOT match (two segments after llm)
    bus.emit("tool.call")  # does not match different prefix
    assert seen == ["llm.call", "llm.error"]


def test_double_wildcard_matches_any_depth():
    bus = EventBus()
    seen = []
    bus.on("llm.**", lambda e: seen.append(e.type))
    bus.emit("llm.call")
    bus.emit("llm.call.start")
    bus.emit("llm.call.start.detail")
    bus.emit("tool.call")  # not under llm
    bus.emit("llm")  # ** requires at least one tail segment
    assert seen == ["llm.call", "llm.call.start", "llm.call.start.detail"]


def test_root_double_wildcard_is_firehose():
    bus = EventBus()
    seen = []
    bus.on("**", lambda e: seen.append(e.type))
    bus.emit("a")
    bus.emit("a.b")
    bus.emit("a.b.c")
    assert seen == ["a", "a.b", "a.b.c"]


def test_mixed_wildcard_in_middle():
    bus = EventBus()
    seen = []
    bus.on("llm.*.start", lambda e: seen.append(e.type))
    bus.emit("llm.call.start")  # matches
    bus.emit("llm.tool.start")  # matches
    bus.emit("llm.start")  # does not match (wrong shape)
    bus.emit("llm.call.start.detail")  # does not match (extra segment)
    assert seen == ["llm.call.start", "llm.tool.start"]


def test_invalid_pattern_rejected():
    bus = EventBus()
    with pytest.raises(ValueError):
        bus.on("", lambda e: None)
    with pytest.raises(ValueError):
        bus.on("a..b", lambda e: None)
    with pytest.raises(ValueError):
        # ** only allowed as final segment
        bus.on("a.**.b", lambda e: None)


# ---------- multiple subscribers + order ----------


def test_multiple_subscribers_all_fire():
    bus = EventBus()
    seen_a = []
    seen_b = []
    bus.on("x", lambda e: seen_a.append(e.type))
    bus.on("x", lambda e: seen_b.append(e.type))
    bus.emit("x")
    assert seen_a == ["x"]
    assert seen_b == ["x"]


def test_registration_order_preserved():
    bus = EventBus()
    seen = []
    bus.on("e", lambda evt: seen.append("a"))
    bus.on("e", lambda evt: seen.append("b"))
    bus.on("e", lambda evt: seen.append("c"))
    bus.emit("e")
    assert seen == ["a", "b", "c"]


def test_wildcard_and_exact_both_fire():
    bus = EventBus()
    seen = []
    bus.on("llm.call.start", lambda e: seen.append("exact"))
    bus.on("llm.*", lambda e: seen.append("wildcard"))
    bus.on("**", lambda e: seen.append("firehose"))
    bus.emit("llm.call.start")  # wildcard "llm.*" doesn't match (3 segs)
    # "**" matches, "llm.call.start" matches; "llm.*" doesn't
    assert seen == ["exact", "firehose"]


# ---------- off / clear ----------


def test_off_removes_subscriber():
    bus = EventBus()
    seen = []

    def h(e: Event) -> None:
        seen.append(e.type)

    bus.on("a", h)
    bus.emit("a")
    assert seen == ["a"]
    removed = bus.off("a", h)
    assert removed is True
    bus.emit("a")
    assert seen == ["a"]  # unchanged
    # off again is a no-op
    assert bus.off("a", h) is False


def test_clear_removes_all():
    bus = EventBus()
    bus.on("a", lambda e: None)
    bus.on("b.*", lambda e: None)
    assert len(bus.subscribers()) == 2
    bus.clear()
    assert bus.subscribers() == []


def test_subscribers_filter_by_pattern():
    bus = EventBus()
    h1 = lambda e: None  # noqa: E731
    h2 = lambda e: None  # noqa: E731
    h3 = lambda e: None  # noqa: E731
    bus.on("a", h1)
    bus.on("b", h2)
    bus.on("a", h3)
    pairs = bus.subscribers("a")
    assert len(pairs) == 2
    assert all(p == "a" for p, _ in pairs)


# ---------- error handling ----------


def test_handler_exception_caught_by_default():
    bus = EventBus()
    errors = []
    bus.set_strict(False)

    def boom(e: Event) -> None:
        raise RuntimeError("nope")

    seen_after = []
    bus.on("e", boom)
    bus.on("e", lambda e: seen_after.append(e.type))
    bus.on("e", lambda e: seen_after.append("again"))
    # default callback just logs; we override here to count
    bus2 = EventBus(handler_error_callback=lambda evt, h, exc: errors.append((evt.type, str(exc))))
    bus2.on("e", boom)
    bus2.on("e", lambda e: seen_after.append("from-bus2"))
    bus2.emit("e")
    assert errors == [("e", "nope")]
    assert "from-bus2" in seen_after

    # original bus also keeps going past the bad handler
    bus.emit("e")
    assert seen_after.count("e") == 1
    assert seen_after.count("again") == 1


def test_strict_mode_re_raises():
    bus = EventBus()
    bus.set_strict(True)
    bus.on("e", lambda e: (_ for _ in ()).throw(ValueError("bad")))
    with pytest.raises(ValueError, match="bad"):
        bus.emit("e")


def test_custom_error_callback_called():
    seen = []

    def cb(event, handler, exc):
        seen.append((event.type, type(exc).__name__))

    bus = EventBus(handler_error_callback=cb)

    def boom(_e: Event) -> None:
        raise KeyError("missing")

    bus.on("x.y", boom)
    bus.emit("x.y")
    assert seen == [("x.y", "KeyError")]


# ---------- history ----------


def test_history_off_by_default():
    bus = EventBus()
    bus.emit("a")
    assert bus.history() == []


def test_history_opt_in_records_events():
    bus = EventBus(keep_history=True, history_size=10)
    for i in range(5):
        bus.emit(f"e.{i}")
    h = bus.history()
    assert [e.type for e in h] == [f"e.{i}" for i in range(5)]


def test_history_caps_at_size():
    bus = EventBus(keep_history=True, history_size=3)
    for i in range(10):
        bus.emit(f"e.{i}")
    h = bus.history()
    assert len(h) == 3
    assert [e.type for e in h] == ["e.7", "e.8", "e.9"]


def test_history_n_param():
    bus = EventBus(keep_history=True, history_size=10)
    for i in range(5):
        bus.emit(f"e.{i}")
    assert len(bus.history(n=2)) == 2
    assert bus.history(n=2)[-1].type == "e.4"


# ---------- Event shape ----------


def test_event_has_id_and_timestamp():
    bus = EventBus()
    captured: list[Event] = []
    bus.on("a", lambda e: captured.append(e))
    bus.emit("a", data={"k": 1})
    assert len(captured) == 1
    e = captured[0]
    assert e.type == "a"
    assert e.data == {"k": 1}
    assert isinstance(e.id, str) and len(e.id) > 0
    assert isinstance(e.timestamp, float)


def test_event_is_frozen():
    e = Event(type="x")
    # FrozenInstanceError is a subclass of AttributeError; either is fine here.
    with pytest.raises(AttributeError):
        e.type = "y"  # type: ignore[misc]


# ---------- async ----------


async def test_async_handler_awaited_via_emit_async():
    bus = EventBus()
    seen = []

    async def h(e: Event) -> None:
        await asyncio.sleep(0)
        seen.append(e.type)

    bus.on("t.*", h)
    await bus.emit_async("t.go", data=1)
    assert seen == ["t.go"]


async def test_sync_handler_runs_inline_on_async_path():
    bus = EventBus()
    seen = []
    bus.on("e", lambda evt: seen.append(evt.type))
    await bus.emit_async("e")
    assert seen == ["e"]


async def test_async_handlers_run_concurrently():
    bus = EventBus()
    started = []
    done = []

    async def slow(name: str):
        async def _h(e: Event) -> None:
            started.append(name)
            await asyncio.sleep(0.02)
            done.append(name)

        return _h

    bus.on("e", await slow("a"))
    bus.on("e", await slow("b"))
    bus.on("e", await slow("c"))
    t0 = asyncio.get_event_loop().time()
    await bus.emit_async("e")
    elapsed = asyncio.get_event_loop().time() - t0
    # concurrent means total ~= 0.02s, not 0.06s
    assert elapsed < 0.05
    assert set(done) == {"a", "b", "c"}


async def test_async_strict_mode_re_raises():
    bus = EventBus()
    bus.set_strict(True)

    async def bad(e: Event) -> None:
        raise RuntimeError("boom-async")

    bus.on("e", bad)
    with pytest.raises(RuntimeError, match="boom-async"):
        await bus.emit_async("e")


async def test_async_error_callback_invoked():
    seen = []
    bus = EventBus(handler_error_callback=lambda evt, h, exc: seen.append(type(exc).__name__))

    async def bad(_e: Event) -> None:
        raise ValueError("nope")

    bus.on("e", bad)
    await bus.emit_async("e")
    assert seen == ["ValueError"]


# ---------- concurrency (threading) ----------


def test_concurrent_emits_do_not_lose_events():
    bus = EventBus()
    seen = []
    seen_lock = threading.Lock()

    def h(e: Event) -> None:
        with seen_lock:
            seen.append(e.type)

    bus.on("t", h)

    def worker(i: int):
        for _ in range(50):
            bus.emit("t", data=i)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    # 10 workers * 50 emits = 500 dispatches to single handler
    assert len(seen) == 500


def test_concurrent_subscribe_and_emit_is_safe():
    bus = EventBus()
    seen_lock = threading.Lock()
    seen = []

    def emit_worker():
        for _ in range(200):
            bus.emit("e")

    def sub_worker():
        for _ in range(50):
            def h(evt, seen=seen, seen_lock=seen_lock):
                with seen_lock:
                    seen.append(1)

            bus.on("e", h)

    threads = [
        threading.Thread(target=emit_worker),
        threading.Thread(target=emit_worker),
        threading.Thread(target=sub_worker),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    # we just want no crash; we don't care about exact counts because the
    # interleaving is non-deterministic.
    assert isinstance(seen, list)
