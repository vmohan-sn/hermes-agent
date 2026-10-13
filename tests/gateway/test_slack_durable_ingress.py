"""Durable Socket Mode receipt before ACK; replay after callback failure/restart."""

import asyncio
import sqlite3
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from plugins.platforms.slack.ingress import SlackIngressJournal, intercept_message_events, journal_path_for_app
from plugins.platforms.slack.adapter import SlackAdapter
from gateway.config import PlatformConfig
from gateway.platforms.base import ProcessingOutcome, MessageType, merge_pending_message_event


def _payload(event_id="Ev123"):
    return {
        "type": "event_callback", "event_id": event_id, "team_id": "T123",
        "event": {"type": "message", "channel": "C123", "user": "U123",
                  "text": "private test content", "ts": "123.456", "thread_ts": "120.000"},
    }


def test_pending_receipt_survives_restart_and_tombstone_suppresses_replay(tmp_path):
    path = tmp_path / "ingress.db"
    journal = SlackIngressJournal(path)
    assert journal.put(_payload()) is True
    assert journal.put(_payload()) is False
    journal = SlackIngressJournal(path)
    pending = journal.next_pending()
    assert pending == ("Ev123", _payload())
    journal.mark_done("Ev123")
    assert journal.next_pending() is None
    assert journal.put(_payload()) is False
    assert "private test content" not in path.read_bytes().decode("utf-8", errors="ignore")
    assert path.stat().st_mode & 0o077 == 0


def test_old_completed_receipts_are_pruned_but_pending_survive(tmp_path):
    path = tmp_path / "ingress.db"
    journal = SlackIngressJournal(path)
    journal.put(_payload("Ev-done"))
    journal.mark_done("Ev-done")
    journal.put(_payload("Ev-pending"))
    with sqlite3.connect(path) as db:
        db.execute("UPDATE receipts SET done_at=1 WHERE event_id='Ev-done'")
    reopened = SlackIngressJournal(path)
    with sqlite3.connect(path) as db:
        assert db.execute("SELECT count(*) FROM receipts WHERE event_id='Ev-done'").fetchone()[0] == 0
        assert db.execute("SELECT count(*) FROM receipts WHERE event_id='Ev-pending'").fetchone()[0] == 1


def test_app_tokens_have_isolated_private_journals(tmp_path):
    first = journal_path_for_app(tmp_path, "app-one-secret")
    second = journal_path_for_app(tmp_path, "app-two-secret")
    assert first != second
    assert first == journal_path_for_app(tmp_path, "app-one-secret")
    assert "secret" not in str(first)


def test_concurrent_journal_readers_claim_one_receipt_once(tmp_path):
    path = tmp_path / "ingress.db"
    first = SlackIngressJournal(path)
    second = SlackIngressJournal(path)
    first.put(_payload())
    assert first.next_pending() == ("Ev123", _payload())
    assert second.next_pending() is None
    first.defer("Ev123", 0)
    assert second.next_pending() == ("Ev123", _payload())


def test_retrying_failure_does_not_block_later_thread(tmp_path):
    journal = SlackIngressJournal(tmp_path / "ingress.db")
    journal.put(_payload("Ev-first"))
    journal.put(_payload("Ev-second"))
    journal.defer("Ev-first", 30)
    assert journal.next_pending()[0] == "Ev-second"
    journal.mark_done("Ev-second")
    assert journal.next_pending() is None


@pytest.mark.asyncio
async def test_socket_message_is_saved_before_ack_and_not_sent_to_bolt(tmp_path):
    journal = SlackIngressJournal(tmp_path / "ingress.db")
    order = []
    bolt = AsyncMock(side_effect=lambda *_: order.append("bolt"))
    client = SimpleNamespace(send_socket_mode_response=AsyncMock(
        side_effect=lambda *_: order.append("ack")))
    wake = asyncio.Event()
    receiver = intercept_message_events(journal, wake, bolt)
    req = SimpleNamespace(type="events_api", envelope_id="env-1", payload=_payload())
    await receiver(client, req)
    assert journal.next_pending() == ("Ev123", _payload())
    assert order == ["ack"]
    assert wake.is_set()
    await receiver(client, req)
    assert order == ["ack", "ack"]  # replayed envelope, never dispatched twice
    other = SimpleNamespace(type="slash_commands", envelope_id="env-2", payload={})
    await receiver(client, other)
    assert order[-1] == "bolt"


@pytest.mark.asyncio
async def test_failed_persist_never_acks(tmp_path):
    journal = SlackIngressJournal(tmp_path / "ingress.db")
    def fail(_payload):
        raise OSError("disk full")
    journal.put = fail
    client = SimpleNamespace(send_socket_mode_response=AsyncMock())
    receiver = intercept_message_events(journal, asyncio.Event(), AsyncMock())
    with pytest.raises(OSError):
        await receiver(client, SimpleNamespace(
            type="events_api", envelope_id="env-1", payload=_payload()))
    client.send_socket_mode_response.assert_not_awaited()


@pytest.mark.asyncio
async def test_pending_is_retried_after_callback_failure(tmp_path):
    journal = SlackIngressJournal(tmp_path / "ingress.db")
    journal.put(_payload())
    failures = 0
    deliveries = []
    async def dispatch(event, payload):
        nonlocal failures
        failures += 1
        if failures == 1:
            raise TimeoutError("temporary lookup failure")
        deliveries.append((event, payload))
    for _ in range(2):
        key, payload = journal.next_pending()
        try:
            await dispatch(payload["event"], payload)
        except TimeoutError:
            journal.defer(key, 0)
        else:
            journal.mark_done(key)
    assert len(deliveries) == 1
    assert deliveries[0][0]["thread_ts"] == "120.000"
    assert journal.next_pending() is None


@pytest.mark.asyncio
async def test_adapter_socket_listener_replays_pending_after_transient_failure(tmp_path, monkeypatch):
    """Exercise the actual adapter-to-socket wiring, not only the inbox API."""
    import plugins.platforms.slack.adapter as slack_adapter
    adapter = SlackAdapter(PlatformConfig(enabled=True, token="fake"))
    adapter._running = True
    adapter._app = object()
    adapter._app_token = "fake"
    adapter._proxy_url = None
    adapter._message_handler = AsyncMock()
    monkeypatch.setattr("hermes_constants.get_hermes_home", lambda: tmp_path)
    adapter._ingress_journal = SlackIngressJournal(journal_path_for_app(tmp_path, "fake"))
    adapter._handle_slack_message = AsyncMock(
        side_effect=[TimeoutError("temporary users.info failure"), None]
    )

    class FakeHandler:
        def __init__(self, *_args, **_kwargs):
            self.client = SimpleNamespace(
                socket_mode_request_listeners=[AsyncMock()],
                send_socket_mode_response=AsyncMock(),
            )

        async def start_async(self):
            await asyncio.Event().wait()

        async def close_async(self):
            pass

    monkeypatch.setattr(slack_adapter, "AsyncSocketModeHandler", FakeHandler)
    adapter._start_socket_mode_handler()
    req = SimpleNamespace(type="events_api", envelope_id="env-1", payload=_payload())
    await adapter._handler.client.socket_mode_request_listeners[0](adapter._handler.client, req)
    for _ in range(150):
        if adapter._handle_slack_message.await_count >= 2:
            break
        await asyncio.sleep(0.01)
    assert adapter._handle_slack_message.await_count == 2
    for _ in range(100):
        if adapter._ingress_journal.next_pending() is None:
            break
        await asyncio.sleep(0.01)
    assert adapter._ingress_journal.next_pending() is None
    adapter._running = False
    await adapter._stop_socket_mode_handler()


@pytest.mark.asyncio
async def test_adapter_failed_receipt_does_not_stall_next_thread(tmp_path):
    adapter = SlackAdapter(PlatformConfig(enabled=True, token="fake"))
    adapter._running = True
    adapter._message_handler = AsyncMock()
    adapter._ingress_journal = SlackIngressJournal(tmp_path / "ingress.db")
    adapter._ingress_journal.put(_payload("Ev-failed"))
    adapter._ingress_journal.put(_payload("Ev-good"))
    delivered = []

    async def dispatch(_event, payload):
        if payload["event_id"] == "Ev-failed":
            raise TimeoutError("first thread unavailable")
        delivered.append(payload["event_id"])
        adapter._running = False

    adapter._handle_slack_message = dispatch
    await asyncio.wait_for(adapter._drain_slack_ingress(), timeout=2)
    assert delivered == ["Ev-good"]
    assert adapter._ingress_journal.next_pending() is None


def test_merged_busy_messages_preserve_all_receipt_ids():
    def event(ident):
        return SimpleNamespace(message_type=MessageType.TEXT, text=ident,
                               media_urls=[], media_types=[],
                               metadata={"slack_ingress_receipt_ids": [ident]})
    pending = {"thread": event("Ev-first")}
    merge_pending_message_event(pending, "thread", event("Ev-second"), merge_text=True)
    assert pending["thread"].text == "Ev-first\nEv-second"
    assert pending["thread"].metadata["slack_ingress_receipt_ids"] == ["Ev-first", "Ev-second"]


@pytest.mark.asyncio
async def test_debounced_busy_messages_preserve_all_receipt_ids():
    adapter = SlackAdapter(PlatformConfig(enabled=True, token="fake"))
    source = SimpleNamespace(platform="slack", user_id="U123", chat_id="C123")
    def event(ident):
        return SimpleNamespace(message_type=MessageType.TEXT, text=ident,
                               media_urls=[], media_types=[], message_id=ident,
                               reply_to_message_id=None, source=source,
                               metadata={"slack_ingress_receipt_ids": [ident]})
    await adapter._queue_text_debounce("thread", event("Ev-first"))
    await adapter._queue_text_debounce("thread", event("Ev-second"))
    assert adapter._text_debounce["thread"].event.metadata["slack_ingress_receipt_ids"] == ["Ev-first", "Ev-second"]
    adapter._discard_text_debounce("thread")


@pytest.mark.asyncio
async def test_priority_rejected_turn_is_not_marked_done_on_empty_response(tmp_path):
    adapter = SlackAdapter(PlatformConfig(enabled=True, token="fake"))
    adapter._ingress_journal = SlackIngressJournal(tmp_path / "ingress.db")
    adapter._ingress_journal.put(_payload("Ev-rejected"))
    adapter._reactions_enabled = lambda: False
    event = SimpleNamespace(metadata={"slack_ingress_receipt_ids": ["Ev-rejected"], "slack_ingress_inline_failed": True}, message_id="123.456")
    await adapter.on_processing_complete(event, ProcessingOutcome.SUCCESS)
    with sqlite3.connect(adapter._ingress_journal.path) as db:
        assert db.execute("SELECT done_at FROM receipts WHERE event_id='Ev-rejected'").fetchone()[0] is None


@pytest.mark.asyncio
async def test_priority_queued_turn_stays_pending_until_its_own_completion(tmp_path):
    adapter = SlackAdapter(PlatformConfig(enabled=True, token="fake"))
    adapter._ingress_journal = SlackIngressJournal(tmp_path / "ingress.db")
    adapter._ingress_journal.put(_payload("Ev-queued"))
    adapter._reactions_enabled = lambda: False
    event = SimpleNamespace(metadata={"slack_ingress_receipt_ids": ["Ev-queued"], "slack_ingress_deferred": True}, message_id="123.456")
    await adapter.on_processing_complete(event, ProcessingOutcome.SUCCESS)
    with sqlite3.connect(adapter._ingress_journal.path) as db:
        assert db.execute("SELECT done_at FROM receipts WHERE event_id='Ev-queued'").fetchone()[0] is None
    event.metadata.pop("slack_ingress_deferred")
    await adapter.on_processing_complete(event, ProcessingOutcome.SUCCESS)
    with sqlite3.connect(adapter._ingress_journal.path) as db:
        assert db.execute("SELECT done_at FROM receipts WHERE event_id='Ev-queued'").fetchone()[0] is not None


@pytest.mark.asyncio
async def test_shutdown_cancelled_turn_remains_replayable(tmp_path):
    adapter = SlackAdapter(PlatformConfig(enabled=True, token="fake"))
    adapter._ingress_journal = SlackIngressJournal(tmp_path / "ingress.db")
    adapter._ingress_journal.put(_payload("Ev-cancelled"))
    adapter._reactions_enabled = lambda: False
    event = SimpleNamespace(metadata={"slack_ingress_receipt_ids": ["Ev-cancelled"]}, message_id="123.456")
    await adapter.on_processing_complete(event, ProcessingOutcome.CANCELLED)
    with sqlite3.connect(adapter._ingress_journal.path) as db:
        assert db.execute("SELECT done_at FROM receipts WHERE event_id='Ev-cancelled'").fetchone()[0] is None


@pytest.mark.asyncio
async def test_user_stop_cancelled_turn_does_not_replay(tmp_path):
    adapter = SlackAdapter(PlatformConfig(enabled=True, token="fake"))
    adapter._ingress_journal = SlackIngressJournal(tmp_path / "ingress.db")
    adapter._ingress_journal.put(_payload("Ev-user-stop"))
    adapter._reactions_enabled = lambda: False
    event = SimpleNamespace(metadata={"slack_ingress_receipt_ids": ["Ev-user-stop"]}, message_id="123.456")
    task = asyncio.current_task()
    task._gateway_user_cancelled = True
    try:
        await adapter.on_processing_complete(event, ProcessingOutcome.CANCELLED)
    finally:
        del task._gateway_user_cancelled
    with sqlite3.connect(adapter._ingress_journal.path) as db:
        assert db.execute("SELECT done_at FROM receipts WHERE event_id='Ev-user-stop'").fetchone()[0] is not None


@pytest.mark.asyncio
async def test_failed_merged_turn_releases_every_receipt_dedup_claim(tmp_path):
    adapter = SlackAdapter(PlatformConfig(enabled=True, token="fake"))
    adapter._ingress_journal = SlackIngressJournal(tmp_path / "ingress.db")
    for ident, ts in (("Ev-first", "123.001"), ("Ev-second", "123.002")):
        payload = _payload(ident)
        payload["event"]["ts"] = ts
        adapter._ingress_journal.put(payload)
        key = adapter._workspace_event_id("T123", ts)
        assert adapter._dedup.is_duplicate(key) is False
    adapter._reactions_enabled = lambda: False
    event = SimpleNamespace(metadata={"slack_ingress_receipt_ids": ["Ev-first", "Ev-second"]},
                            message_id="123.002")
    await adapter.on_processing_complete(event, ProcessingOutcome.FAILURE)
    for ts in ("123.001", "123.002"):
        assert adapter._dedup.is_duplicate(adapter._workspace_event_id("T123", ts)) is False


@pytest.mark.asyncio
async def test_inline_gateway_consumer_completes_receipt_without_background_hook(tmp_path):
    adapter = SlackAdapter(PlatformConfig(enabled=True, token="fake"))
    adapter._running = True
    adapter._message_handler = AsyncMock()
    adapter._ingress_journal = SlackIngressJournal(tmp_path / "ingress.db")
    adapter._ingress_journal.put(_payload("Ev-inline"))

    async def dispatch(event, payload):
        payload["_slack_ingress_inline_complete"] = True
    adapter._handle_slack_message = dispatch
    task = asyncio.create_task(adapter._drain_slack_ingress())
    await asyncio.sleep(0.1)
    adapter._running = False
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    with sqlite3.connect(adapter._ingress_journal.path) as db:
        assert db.execute("SELECT done_at FROM receipts WHERE event_id='Ev-inline'").fetchone()[0] is not None


@pytest.mark.asyncio
async def test_no_gateway_callback_never_completes_receipt(tmp_path):
    adapter = SlackAdapter(PlatformConfig(enabled=True, token="fake"))
    adapter._running = True
    adapter._ingress_journal = SlackIngressJournal(tmp_path / "ingress.db")
    adapter._ingress_journal.put(_payload("Ev-unrouted"))
    adapter._message_handler = None
    adapter._handle_slack_message = AsyncMock()
    task = asyncio.create_task(adapter._drain_slack_ingress())
    await asyncio.sleep(0.1)
    adapter._running = False
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    adapter._handle_slack_message.assert_not_awaited()
    with sqlite3.connect(adapter._ingress_journal.path) as db:
        assert db.execute("SELECT count(*) FROM receipts WHERE done_at IS NULL").fetchone()[0] == 1


@pytest.mark.asyncio
async def test_dead_ingress_worker_is_restarted_with_healthy_socket():
    adapter = SlackAdapter(PlatformConfig(enabled=True, token="fake"))
    adapter._running = True
    adapter._socket_watchdog_interval_s = 0.01
    adapter._socket_mode_task = asyncio.create_task(asyncio.Event().wait())
    adapter._ingress_worker_task = asyncio.create_task(asyncio.sleep(0))
    await adapter._ingress_worker_task
    adapter._handler = SimpleNamespace(client=SimpleNamespace(is_connected=lambda: True))
    async def idle_drain():
        await asyncio.Event().wait()
    adapter._drain_slack_ingress = idle_drain
    adapter._restart_socket_mode = AsyncMock()
    watchdog = asyncio.create_task(adapter._socket_watchdog_loop())
    for _ in range(100):
        if adapter._ingress_worker_task and not adapter._ingress_worker_task.done():
            break
        await asyncio.sleep(0.01)
    assert adapter._ingress_worker_task is not None
    assert not adapter._ingress_worker_task.done()
    adapter._restart_socket_mode.assert_not_awaited()
    adapter._running = False
    watchdog.cancel()
    adapter._ingress_worker_task.cancel()
    adapter._socket_mode_task.cancel()
    await asyncio.gather(watchdog, adapter._ingress_worker_task,
                         adapter._socket_mode_task, return_exceptions=True)


@pytest.mark.asyncio
async def test_accepted_message_receipt_waits_for_processing_result(tmp_path):
    adapter = SlackAdapter(PlatformConfig(enabled=True, token="fake"))
    adapter._running = True
    adapter._message_handler = AsyncMock()
    adapter._ingress_journal = SlackIngressJournal(tmp_path / "ingress.db")
    adapter._ingress_journal.put(_payload("Ev-running"))
    adapter._reactions_enabled = lambda: False
    accepted = asyncio.Event()
    async def dispatch(event, payload):
        payload["_slack_ingress_handed_off"] = True
        accepted.set()
        adapter._running = False
    adapter._handle_slack_message = dispatch
    await asyncio.wait_for(adapter._drain_slack_ingress(), timeout=2)
    assert accepted.is_set()
    assert adapter._ingress_journal.put(_payload("Ev-running")) is False
    # A restart must still have the receipt until actual processing completes.
    with sqlite3.connect(adapter._ingress_journal.path) as db:
        assert db.execute("SELECT count(*) FROM receipts WHERE done_at IS NULL").fetchone()[0] == 1
    event = SimpleNamespace(metadata={"slack_ingress_receipt_ids": ["Ev-running"]})
    await adapter.on_processing_complete(event, ProcessingOutcome.SUCCESS)
    with sqlite3.connect(adapter._ingress_journal.path) as db:
        assert db.execute("SELECT count(*) FROM receipts WHERE done_at IS NULL").fetchone()[0] == 0
