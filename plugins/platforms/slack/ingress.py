"""Small durable inbox for Slack Socket Mode message envelopes.

An Events API ACK is a transport receipt, not proof the async Bolt listener ran.
Persist the event before ACK, then replay pending events into the adapter.
Only message/app_mention events use this path; other Bolt handlers are unchanged.
"""

import asyncio
import hashlib
import json
import os
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Callable


def journal_path_for_app(home: Path, app_token: str) -> Path:
    """Separate Socket Mode apps without storing their credential in a path."""
    digest = hashlib.sha256(app_token.encode("utf-8")).hexdigest()[:24]
    return Path(home) / f"slack-ingress-{digest}.db"


class SlackIngressJournal:
    """ACK boundary and restart-safe pending inbox for a single Slack app."""

    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        # Create the file privately before SQLite opens it, and tighten any
        # pre-existing file whose permissions were accidentally broadened.
        fd = os.open(self.path, os.O_CREAT | os.O_RDWR, 0o600)
        os.fchmod(fd, 0o600)
        os.close(fd)
        with self._connect() as db:
            db.execute("""CREATE TABLE IF NOT EXISTS receipts (
                event_id TEXT PRIMARY KEY,
                payload TEXT,
                received_at REAL NOT NULL,
                done_at REAL,
                next_attempt_at REAL NOT NULL DEFAULT 0,
                claimed_until REAL NOT NULL DEFAULT 0
            )""")
            # Retain a bounded duplicate-suppression window; never prune
            # unresolved receipts, since they are the only replay source.
            db.execute("DELETE FROM receipts WHERE done_at IS NOT NULL AND done_at < ?",
                       (time.time() - 30 * 86400,))

    @contextmanager
    def _connect(self):
        db = sqlite3.connect(self.path, timeout=5)
        try:
            db.execute("PRAGMA synchronous=FULL")
            db.execute("PRAGMA secure_delete=ON")
            with db:
                yield db
        finally:
            db.close()

    @staticmethod
    def _key(payload: dict) -> str:
        event_id = payload.get("event_id")
        if event_id:
            return str(event_id)
        # Nonstandard Events API payload: stable across different envelope IDs
        # and retries, without storing the whole raw event as the index key.
        event = payload.get("event") or {}
        identity = [payload.get("team_id"), event.get("channel"),
                    event.get("type"), event.get("subtype"),
                    event.get("event_ts") or event.get("ts")]
        return hashlib.sha256(json.dumps(identity).encode("utf-8")).hexdigest()

    def put(self, payload: dict) -> bool:
        """Commit before ACK; duplicate receipts retain pending/done state."""
        key = self._key(payload)
        with self._connect() as db:
            cursor = db.execute(
                "INSERT OR IGNORE INTO receipts (event_id, payload, received_at, done_at) "
                "VALUES (?, ?, ?, NULL)",
                (key, json.dumps(payload, separators=(",", ":")), time.time()),
            )
            return cursor.rowcount == 1

    def next_pending(self):
        with self._connect() as db:
            # Serialize claim+read across distinct adapters sharing this app.
            db.execute("BEGIN IMMEDIATE")
            now = time.time()
            row = db.execute(
                "SELECT event_id, payload FROM receipts WHERE done_at IS NULL "
                "AND next_attempt_at <= ? AND claimed_until <= ? "
                "ORDER BY received_at LIMIT 1",
                (now, now),
            ).fetchone()
            if row:
                db.execute("UPDATE receipts SET claimed_until=? WHERE event_id=?",
                           (now + 30, row[0]))
        return (row[0], json.loads(row[1])) if row else None

    def defer(self, event_id: str, delay: float) -> None:
        with self._connect() as db:
            db.execute(
                "UPDATE receipts SET next_attempt_at=?, claimed_until=0 "
                "WHERE event_id=? AND done_at IS NULL",
                (time.time() + delay, event_id),
            )

    def reset_deferred_on_restart(self) -> None:
        """A previous process cannot still own any in-flight receipt."""
        with self._connect() as db:
            db.execute("UPDATE receipts SET next_attempt_at=0, claimed_until=0 "
                       "WHERE done_at IS NULL")

    def mark_done(self, event_id: str) -> None:
        # Retain an event-id tombstone across restarts so Slack redeliveries
        # cannot run the same turn again; scrub the full message body.
        with self._connect() as db:
            db.execute(
                "UPDATE receipts SET payload=NULL, done_at=? WHERE event_id=?",
                (time.time(), event_id),
            )

    def event_identity(self, event_id: str) -> tuple[str, str] | None:
        """Return the event's workspace and timestamp for retry dedup release."""
        with self._connect() as db:
            row = db.execute("SELECT payload FROM receipts WHERE event_id=?", (event_id,)).fetchone()
        if not row or not row[0]:
            return None
        payload = json.loads(row[0])
        event = payload.get("event") or {}
        return (str(event.get("team") or event.get("team_id") or payload.get("team_id") or ""),
                str(event.get("event_ts") or event.get("ts") or ""))


def intercept_message_events(
    journal: SlackIngressJournal,
    wakeup: asyncio.Event,
    bolt_handler: Callable,
):
    """Intercept message envelopes before Bolt's auto-ACK; pass others through."""
    async def receive(client, request):
        payload = request.payload
        event = payload.get("event") if isinstance(payload, dict) else None
        if (
            request.type == "events_api"
            and isinstance(event, dict)
            and event.get("type") in ("message", "app_mention")
        ):
            # No ACK on disk error; Slack is then free to retry the envelope.
            await asyncio.to_thread(journal.put, payload)
            from slack_sdk.socket_mode.response import SocketModeResponse
            await client.send_socket_mode_response(
                SocketModeResponse(envelope_id=request.envelope_id)
            )
            wakeup.set()
            return
        await bolt_handler(client, request)
    return receive
