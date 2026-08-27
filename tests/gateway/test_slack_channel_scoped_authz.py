"""Channel-scoped Slack authorization for shared SambaNova channels."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from gateway.session import Platform, SessionSource


@pytest.fixture(autouse=True)
def _isolate_slack_env(monkeypatch):
    for var in (
        "SLACK_ALLOWED_CHANNELS",
        "SLACK_ALLOWED_USERS",
        "SLACK_ALLOW_ALL_USERS",
        "GATEWAY_ALLOW_ALL_USERS",
        "GATEWAY_ALLOWED_USERS",
    ):
        monkeypatch.delenv(var, raising=False)


def _make_bare_runner():
    from gateway.run import GatewayRunner

    runner = object.__new__(GatewayRunner)
    runner.pairing_store = SimpleNamespace(is_approved=lambda *_a, **_kw: False)
    return runner


def _source(*, chat_id: str, chat_type: str, user_id: str) -> SessionSource:
    return SessionSource(
        platform=Platform.SLACK,
        chat_id=chat_id,
        chat_type=chat_type,
        user_id=user_id,
        user_name="Human",
        is_bot=False,
    )


def test_slack_approved_channel_authorizes_any_human_but_not_their_dm(monkeypatch):
    runner = _make_bare_runner()
    monkeypatch.setenv("SLACK_ALLOWED_CHANNELS", "C_APPROVED,C_OTHER")
    monkeypatch.setenv("SLACK_ALLOWED_USERS", "U_VASANTH")

    channel = _source(chat_id="C_APPROVED", chat_type="group", user_id="U_COLLEAGUE")
    dm = _source(chat_id="D_COLLEAGUE", chat_type="dm", user_id="U_COLLEAGUE")

    assert runner._is_user_authorized(channel) is True
    assert runner._is_user_authorized(dm) is False


def test_slack_unapproved_channel_remains_denied(monkeypatch):
    runner = _make_bare_runner()
    monkeypatch.setenv("SLACK_ALLOWED_CHANNELS", "C_APPROVED")
    monkeypatch.setenv("SLACK_ALLOWED_USERS", "U_VASANTH")

    source = _source(chat_id="C_UNAPPROVED", chat_type="group", user_id="U_COLLEAGUE")

    assert runner._is_user_authorized(source) is False


def test_slack_allowlisted_owner_keeps_dm_access(monkeypatch):
    runner = _make_bare_runner()
    monkeypatch.setenv("SLACK_ALLOWED_CHANNELS", "C_APPROVED")
    monkeypatch.setenv("SLACK_ALLOWED_USERS", "U_VASANTH")

    dm = _source(chat_id="D_VASANTH", chat_type="dm", user_id="U_VASANTH")

    assert runner._is_user_authorized(dm) is True
