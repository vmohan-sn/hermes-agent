"""Regression guard for Slack bot/workflow-sender authorization bypass.

Mirrors tests/gateway/test_feishu_bot_auth_bypass.py for Platform.SLACK.

Slack Workflow Builder posts (and other app/bot messages) arrive as
``subtype=bot_message`` with ``user=None``, so the SessionSource carries
``is_bot=True`` and ``user_id=None``. Without the #4466 bot bypass running
*before* the no-user-id guard, these senders are rejected at
``_is_user_authorized`` even when the operator enabled ``SLACK_ALLOW_BOTS`` --
the bug that makes @mentioning the bot from a Slack workflow do nothing.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from agent.secret_scope import (
    reset_secret_scope,
    set_multiplex_active,
    set_secret_scope,
)
from gateway.config import PlatformConfig
from gateway.session import Platform, SessionSource


@pytest.fixture(autouse=True)
def _isolate_slack_env(monkeypatch):
    for var in (
        "SLACK_ALLOW_BOTS",
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


def _make_slack_bot_source():
    # Workflow Builder / app posts: subtype=bot_message, user=None.
    return SessionSource(
        platform=Platform.SLACK,
        chat_id="C0123",
        chat_type="group",
        user_id=None,
        user_name="",
        is_bot=True,
    )


def _make_slack_human_source(user_id="U_human"):
    return SessionSource(
        platform=Platform.SLACK,
        chat_id="C0123",
        chat_type="group",
        user_id=user_id,
        user_name="Human",
        is_bot=False,
    )


def test_slack_bot_authorized_when_allow_bots_all(monkeypatch):
    runner = _make_bare_runner()
    monkeypatch.setenv("SLACK_ALLOW_BOTS", "all")
    assert runner._is_user_authorized(_make_slack_bot_source()) is True


def test_slack_bot_authorized_from_adapter_config_when_env_absent():
    """Config-only allow_bots must authorize Workflow Builder posts."""
    runner = _make_bare_runner()
    runner.adapters = {
        Platform.SLACK: SimpleNamespace(
            config=PlatformConfig(enabled=True, extra={"allow_bots": "all"})
        )
    }

    assert runner._is_user_authorized(_make_slack_bot_source()) is True


def test_secondary_profile_does_not_borrow_global_allow_bots(monkeypatch):
    """A multiplex profile with no bot policy must remain fail-closed."""
    runner = _make_bare_runner()
    runner.adapters = {}
    runner._profile_adapters = {
        "jira-marketing": {
            Platform.SLACK: SimpleNamespace(
                config=PlatformConfig(enabled=True, extra={}),
                _slack_allow_bots=lambda: "all",
            )
        }
    }
    source = _make_slack_bot_source()
    source.profile = "jira-marketing"

    monkeypatch.setenv("SLACK_ALLOW_BOTS", "all")
    set_multiplex_active(True)
    token = set_secret_scope({})
    try:
        assert runner._is_user_authorized(source) is False
    finally:
        reset_secret_scope(token)
        set_multiplex_active(False)


def test_adapter_config_none_overrides_environment_all(monkeypatch):
    """The selected Slack profile config is authoritative over global env."""
    runner = _make_bare_runner()
    runner.adapters = {
        Platform.SLACK: SimpleNamespace(
            config=PlatformConfig(enabled=True, extra={"allow_bots": "none"})
        )
    }
    monkeypatch.setenv("SLACK_ALLOW_BOTS", "all")

    assert runner._is_user_authorized(_make_slack_bot_source()) is False


def test_slack_human_unaffected_by_bot_bypass(monkeypatch):
    runner = _make_bare_runner()
    monkeypatch.setenv("SLACK_ALLOW_ALL_USERS", "true")
    assert runner._is_user_authorized(_make_slack_human_source()) is True
