import time

import pytest

from services.security import SlidingWindowLimiter, redact_secrets, tokens_match
from tests.test_telegram_orchestrator import (
    TEST_AUTH_TOKEN,
    TEST_CHAT_ID,
    telegram_update,
    webhook_headers,
)

pytestmark = pytest.mark.anyio


def test_tokens_match_is_fail_closed():
    assert tokens_match("abc123", "abc123")
    assert not tokens_match("abc123", "abc124")
    assert not tokens_match("", "abc123")
    assert not tokens_match(None, "abc123")
    assert not tokens_match("abc123", "")
    assert not tokens_match("", "")


def test_tokens_match_handles_non_ascii_without_raising():
    assert not tokens_match("tökën", "token")
    assert tokens_match("tökën", "tökën")


def test_redact_secrets_masks_configured_values_and_bot_urls(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-super-secret-value")
    text = (
        "failed calling sk-ant-super-secret-value at "
        "https://api.telegram.org/bot123456:ABC-def_GHI/sendMessage"
    )
    cleaned = redact_secrets(text)
    assert "sk-ant-super-secret-value" not in cleaned
    assert "ABC-def_GHI" not in cleaned


def test_sliding_window_limiter_blocks_then_recovers():
    limiter = SlidingWindowLimiter()
    assert limiter.allow("k", 2, 60, now=0)
    assert limiter.allow("k", 2, 60, now=1)
    assert not limiter.allow("k", 2, 60, now=2)
    assert limiter.allow("k", 2, 60, now=61)


async def test_webhook_non_ascii_secret_is_401_not_500(app_client):
    response = await app_client.post(
        "/api/v1/telegram/webhook",
        headers={"X-Telegram-Bot-Api-Secret-Token": "tökën".encode("utf-8")},
        json=telegram_update("/status"),
    )
    assert response.status_code == 401


async def test_dedicated_webhook_secret_replaces_helix_token(
    app_client, monkeypatch
):
    monkeypatch.setenv("TELEGRAM_WEBHOOK_SECRET", "dedicated-webhook-secret")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", TEST_CHAT_ID)

    old = await app_client.post(
        "/api/v1/telegram/webhook",
        headers=webhook_headers(TEST_AUTH_TOKEN),
        json=telegram_update("/status"),
    )
    assert old.status_code == 401

    new = await app_client.post(
        "/api/v1/telegram/webhook",
        headers=webhook_headers("dedicated-webhook-secret"),
        json={"update_id": 1, "message": {"text": "/status"}},
    )
    assert new.status_code == 200


async def test_stale_or_undated_update_is_dropped(app_client, monkeypatch):
    monkeypatch.setenv("TELEGRAM_CHAT_ID", TEST_CHAT_ID)

    stale = telegram_update("/status")
    stale["message"]["date"] = int(time.time()) - 3600
    response = await app_client.post(
        "/api/v1/telegram/webhook", headers=webhook_headers(), json=stale
    )
    assert response.json()["status"] == "stale_dropped"

    undated = telegram_update("/status")
    del undated["message"]["date"]
    response = await app_client.post(
        "/api/v1/telegram/webhook", headers=webhook_headers(), json=undated
    )
    assert response.json()["status"] == "stale_dropped"


async def test_oversized_body_is_rejected(app_client):
    response = await app_client.post(
        "/api/v1/telegram/webhook",
        headers=webhook_headers(),
        content=b"x" * (70 * 1024),
    )
    assert response.status_code == 413


async def test_bot_sender_is_dropped(app_client, monkeypatch):
    monkeypatch.setenv("TELEGRAM_CHAT_ID", TEST_CHAT_ID)
    payload = telegram_update("/status")
    payload["message"]["from"]["is_bot"] = True
    response = await app_client.post(
        "/api/v1/telegram/webhook", headers=webhook_headers(), json=payload
    )
    assert response.json()["status"] == "unauthorized_user_dropped"


async def test_costly_command_is_throttled_on_repeat(
    app_module, app_client, db_pool_factory, monkeypatch
):
    pool, _, _ = db_pool_factory(fetchone_sequence=[(1,), (2,)])
    app_module.app.state.db_pool = pool
    monkeypatch.setenv("TELEGRAM_CHAT_ID", TEST_CHAT_ID)
    monkeypatch.setattr(
        "telegram_orchestrator.emit_executive_notification", lambda event: None
    )
    monkeypatch.setattr(
        "telegram_orchestrator.is_vendor_circuit_healthy",
        lambda *a, **k: _false(),
    )

    first = await app_client.post(
        "/api/v1/telegram/webhook",
        headers=webhook_headers(),
        json=telegram_update("/optimize", update_id=101),
    )
    second = await app_client.post(
        "/api/v1/telegram/webhook",
        headers=webhook_headers(),
        json=telegram_update("/optimize", update_id=102),
    )

    assert first.json()["status"] == "processed"
    assert second.json()["status"] == "throttled"


async def _false():
    return False
