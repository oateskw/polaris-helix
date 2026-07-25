from unittest.mock import AsyncMock, MagicMock

import pytest

pytestmark = pytest.mark.anyio
TEST_AUTH_TOKEN = "helix_super_secure_test_token"
TEST_CHAT_ID = "8828510879"


def telegram_update(text: str, update_id: int = 1) -> dict:
    return {
        "update_id": update_id,
        "message": {
            "from": {"id": int(TEST_CHAT_ID)},
            "chat": {"id": int(TEST_CHAT_ID)},
            "text": text,
        },
    }


def webhook_headers(secret: str = TEST_AUTH_TOKEN) -> dict[str, str]:
    return {"X-Telegram-Bot-Api-Secret-Token": secret}


async def test_webhook_requires_helix_auth_token(app_client):
    response = await app_client.post(
        "/api/v1/telegram/webhook", json=telegram_update("/status")
    )
    assert response.status_code == 401


async def test_webhook_ignores_malformed_message(app_client):
    response = await app_client.post(
        "/api/v1/telegram/webhook",
        headers=webhook_headers(),
        json={"update_id": 1, "message": {"text": "/status"}},
    )
    assert response.status_code == 200
    assert response.json()["status"] == "ignored"


async def test_webhook_rejects_sender_or_chat_mismatch(
    app_module, app_client, monkeypatch
):
    monkeypatch.setenv("TELEGRAM_CHAT_ID", TEST_CHAT_ID)
    payload = telegram_update("/status")
    payload["message"]["chat"]["id"] = 999

    response = await app_client.post(
        "/api/v1/telegram/webhook", headers=webhook_headers(), json=payload
    )

    assert response.json()["status"] == "unauthorized_user_dropped"


async def test_duplicate_update_is_not_reprocessed(
    app_module, app_client, db_pool_factory, monkeypatch
):
    pool, _, _ = db_pool_factory(fetchone_sequence=[None])
    app_module.app.state.db_pool = pool
    monkeypatch.setenv("TELEGRAM_CHAT_ID", TEST_CHAT_ID)

    response = await app_client.post(
        "/api/v1/telegram/webhook",
        headers=webhook_headers(),
        json=telegram_update("/status", update_id=44),
    )

    assert response.status_code == 200
    assert response.json()["status"] == "duplicate"


async def test_exact_command_parsing_does_not_execute_prefix_match(
    app_module, app_client, db_pool_factory, monkeypatch
):
    pool, _, _ = db_pool_factory(fetchone_sequence=[(45,)])
    app_module.app.state.db_pool = pool
    monkeypatch.setenv("TELEGRAM_CHAT_ID", TEST_CHAT_ID)
    notify = MagicMock()
    monkeypatch.setattr("telegram_orchestrator.emit_executive_notification", notify)

    response = await app_client.post(
        "/api/v1/telegram/webhook",
        headers=webhook_headers(),
        json=telegram_update("/executefoo", update_id=45),
    )

    assert response.status_code == 200
    event = notify.call_args.args[0]
    assert event.headline == "Unknown Telegram Command"


async def test_bot_suffix_command_is_supported(
    app_module, app_client, db_pool_factory, monkeypatch
):
    pool, _, _ = db_pool_factory(fetchone_sequence=[(46,)])
    app_module.app.state.db_pool = pool
    monkeypatch.setenv("TELEGRAM_CHAT_ID", TEST_CHAT_ID)
    monkeypatch.setattr(
        "telegram_orchestrator.emit_executive_notification", MagicMock()
    )

    response = await app_client.post(
        "/api/v1/telegram/webhook",
        headers=webhook_headers(),
        json=telegram_update("/help@PolarisBot", update_id=46),
    )

    assert response.status_code == 200
    assert response.json()["status"] == "processed"


async def test_execute_rejects_non_dispatchable_logging_agent(
    app_module, app_client, db_pool_factory, monkeypatch
):
    pool, _, _ = db_pool_factory(fetchone_sequence=[(47,)])
    app_module.app.state.db_pool = pool
    monkeypatch.setenv("TELEGRAM_CHAT_ID", TEST_CHAT_ID)
    notify = MagicMock()
    monkeypatch.setattr("telegram_orchestrator.emit_executive_notification", notify)

    response = await app_client.post(
        "/api/v1/telegram/webhook",
        headers=webhook_headers(),
        json=telegram_update("/execute chiron", update_id=47),
    )

    assert response.status_code == 200
    assert notify.call_args.args[0].headline == "Execution Command Rejected"


async def test_execute_advances_strategy_only_after_accepted_dispatch(
    app_module, app_client, db_pool_factory, monkeypatch
):
    strategy = {
        "industry_id": 12,
        "industry_name": "Logistics",
        "moltset_category": "Transportation",
        "bottleneck_profile": "Manual dispatching",
        "pain_tokens": ["delay"],
        "target_region": "Texas",
    }
    pool, _, cursors = db_pool_factory(fetchone_sequence=[(48,), strategy])
    app_module.app.state.db_pool = pool
    monkeypatch.setenv("TELEGRAM_CHAT_ID", TEST_CHAT_ID)
    monkeypatch.setattr(
        "telegram_orchestrator.load_vendor_isolation_matrix",
        AsyncMock(return_value={}),
        raising=False,
    )
    monkeypatch.setattr(
        "services.mesh_client.trigger_downstream_mesh_with_payload",
        AsyncMock(return_value={"nova": True}),
    )
    monkeypatch.setattr(
        "telegram_orchestrator.emit_executive_notification", MagicMock()
    )

    response = await app_client.post(
        "/api/v1/telegram/webhook",
        headers=webhook_headers(),
        json=telegram_update("/execute nova", update_id=48),
    )

    assert response.status_code == 200
    executed_sql = " ".join(query for query, _ in cursors[0].executed)
    assert "SET last_targeted_at = NOW()" in executed_sql


async def test_execute_does_not_advance_strategy_when_dispatch_fails(
    app_module, app_client, db_pool_factory, monkeypatch
):
    strategy = {
        "industry_id": 12,
        "industry_name": "Logistics",
        "moltset_category": "Transportation",
        "bottleneck_profile": "Manual dispatching",
        "pain_tokens": ["delay"],
        "target_region": "Texas",
    }
    pool, _, cursors = db_pool_factory(fetchone_sequence=[(49,), strategy])
    app_module.app.state.db_pool = pool
    monkeypatch.setenv("TELEGRAM_CHAT_ID", TEST_CHAT_ID)
    monkeypatch.setattr(
        "services.mesh_client.trigger_downstream_mesh_with_payload",
        AsyncMock(return_value={"nova": False}),
    )
    monkeypatch.setattr(
        "core.engine.load_vendor_isolation_matrix", AsyncMock(return_value={})
    )
    monkeypatch.setattr(
        "telegram_orchestrator.emit_executive_notification", MagicMock()
    )

    response = await app_client.post(
        "/api/v1/telegram/webhook",
        headers=webhook_headers(),
        json=telegram_update("/execute nova", update_id=49),
    )

    assert response.status_code == 200
    executed_sql = " ".join(query for query, _ in cursors[0].executed)
    assert "SET last_targeted_at = NOW()" not in executed_sql


async def test_schedule_command_rejects_unschedulable_logging_agent(
    app_module, app_client, db_pool_factory, monkeypatch
):
    pool, _, cursors = db_pool_factory(fetchone_sequence=[(50,)])
    app_module.app.state.db_pool = pool
    monkeypatch.setenv("TELEGRAM_CHAT_ID", TEST_CHAT_ID)
    notify = MagicMock()
    monkeypatch.setattr("telegram_orchestrator.emit_executive_notification", notify)

    response = await app_client.post(
        "/api/v1/telegram/webhook",
        headers=webhook_headers(),
        json=telegram_update("/enable chiron", update_id=50),
    )

    assert response.status_code == 200
    assert notify.call_args.args[0].headline == "Command Rejected"
    executed_sql = " ".join(query for query, _ in cursors[0].executed)
    assert "INSERT INTO helix_agent_schedule" not in executed_sql


def test_worker_status_handles_running_cancelled_and_crashed_tasks():
    from telegram_orchestrator import _worker_status

    running = MagicMock()
    running.cancelled.return_value = False
    running.done.return_value = False

    cancelled = MagicMock()
    cancelled.cancelled.return_value = True

    crashed = MagicMock()
    crashed.cancelled.return_value = False
    crashed.done.return_value = True
    crashed.exception.return_value = RuntimeError("worker failed")

    assert _worker_status(running) == "running"
    assert _worker_status(cancelled) == "cancelled"
    assert _worker_status(crashed) == "crashed: worker failed"
