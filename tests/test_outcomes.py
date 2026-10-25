from unittest.mock import AsyncMock, MagicMock

import pytest

from services.outcomes import record_outcome_event, stage_from_status
from telegram_orchestrator import _parse_outcome_argument
from tests.test_main_api_endpoints import TEST_AUTH_TOKEN
from tests.test_telegram_orchestrator import (
    TEST_CHAT_ID,
    telegram_update,
    webhook_headers,
)

pytestmark = pytest.mark.anyio


@pytest.mark.parametrize(
    "status,expected",
    [
        ("replied", "reply"),
        ("positive reply", "positive_reply"),
        ("meeting_booked", "meeting_booked"),
        ("Booked", "meeting_booked"),
        ("deal won", "deal_won"),
        ("converted", "deal_won"),
        ("hard bounce", "bounced"),
        ("opt-out", "opt_out"),
        ("unsubscribed", "opt_out"),
        ("ready_to_send", None),
        ("", None),
        (None, None),
    ],
)
def test_stage_from_status(status, expected):
    assert stage_from_status(status) == expected


async def test_record_outcome_event_snapshots_region_and_strategy(db_pool_factory):
    pool, connection, cursors = db_pool_factory(
        fetchone_sequence=[
            {"industry": "Retail"},
            {"target_region": "Texas", "campaign_strategy": "LIVE_AUDIT"},
            {"id": 7},
        ]
    )

    result = await record_outcome_event(
        pool, lead_email=" Owner@Shop.com ", stage="meeting_booked", source="telegram"
    )

    assert result == {"recorded": True, "duplicate": False, "industry_name": "Retail"}
    insert_sql, params = cursors[0].executed[-1]
    assert "ON CONFLICT (lead_email, stage, source_event_id) DO NOTHING" in insert_sql
    assert params[:5] == (
        "owner@shop.com",
        "Retail",
        "Texas",
        "LIVE_AUDIT",
        "meeting_booked",
    )
    assert connection.commit_calls == 1


async def test_record_outcome_event_reports_duplicate(db_pool_factory):
    pool, _, _ = db_pool_factory(
        fetchone_sequence=[
            {"industry": "Retail"},
            {"target_region": "Texas", "campaign_strategy": None},
            None,
        ]
    )
    result = await record_outcome_event(
        pool, lead_email="a@b.com", stage="reply", source="vanguard_status"
    )
    assert result["recorded"] is False
    assert result["duplicate"] is True


@pytest.mark.parametrize(
    "kwargs",
    [
        {"lead_email": "", "stage": "reply", "source": "telegram"},
        {"lead_email": "a@b.com", "stage": "nonsense", "source": "telegram"},
        {"lead_email": "a@b.com", "stage": "reply", "source": "nonsense"},
    ],
)
async def test_record_outcome_event_rejects_bad_input(db_pool_factory, kwargs):
    pool, _, cursors = db_pool_factory()
    result = await record_outcome_event(pool, **kwargs)
    assert result["recorded"] is False
    assert "error" in result
    assert cursors[0].executed == []


async def test_record_outcome_event_never_raises_on_db_failure(db_pool_factory):
    pool, _, _ = db_pool_factory(fail_execute=RuntimeError("db down"))
    result = await record_outcome_event(
        pool, lead_email="a@b.com", stage="reply", source="telegram"
    )
    assert result["recorded"] is False
    assert "db down" in result["error"]


@pytest.mark.parametrize(
    "command,argument,ok",
    [
        ("/booked", "a@b.com", True),
        ("/reply", "a@b.com", True),
        ("/won", "a@b.com 2500", True),
        ("/won", "a@b.com $2,500.50", True),
        ("/won", "a@b.com", False),
        ("/won", "a@b.com abc", False),
        ("/won", "a@b.com -5", False),
        ("/won", "a@b.com 0", False),
        ("/won", "a@b.com 999999999999", False),
        ("/won", "a@b.com nan", False),
        ("/booked", "", False),
        ("/booked", "not-an-email", False),
        ("/booked", "a@b.com extra", False),
        ("/booked", "a<script>@b.com", False),
    ],
)
def test_parse_outcome_argument(command, argument, ok):
    email, _, error = _parse_outcome_argument(command, argument)
    assert (error is None) is ok
    assert (email is not None) is ok


async def test_won_command_records_amount_and_applies_boost(
    app_module, app_client, db_pool_factory, monkeypatch
):
    pool, _, _ = db_pool_factory(fetchone_sequence=[(1,)])
    app_module.app.state.db_pool = pool
    monkeypatch.setenv("TELEGRAM_CHAT_ID", TEST_CHAT_ID)
    record = AsyncMock(
        return_value={"recorded": True, "duplicate": False, "industry_name": "Retail"}
    )
    boost = AsyncMock()
    notify = MagicMock()
    monkeypatch.setattr("telegram_orchestrator.record_outcome_event", record)
    monkeypatch.setattr("telegram_orchestrator.process_roi_attribution_boost", boost)
    monkeypatch.setattr("telegram_orchestrator.emit_executive_notification", notify)

    response = await app_client.post(
        "/api/v1/telegram/webhook",
        headers=webhook_headers(),
        json=telegram_update("/won Owner@Shop.com 2500", update_id=900),
    )

    assert response.json()["status"] == "processed"
    kwargs = record.await_args.kwargs
    assert kwargs["stage"] == "deal_won"
    assert kwargs["amount"] == 2500.0
    assert kwargs["source"] == "telegram"
    assert kwargs["source_event_id"] == "tg:900"
    boost.assert_awaited_once()
    assert boost.await_args.kwargs["record_ledger"] is False
    assert notify.call_args.args[0].headline == "Outcome Recorded"


async def test_invalid_outcome_command_writes_nothing(
    app_module, app_client, db_pool_factory, monkeypatch
):
    pool, _, _ = db_pool_factory(fetchone_sequence=[(1,)])
    app_module.app.state.db_pool = pool
    monkeypatch.setenv("TELEGRAM_CHAT_ID", TEST_CHAT_ID)
    record = AsyncMock()
    notify = MagicMock()
    monkeypatch.setattr("telegram_orchestrator.record_outcome_event", record)
    monkeypatch.setattr("telegram_orchestrator.emit_executive_notification", notify)

    await app_client.post(
        "/api/v1/telegram/webhook",
        headers=webhook_headers(),
        json=telegram_update("/won a@b.com lots", update_id=901),
    )

    record.assert_not_awaited()
    assert notify.call_args.args[0].headline == "Outcome Command Rejected"


async def test_outcomes_endpoint_requires_token_and_records(
    app_module, app_client, db_pool_factory, monkeypatch
):
    pool, _, _ = db_pool_factory()
    app_module.app.state.db_pool = pool
    record = AsyncMock(return_value={"recorded": True, "industry_name": "Retail"})
    boost = AsyncMock()
    monkeypatch.setattr(app_module, "record_outcome_event", record)
    monkeypatch.setattr(app_module, "process_roi_attribution_boost", boost)
    body = {"email": "a@b.com", "stage": "meeting_booked"}

    denied = await app_client.post("/api/v1/outcomes", json=body)
    assert denied.status_code == 401
    record.assert_not_awaited()

    ok = await app_client.post(
        "/api/v1/outcomes", json=body, headers={"X-Helix-Auth-Token": TEST_AUTH_TOKEN}
    )
    assert ok.status_code == 200
    assert ok.json()["status"] == "recorded"
    boost.assert_awaited_once()

    bad_stage = await app_client.post(
        "/api/v1/outcomes",
        json={"email": "a@b.com", "stage": "blueprint_submitted"},
        headers={"X-Helix-Auth-Token": TEST_AUTH_TOKEN},
    )
    assert bad_stage.status_code == 422
