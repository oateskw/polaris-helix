import datetime
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

import pytest

from services.strategy import (
    apply_proposal,
    compute_scorecard,
    generate_proposals,
    review_is_due,
)
from tests.test_telegram_orchestrator import (
    TEST_CHAT_ID,
    telegram_update,
    webhook_headers,
)

pytestmark = pytest.mark.anyio
ET = ZoneInfo("America/New_York")


def row(name, **overrides):
    base = {
        "industry_name": name,
        "priority_score": 20,
        "is_active": True,
        "leads": 100,
        "replies": 0,
        "meetings": 0,
        "deals": 0,
        "revenue": 0.0,
        "bounces": 0,
        "opt_outs": 0,
        "blueprints": 0,
        "points": 0,
        "yield_per_100": 0.0,
        "negative_rate": 0.0,
    }
    base.update(overrides)
    return base


def kinds(proposals):
    return {
        p["payload"]["industry_name"]: (p["kind"], p["payload"].get("delta"))
        for p in proposals
    }


def test_review_due_from_monday_7am_once_per_week():
    monday_6am = datetime.datetime(2026, 10, 26, 6, 0, tzinfo=ET)
    monday_8am = datetime.datetime(2026, 10, 26, 8, 0, tzinfo=ET)
    friday = datetime.datetime(2026, 10, 30, 12, 0, tzinfo=ET)
    last_week = datetime.datetime(2026, 10, 19, 7, 5, tzinfo=ET)
    this_week = datetime.datetime(2026, 10, 26, 7, 5, tzinfo=ET)

    assert not review_is_due(monday_6am, last_week)
    assert review_is_due(monday_8am, last_week)
    assert review_is_due(monday_8am, None)
    assert not review_is_due(monday_8am, this_week)
    # Catch-up: the host slept through Monday and woke on Friday.
    assert review_is_due(friday, last_week)
    assert not review_is_due(friday, this_week)


def test_small_samples_produce_no_proposals():
    rows = [row("A", leads=10), row("B", leads=29, bounces=20, negative_rate=0.7)]
    assert generate_proposals(rows, min_lead_sample=30) == []


def test_high_negative_rate_cuts_priority():
    rows = [row("A", bounces=8, negative_rate=0.08, points=15, replies=1)]
    result = kinds(
        generate_proposals(rows, min_lead_sample=30, negative_rate_limit=0.05)
    )
    assert result == {"A": ("adjust_priority", -10)}


def test_no_results_cuts_then_pauses_with_bigger_sample():
    cut = generate_proposals([row("A", leads=50)], min_lead_sample=30)
    assert kinds(cut) == {"A": ("adjust_priority", -5)}

    pause = generate_proposals([row("A", leads=95)], min_lead_sample=30)
    assert kinds(pause) == {"A": ("pause_industry", None)}

    floor = generate_proposals(
        [row("A", leads=50, priority_score=1)], min_lead_sample=30
    )
    assert floor == []


def test_top_performer_above_median_is_boosted():
    rows = [
        row("Low", points=15, replies=1, yield_per_100=1.0),
        row("Mid", points=40, meetings=1, yield_per_100=2.0),
        row("High", points=100, deals=1, yield_per_100=9.0),
    ]
    result = kinds(generate_proposals(rows, min_lead_sample=30))
    assert result == {"High": ("adjust_priority", 10)}


def test_single_eligible_industry_is_not_boosted():
    rows = [row("Only", points=100, deals=1, yield_per_100=9.0)]
    assert generate_proposals(rows, min_lead_sample=30) == []


def test_paused_industry_with_results_is_proposed_for_resume():
    rows = [row("Paused", is_active=False, points=40, meetings=1, leads=5)]
    result = kinds(generate_proposals(rows, min_lead_sample=30))
    assert result == {"Paused": ("resume_industry", None)}


def test_paused_industry_without_results_is_left_alone():
    assert (
        generate_proposals([row("Paused", is_active=False)], min_lead_sample=30) == []
    )


async def test_compute_scorecard_merges_events_leads_and_scheduler(db_pool_factory):
    pool, _, _ = db_pool_factory(
        fetchall_sequence=[
            [
                {"industry_name": "Retail", "stage": "reply", "n": 3, "revenue": 0},
                {
                    "industry_name": "Retail",
                    "stage": "deal_won",
                    "n": 1,
                    "revenue": 2500,
                },
                {"industry_name": "Retail", "stage": "bounced", "n": 2, "revenue": 0},
                {"industry_name": "Gone", "stage": "reply", "n": 9, "revenue": 0},
            ],
            [{"industry": "Retail", "n": 50}],
            [
                {"industry_name": "Retail", "priority_score": 12, "is_active": True},
                {"industry_name": "Energy", "priority_score": 3, "is_active": False},
            ],
        ]
    )

    rows = await compute_scorecard(pool, 28)
    retail = next(r for r in rows if r["industry_name"] == "Retail")

    assert [r["industry_name"] for r in rows] == ["Retail", "Energy"]
    assert retail["leads"] == 50
    assert retail["replies"] == 3
    assert retail["deals"] == 1
    assert retail["revenue"] == 2500.0
    assert retail["points"] == 15 * 3 + 100
    assert retail["yield_per_100"] == round(145 / 50 * 100, 1)
    assert retail["negative_rate"] == 0.04


async def test_apply_adjust_priority_marks_applied(db_pool_factory):
    pool, connection, cursors = db_pool_factory(
        fetchone_sequence=[
            {
                "id": 5,
                "kind": "adjust_priority",
                "payload": {"industry_name": "Retail", "delta": -10},
                "status": "pending",
            },
            {"industry_name": "Retail", "priority_score": 10, "is_active": True},
        ]
    )

    result = await apply_proposal(pool, 5)

    assert result["ok"] is True
    sql = " ".join(q for q, _ in cursors[0].executed)
    assert "GREATEST(priority_score + %s, 1)" in sql
    assert "status = 'applied'" in sql
    assert connection.commit_calls == 1


async def test_apply_refuses_non_pending_and_missing(db_pool_factory):
    pool, connection, cursors = db_pool_factory(
        fetchone_sequence=[
            {
                "id": 5,
                "kind": "pause_industry",
                "payload": {"industry_name": "X"},
                "status": "applied",
            },
            None,
        ]
    )

    already = await apply_proposal(pool, 5)
    missing = await apply_proposal(pool, 6)

    assert already == {"ok": False, "reason": "already_applied"}
    assert missing == {"ok": False, "reason": "not_found"}
    assert connection.commit_calls == 0
    assert not any(
        "UPDATE helix_brain_scheduler" in q for q, _ in cursors[0].executed
    )


async def test_approve_command_applies_and_reports(
    app_module, app_client, db_pool_factory, monkeypatch
):
    pool, _, _ = db_pool_factory(fetchone_sequence=[(1,)])
    app_module.app.state.db_pool = pool
    monkeypatch.setenv("TELEGRAM_CHAT_ID", TEST_CHAT_ID)
    notify = MagicMock()
    monkeypatch.setattr("telegram_orchestrator.emit_executive_notification", notify)

    async def fake_apply(db_pool, proposal_id):
        assert proposal_id == 12
        return {
            "ok": True,
            "kind": "pause_industry",
            "result": {
                "industry_name": "Retail",
                "priority_score": 4,
                "is_active": False,
            },
        }

    async def fake_log(*args, **kwargs):
        return None

    monkeypatch.setattr("telegram_orchestrator.apply_proposal", fake_apply)
    monkeypatch.setattr(
        "telegram_orchestrator.write_helix_command_center_log", fake_log
    )

    response = await app_client.post(
        "/api/v1/telegram/webhook",
        headers=webhook_headers(),
        json=telegram_update("/approve #12", update_id=950),
    )

    assert response.json()["status"] == "processed"
    event = notify.call_args.args[0]
    assert event.headline == "Proposal Applied"
    assert "paused" in event.impact


async def test_approve_rejects_non_numeric_id(
    app_module, app_client, db_pool_factory, monkeypatch
):
    pool, _, _ = db_pool_factory(fetchone_sequence=[(1,)])
    app_module.app.state.db_pool = pool
    monkeypatch.setenv("TELEGRAM_CHAT_ID", TEST_CHAT_ID)
    notify = MagicMock()
    monkeypatch.setattr("telegram_orchestrator.emit_executive_notification", notify)

    await app_client.post(
        "/api/v1/telegram/webhook",
        headers=webhook_headers(),
        json=telegram_update("/approve 1; drop table", update_id=951),
    )

    assert notify.call_args.args[0].headline == "Command Rejected"


def test_priority_first_ordering_flag(monkeypatch):
    import importlib

    import core.engine as engine

    try:
        monkeypatch.setenv("HELIX_PRIORITY_FIRST_ORDERING", "true")
        importlib.reload(engine)
        assert engine.SCHEDULER_ORDER_BY.startswith("priority_score DESC")

        monkeypatch.delenv("HELIX_PRIORITY_FIRST_ORDERING")
        importlib.reload(engine)
        assert engine.SCHEDULER_ORDER_BY.startswith("last_targeted_at ASC")
    finally:
        monkeypatch.delenv("HELIX_PRIORITY_FIRST_ORDERING", raising=False)
        importlib.reload(engine)


async def test_review_loop_runs_once_when_due_and_stays_quiet_otherwise(monkeypatch):
    import asyncio
    from types import SimpleNamespace

    import services.strategy as strategy

    app = SimpleNamespace(state=SimpleNamespace(db_pool=object()))
    review = MagicMock()
    marker_writes = []
    notify = MagicMock()

    async def fake_review(db_pool):
        review()
        return {"scorecard": [], "created": [], "pending": []}

    async def fake_log(db_pool, level, message, action, *rest):
        marker_writes.append(action)

    async def fake_last(db_pool):
        return None

    async def stop_after_first_tick(_seconds):
        raise asyncio.CancelledError

    monkeypatch.setattr(strategy, "run_strategy_review", fake_review)
    monkeypatch.setattr(strategy, "write_helix_command_center_log", fake_log)
    monkeypatch.setattr(strategy, "get_last_review_at", fake_last)
    monkeypatch.setattr(strategy, "emit_executive_notification", notify)
    monkeypatch.setattr(strategy.asyncio, "sleep", stop_after_first_tick)

    monkeypatch.setattr(strategy, "review_is_due", lambda now, last: True)
    with pytest.raises(asyncio.CancelledError):
        await strategy.strategy_review_loop(app)
    assert review.call_count == 1
    assert marker_writes == [strategy.REVIEW_MARKER]
    assert notify.call_args.args[0].event_type == "strategy_review"

    monkeypatch.setattr(strategy, "review_is_due", lambda now, last: False)
    with pytest.raises(asyncio.CancelledError):
        await strategy.strategy_review_loop(app)
    assert review.call_count == 1

    monkeypatch.setenv("HELIX_STRATEGY_REVIEW_ENABLED", "false")
    monkeypatch.setattr(strategy, "review_is_due", lambda now, last: True)
    with pytest.raises(asyncio.CancelledError):
        await strategy.strategy_review_loop(app)
    assert review.call_count == 1
