from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from core.engine import dispatch_nova_if_due


@pytest.mark.anyio
@pytest.mark.parametrize("accepted", [True, False])
async def test_nova_completion_log_requires_accepted_ignition(
    accepted, db_pool_factory, monkeypatch
):
    pool, _, cursors = db_pool_factory(cursor_count=2)
    cursors[0].fetchone_values.append((0,))
    cursors[1].fetchone_values.append(
        {
            "industry_id": 42,
            "industry_name": "Logistics",
            "moltset_category": "Transportation",
            "bottleneck_profile": "Manual dispatching",
            "pain_tokens": [],
            "target_region": "Texas",
            "campaign_strategy": "BEFORE_AFTER",
        }
    )
    app = SimpleNamespace(state=SimpleNamespace(db_pool=pool))
    mesh_dispatch = AsyncMock(return_value={"nova": accepted})
    command_log = AsyncMock()
    monkeypatch.setattr(
        "core.engine.trigger_downstream_mesh_with_payload", mesh_dispatch
    )
    monkeypatch.setattr(
        "core.engine.load_vendor_isolation_matrix",
        AsyncMock(return_value={"MoltSet": False}),
    )
    monkeypatch.setattr(
        "core.engine.fetch_moltset_records_1w_window",
        AsyncMock(
            return_value={
                "limit": 2500.0,
                "used": 1200.0,
                "remaining": 1300.0,
                "resets_at": "2026-08-06T13:54:59Z",
            }
        ),
    )
    monkeypatch.setattr(
        "core.engine.compute_daily_record_budget", lambda *args, **kwargs: (500, 50, 7)
    )
    monkeypatch.setattr(
        "core.engine.estimate_nova_records_per_people_query",
        AsyncMock(return_value=100),
    )
    monkeypatch.setattr(
        "core.engine.get_nova_consumed_records_today", AsyncMock(return_value=0)
    )
    monkeypatch.setattr("core.engine.write_helix_command_center_log", command_log)

    await dispatch_nova_if_due(app)

    mesh_dispatch.assert_awaited_once()
    if accepted:
        assert command_log.await_count >= 2
        assert any(
            call.args[3] == "NOVA_DAILY_BUDGET_DISPATCHED"
            for call in command_log.await_args_list
        )
    else:
        assert command_log.await_count >= 1


@pytest.mark.anyio
async def test_recent_rejected_ignition_starts_nova_backoff(
    db_pool_factory, monkeypatch
):
    pool, _, cursors = db_pool_factory(fetchone_sequence=[(1,)])
    app = SimpleNamespace(state=SimpleNamespace(db_pool=pool))
    mesh_dispatch = AsyncMock()
    monkeypatch.setattr(
        "core.engine.trigger_downstream_mesh_with_payload", mesh_dispatch
    )
    monkeypatch.setattr(
        "core.engine.load_vendor_isolation_matrix",
        AsyncMock(return_value={"MoltSet": False}),
    )
    monkeypatch.setattr(
        "core.engine.fetch_moltset_records_1w_window",
        AsyncMock(
            return_value={
                "limit": 2500.0,
                "used": 1200.0,
                "remaining": 1300.0,
                "resets_at": "2026-08-06T13:54:59Z",
            }
        ),
    )
    monkeypatch.setattr(
        "core.engine.compute_daily_record_budget", lambda *args, **kwargs: (500, 50, 7)
    )
    monkeypatch.setattr(
        "core.engine.estimate_nova_records_per_people_query",
        AsyncMock(return_value=100),
    )
    monkeypatch.setattr(
        "core.engine.get_nova_consumed_records_today", AsyncMock(return_value=0)
    )

    await dispatch_nova_if_due(app)

    assert "DISPATCH_NOVA_REJECTED" in cursors[0].executed[0][0]
    mesh_dispatch.assert_not_awaited()


@pytest.mark.anyio
async def test_daily_budget_deferral_emits_explicit_leveling_telegram_notice(
    db_pool_factory, monkeypatch
):
    pool, _, cursors = db_pool_factory(fetchone_sequence=[(0,)])
    app = SimpleNamespace(state=SimpleNamespace(db_pool=pool))

    monkeypatch.setattr(
        "core.engine.load_vendor_isolation_matrix",
        AsyncMock(return_value={"MoltSet": False}),
    )
    monkeypatch.setattr(
        "core.engine.fetch_moltset_records_1w_window",
        AsyncMock(
            return_value={
                "limit": 2500.0,
                "used": 1500.0,
                "remaining": 1000.0,
                "resets_at": "2026-08-06T13:54:59Z",
            }
        ),
    )
    monkeypatch.setattr(
        "core.engine.compute_daily_record_budget", lambda *args, **kwargs: (100, 10, 7)
    )
    monkeypatch.setattr(
        "core.engine.estimate_nova_records_per_people_query",
        AsyncMock(return_value=150),
    )
    monkeypatch.setattr(
        "core.engine.get_nova_consumed_records_today", AsyncMock(return_value=100)
    )
    command_log = AsyncMock()
    telegram = MagicMock()
    mesh_dispatch = AsyncMock()
    monkeypatch.setattr("core.engine.write_helix_command_center_log", command_log)
    monkeypatch.setattr("core.engine.send_telegram_broadcast", telegram)
    monkeypatch.setattr(
        "core.engine.trigger_downstream_mesh_with_payload", mesh_dispatch
    )

    await dispatch_nova_if_due(app)

    assert command_log.await_count >= 2
    assert any(
        call.args[3] == "NOVA_DAILY_BUDGET_DEFERRED"
        for call in command_log.await_args_list
    )
    telegram.assert_called_once()
    assert "held back to level out the weekly allotment" in telegram.call_args.args[0]
    mesh_dispatch.assert_not_awaited()
