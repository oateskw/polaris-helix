from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from core.engine import dispatch_nova_if_due


@pytest.mark.anyio
@pytest.mark.parametrize("accepted", [True, False])
async def test_nova_completion_log_requires_accepted_ignition(
    accepted, db_pool_factory, monkeypatch
):
    pool, _, cursors = db_pool_factory(cursor_count=3)
    cursors[0].fetchone_values.append((0,))
    cursors[1].fetchall_values.append(
        [{"vendor_name": "MoltSet", "is_circuit_broken": False}]
    )
    cursors[2].fetchone_values.append(
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
    monkeypatch.setattr("core.engine.write_helix_command_center_log", command_log)

    await dispatch_nova_if_due(app)

    mesh_dispatch.assert_awaited_once()
    if accepted:
        command_log.assert_awaited_once()
        assert command_log.await_args.args[3] == "DISPATCH_NOVA_30_MINUTE"
    else:
        command_log.assert_not_awaited()


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

    await dispatch_nova_if_due(app)

    assert "DISPATCH_NOVA_REJECTED" in cursors[0].executed[0][0]
    mesh_dispatch.assert_not_awaited()
