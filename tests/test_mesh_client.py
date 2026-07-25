from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from services.mesh_client import (
    build_nova_ignition_payload,
    trigger_downstream_mesh_with_payload,
)


def test_build_nova_ignition_payload_matches_nova_schema():
    payload = build_nova_ignition_payload(
        {
            "industry_id": 42,
            "industry_name": "Logistics",
            "moltset_category": "Transportation",
            "bottleneck_profile": "Manual dispatching",
            "target_region": "Texas",
        },
        ["PREFER: regional carriers"],
    )

    assert payload == {
        "target_region": "Texas",
        "target_company_limit": 25,
        "learned_guidelines": ["PREFER: regional carriers"],
        "industries": [
            {
                "industry_id": 42,
                "industry_name": "Logistics",
                "moltset_category": "Transportation",
                "bottleneck_profile": "Manual dispatching",
            }
        ],
    }
    assert "daily_quota_limit" not in payload


def test_build_nova_ignition_payload_defaults_nullable_strategy_text():
    payload = build_nova_ignition_payload(
        {
            "industry_id": 42,
            "industry_name": None,
            "moltset_category": None,
            "bottleneck_profile": None,
            "target_region": None,
        },
        [],
    )

    assert payload["target_region"] == "United States"
    assert payload["industries"] == [
        {
            "industry_id": 42,
            "industry_name": "General Operations",
            "moltset_category": "Professional Services",
            "bottleneck_profile": "Operational inefficiency",
        }
    ]


@pytest.mark.anyio
@pytest.mark.parametrize("accepted", [True, False])
async def test_nova_mesh_propagates_ignition_acceptance(
    accepted, db_pool_factory, monkeypatch
):
    pool, _, _ = db_pool_factory()
    post_proxy = AsyncMock(return_value=accepted)
    monkeypatch.setattr(
        "services.mesh_client.query_learned_guidelines",
        AsyncMock(return_value=[]),
    )
    monkeypatch.setattr("services.mesh_client.async_http_post_proxy", post_proxy)
    monkeypatch.setattr(
        "services.telemetry.write_helix_command_center_log", AsyncMock()
    )
    monkeypatch.setattr("services.telemetry.emit_executive_notification", MagicMock())

    result = await trigger_downstream_mesh_with_payload(
        pool,
        SimpleNamespace(),
        {
            "industry_id": 42,
            "industry_name": "Logistics",
            "moltset_category": "Transportation",
            "bottleneck_profile": "Manual dispatching",
            "pain_tokens": [],
            "target_region": "Texas",
        },
        ["nova"],
        {},
    )

    post_proxy.assert_awaited_once()
    assert result == {"nova": accepted}
