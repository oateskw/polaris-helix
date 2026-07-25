from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from services.mesh_client import (
    build_beacon_execution_payload,
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
        "run-42",
    )

    assert payload == {
        "run_id": "run-42",
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
        "run-42",
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


def test_build_beacon_execution_payload_propagates_nexus_strategy():
    payload = build_beacon_execution_payload(
        {
            "industry_id": 42,
            "industry_name": "Logistics",
            "target_region": "Texas",
            "bottleneck_profile": "Manual dispatching",
            "campaign_strategy": "LIVE_AUDIT",
            "nexus_directive": "Calculate the hidden payroll drain.",
        },
        ["Use concrete payroll figures."],
    )

    assert payload == {
        "industry_id": 42,
        "industry_name": "Logistics",
        "target_region": "Texas",
        "bottleneck_profile": "Manual dispatching",
        "campaign_strategy": "LIVE_AUDIT",
        "nexus_directive": "Calculate the hidden payroll drain.",
        "learned_guidelines": ["Use concrete payroll figures."],
    }


@pytest.mark.parametrize("campaign_strategy", [None, "UNSUPPORTED_STRATEGY"])
def test_build_beacon_execution_payload_rejects_invalid_strategy(campaign_strategy):
    with pytest.raises(ValueError, match="selected by Nexus"):
        build_beacon_execution_payload(
            {
                "industry_id": 42,
                "industry_name": "Logistics",
                "target_region": "Texas",
                "bottleneck_profile": "Manual dispatching",
                "campaign_strategy": campaign_strategy,
            },
            [],
        )


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


@pytest.mark.anyio
async def test_beacon_mesh_posts_nexus_campaign_strategy(db_pool_factory, monkeypatch):
    pool, _, _ = db_pool_factory()
    post_proxy = AsyncMock(return_value=True)
    monkeypatch.setattr(
        "services.mesh_client.query_learned_guidelines",
        AsyncMock(return_value=["Use concrete payroll figures."]),
    )
    monkeypatch.setattr("services.mesh_client.async_http_post_proxy", post_proxy)
    monkeypatch.setattr(
        "services.telemetry.write_helix_command_center_log", AsyncMock()
    )
    monkeypatch.setattr("services.telemetry.send_telegram_broadcast", MagicMock())

    result = await trigger_downstream_mesh_with_payload(
        pool,
        SimpleNamespace(),
        {
            "industry_id": 42,
            "industry_name": "Logistics",
            "target_region": "Texas",
            "bottleneck_profile": "Manual dispatching",
            "campaign_strategy": "OBJECTION_KILLER",
            "nexus_directive": "Address reliability concerns directly.",
        },
        ["beacon"],
        {},
    )

    assert post_proxy.await_args.args[2]["campaign_strategy"] == "OBJECTION_KILLER"
    assert result == {"beacon": True}


@pytest.mark.anyio
async def test_nexus_mesh_normalizes_quoted_helix_auth_token(
    db_pool_factory, monkeypatch
):
    pool, _, _ = db_pool_factory()
    post_proxy = AsyncMock(return_value=True)
    monkeypatch.setenv("HELIX_AUTH_TOKEN", ' "shared-token" ')
    monkeypatch.setattr(
        "services.mesh_client.query_learned_guidelines", AsyncMock(return_value=[])
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
            "target_region": "Texas",
            "bottleneck_profile": "Manual dispatching",
            "pain_tokens": ["delay"],
        },
        ["nexus"],
        {},
    )

    assert post_proxy.await_args.args[1]["X-Nexus-Auth-Token"] == "shared-token"
    assert result == {"nexus": True}


@pytest.mark.anyio
async def test_nexus_mesh_skips_dispatch_without_helix_auth_token(
    db_pool_factory, monkeypatch
):
    pool, _, _ = db_pool_factory()
    post_proxy = AsyncMock()
    command_log = AsyncMock()
    monkeypatch.delenv("HELIX_AUTH_TOKEN", raising=False)
    monkeypatch.setattr("services.mesh_client.async_http_post_proxy", post_proxy)
    monkeypatch.setattr(
        "services.telemetry.write_helix_command_center_log", command_log
    )
    monkeypatch.setattr("services.telemetry.emit_executive_notification", MagicMock())

    result = await trigger_downstream_mesh_with_payload(
        pool,
        SimpleNamespace(),
        {"industry_name": "Logistics"},
        ["nexus"],
        {},
    )

    post_proxy.assert_not_awaited()
    assert command_log.await_args.args[3] == "SKIP_NEXUS_AUTH_UNAVAILABLE"
    assert result == {"nexus": False}
