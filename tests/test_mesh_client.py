from services.mesh_client import build_nova_ignition_payload


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
