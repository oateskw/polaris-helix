import pytest

from core.engine import parse_moltset_search_capacity


def test_parse_moltset_search_capacity_reads_five_hour_remaining():
    payload = {
        "status": "ok",
        "results": {
            "status": "active",
            "fair_use": {"search": {"requests": {"5h": {"remaining": 37}}}},
        },
    }

    assert parse_moltset_search_capacity(payload) == 37.0


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"status": "ok", "results": {"status": "inactive"}},
        {
            "status": "error",
            "results": {
                "status": "active",
                "fair_use": {"search": {"requests": {"5h": {"remaining": 10}}}},
            },
        },
        {
            "status": "ok",
            "results": {
                "status": "active",
                "fair_use": {"search": {"requests": {"5h": {"remaining": "10"}}}},
            },
        },
    ],
)
def test_parse_moltset_search_capacity_fails_closed(payload):
    assert parse_moltset_search_capacity(payload) is None


def test_parse_moltset_search_capacity_preserves_zero():
    payload = {
        "status": "ok",
        "results": {
            "status": "active",
            "fair_use": {"search": {"requests": {"5h": {"remaining": 0}}}},
        },
    }

    assert parse_moltset_search_capacity(payload) == 0.0
