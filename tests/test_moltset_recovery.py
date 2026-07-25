import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from core.engine import (
    check_and_heal_moltset_circuit,
    is_moltset_probe_due,
    parse_moltset_search_capacity,
)


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


def test_moltset_probe_waits_full_thirty_minutes_after_trip():
    now_utc = datetime.datetime(2026, 7, 25, 17, 0, tzinfo=datetime.timezone.utc)

    assert not is_moltset_probe_due(
        now_utc - datetime.timedelta(minutes=29, seconds=59), now_utc
    )
    assert is_moltset_probe_due(now_utc - datetime.timedelta(minutes=30), now_utc)


def test_moltset_probe_without_persisted_timestamp_fails_closed():
    assert not is_moltset_probe_due(None)


@pytest.mark.anyio
async def test_broken_moltset_circuit_does_not_probe_during_cooldown(
    db_pool_factory, monkeypatch
):
    recently_tripped = datetime.datetime.now(
        datetime.timezone.utc
    ) - datetime.timedelta(minutes=29)
    pool, _, cursors = db_pool_factory(
        fetchone_sequence=[
            {
                "is_circuit_broken": True,
                "credits_remaining": 0,
                "last_updated_at": recently_tripped,
            }
        ]
    )
    app = SimpleNamespace(state=SimpleNamespace(db_pool=pool))
    post = MagicMock()
    monkeypatch.setattr("core.engine.requests.post", post)

    await check_and_heal_moltset_circuit(app)

    post.assert_not_called()
    assert "moltset" in app.state.disabled_circuits
    assert "last_updated_at" in cursors[0].executed[0][0]


@pytest.mark.anyio
async def test_due_moltset_probe_claims_window_heals_and_logs(
    db_pool_factory, monkeypatch
):
    tripped_at = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(
        minutes=31
    )
    pool, _, cursors = db_pool_factory(
        fetchone_sequence=[
            {
                "is_circuit_broken": True,
                "credits_remaining": 0,
                "last_updated_at": tripped_at,
            }
        ],
        cursor_count=3,
    )
    cursors[1].fetchone_values.append(("MoltSet",))
    app = SimpleNamespace(state=SimpleNamespace(db_pool=pool))

    response = MagicMock(status_code=200)
    response.json.return_value = {
        "status": "ok",
        "results": {
            "status": "active",
            "fair_use": {"search": {"requests": {"5h": {"remaining": 2382}}}},
        },
    }
    post = MagicMock(return_value=response)
    command_log = AsyncMock()
    monkeypatch.setenv("MOLTSET_API_KEY", "test-key")
    monkeypatch.setattr("core.engine.requests.post", post)
    monkeypatch.setattr("core.engine.write_helix_command_center_log", command_log)
    monkeypatch.setattr("core.engine.emit_executive_notification", MagicMock())

    await check_and_heal_moltset_circuit(app)

    post.assert_called_once()
    assert "last_updated_at = NOW()" in cursors[1].executed[0][0]
    assert "is_circuit_broken = FALSE" in cursors[2].executed[0][0]
    assert "moltset" not in app.state.disabled_circuits
    assert [call.args[3] for call in command_log.await_args_list] == [
        "MOLTSET_RECOVERY_PROBE",
        "MOLTSET_CIRCUIT_HEALED",
    ]
