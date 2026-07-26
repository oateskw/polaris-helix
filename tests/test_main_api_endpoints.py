import datetime
from unittest.mock import AsyncMock
import pytest

TEST_AUTH_TOKEN = "helix_super_secure_test_token"
TEST_VANGUARD_KEY = "test_vanguard_key"


class HybridMockRow(dict):
    def __getitem__(self, key):
        if isinstance(key, int):
            return list(self.values())[key]
        return super().__getitem__(key)


pytestmark = pytest.mark.anyio


async def test_startup_schema_provisions_telegram_update_deduplication(
    app_module, db_pool_factory
):
    pool, connection, cursors = db_pool_factory()

    await app_module.ensure_observability_schema(pool)

    startup_sql, params = cursors[0].executed[0]
    assert params is None
    assert "CREATE TABLE IF NOT EXISTS telegram_processed_updates" in startup_sql
    assert "CREATE INDEX IF NOT EXISTS idx_tpu_processed_at" in startup_sql
    assert connection.commit_calls == 1


async def test_root_heartbeat_get_and_head(app_client):
    get_res = await app_client.get("/")
    head_res = await app_client.head("/")

    assert get_res.status_code == 200
    assert get_res.json() == {"status": "healthy", "service": "polaris-helix"}
    assert head_res.status_code == 200


async def test_health_reports_background_worker_states(
    app_module, app_client, monkeypatch
):
    class RunningTask:
        def done(self):
            return False

    monkeypatch.setattr(
        app_module.app.state, "auditor_task", RunningTask(), raising=False
    )
    monkeypatch.setattr(
        app_module.app.state, "awareness_task", RunningTask(), raising=False
    )
    monkeypatch.setattr(
        app_module.app.state, "credit_sync_task", RunningTask(), raising=False
    )
    monkeypatch.setattr(
        app_module.app.state, "optimizer_task", RunningTask(), raising=False
    )

    response = await app_client.get("/api/v1/health")

    assert response.status_code == 200
    assert response.json()["status"] == "healthy"
    assert response.json()["workers"] == {
        "performance_auditor": "running",
        "awareness_dispatcher": "running",
        "vendor_credit_sync": "running",
        "prompt_optimizer": "running",
    }


async def test_ingest_leads_unauthorized(app_module, app_client, db_pool_factory):
    pool, _, _ = db_pool_factory()
    app_module.app.state.db_pool = pool

    payload = [{"email": "unauth@example.com", "industry": "Finance and Banking"}]
    response = await app_client.post(
        "/api/v1/ingest",
        json=payload,
        headers={"X-Helix-Auth-Token": "wrong"},
    )

    assert response.status_code == 401


async def test_ingest_leads_success_escalates_by_industry(
    app_module, app_client, db_pool_factory, monkeypatch
):
    pool, connection, cursors = db_pool_factory()
    app_module.app.state.db_pool = pool

    escalate = AsyncMock()
    monkeypatch.setattr(app_module, "evaluate_lead_density_and_escalate", escalate)

    payload = [
        {"email": "a@example.com", "industry": "Finance and Banking"},
        {"email": "b@example.com", "industry": "Finance and Banking"},
        {"email": "c@example.com", "industry": "Education"},
        {"email": "d@example.com", "industry": None},
    ]

    response = await app_client.post(
        "/api/v1/ingest",
        json=payload,
        headers={"X-Helix-Auth-Token": TEST_AUTH_TOKEN},
    )

    assert response.status_code == 200
    assert response.json()["processed"] == 4
    assert connection.commit_calls == 1
    assert len(cursors[0].executed) == 4
    assert escalate.await_count == 2


async def test_inbound_blueprints_unassigned_skips_escalation(
    app_module, app_client, db_pool_factory, monkeypatch
):
    pool, connection, _ = db_pool_factory()
    app_module.app.state.db_pool = pool

    escalate = AsyncMock()
    monkeypatch.setattr(app_module, "evaluate_lead_density_and_escalate", escalate)

    response = await app_client.post(
        "/api/v1/inbound-blueprints",
        headers={"X-Helix-Auth-Token": TEST_AUTH_TOKEN},
        json={
            "headcount": 10,
            "weekly_hours_wasted": 2,
            "annual_wasted_hours": 100,
            "annual_financial_bleed": 1234.5,
            "friction_points": ["handoff lag"],
            "name": "A",
            "company_name": "B",
            "email": "owner@company.com",
            "industry_name": "Unassigned",
        },
    )

    assert response.status_code == 200
    assert connection.commit_calls == 1
    escalate.assert_not_awaited()


async def test_update_lead_status_auth_and_success(
    app_module, app_client, db_pool_factory, monkeypatch
):
    pool, connection, _ = db_pool_factory()
    app_module.app.state.db_pool = pool

    task_spy = []

    def capture_task(coro):
        task_spy.append(coro)
        coro.close()

        class _Task:
            pass

        return _Task()

    monkeypatch.setattr(app_module.asyncio, "create_task", capture_task)

    bad = await app_client.post(
        "/api/v1/leads/status-update?email=t@example.com",
        json={"status": "meeting_booked"},
        headers={"X-Vanguard-Key": "bad"},
    )
    assert bad.status_code == 401

    good = await app_client.post(
        "/api/v1/leads/status-update?email=t@example.com",
        json={"status": "meeting_booked"},
        headers={"X-Vanguard-Key": TEST_VANGUARD_KEY},
    )
    assert good.status_code == 200
    assert good.json()["status"] == "attribution_processed"
    assert connection.commit_calls == 1
    assert len(task_spy) == 1


async def test_ingest_log_missing_inserted_id_returns_500(
    app_module, app_client, db_pool_factory
):
    pool, _, _ = db_pool_factory(fetchone_sequence=[None])
    app_module.app.state.db_pool = pool

    response = await app_client.post(
        "/api/v1/logs",
        json={"agent_name": "nova", "log_level": "INFO", "message": "m"},
        headers={"X-Helix-Auth-Token": TEST_AUTH_TOKEN},
    )

    assert response.status_code == 500
    assert "No ID returned" in response.json()["detail"]


async def test_ingest_log_accepts_debug_level(
    app_module, app_client, db_pool_factory, monkeypatch
):
    pool, _, _ = db_pool_factory(fetchone_sequence=[(91,)])
    app_module.app.state.db_pool = pool
    monkeypatch.setattr(
        app_module.asyncio, "create_task", lambda coroutine: coroutine.close()
    )

    response = await app_client.post(
        "/api/v1/logs",
        headers={"X-Helix-Auth-Token": TEST_AUTH_TOKEN},
        json={"agent_name": "nova", "log_level": "DEBUG", "message": "detail"},
    )

    assert response.status_code == 200
    assert response.json()["logged_id"] == 91


async def test_ingest_log_preserves_extra_metadata_keys(
    app_module, app_client, db_pool_factory, monkeypatch
):
    pool, _, cursors = db_pool_factory(fetchone_sequence=[(93,)])
    app_module.app.state.db_pool = pool
    monkeypatch.setattr(
        app_module.asyncio, "create_task", lambda coroutine: coroutine.close()
    )

    response = await app_client.post(
        "/api/v1/logs",
        headers={"X-Helix-Auth-Token": TEST_AUTH_TOKEN},
        json={
            "agent_name": "pulsar",
            "log_level": "INFO",
            "message": "Webhook batch completed.",
            "metadata": {
                "run_id": "pulsar-123",
                "lifecycle_event": "RUN_COMPLETED",
                "stage": "batch_complete",
                "dms_sent": 1,
                "dms_skipped": 0,
            },
        },
    )

    assert response.status_code == 200
    assert response.json()["logged_id"] == 93

    _, params = cursors[0].executed[0]
    inserted_metadata = params[3].obj
    assert inserted_metadata["stage"] == "batch_complete"
    assert inserted_metadata["dms_sent"] == 1
    assert inserted_metadata["dms_skipped"] == 0


@pytest.mark.parametrize(
    "metadata",
    [None, {}, {"traceback": ""}, {"traceback": "ValueError: malformed"}],
)
async def test_ingest_log_rejects_error_without_python_traceback(
    app_module, app_client, db_pool_factory, metadata
):
    pool, _, cursors = db_pool_factory(fetchone_sequence=[(91,)])
    app_module.app.state.db_pool = pool

    response = await app_client.post(
        "/api/v1/logs",
        headers={"X-Helix-Auth-Token": TEST_AUTH_TOKEN},
        json={
            "agent_name": "nova",
            "log_level": "ERROR",
            "message": "ValueError: malformed",
            "metadata": metadata,
        },
    )

    assert response.status_code == 422
    assert cursors[0].executed == []


async def test_ingest_log_accepts_error_with_python_traceback(
    app_module, app_client, db_pool_factory, monkeypatch
):
    pool, _, _ = db_pool_factory(fetchone_sequence=[(92,)])
    app_module.app.state.db_pool = pool
    monkeypatch.setattr(
        app_module.asyncio, "create_task", lambda coroutine: coroutine.close()
    )

    response = await app_client.post(
        "/api/v1/logs",
        headers={"X-Helix-Auth-Token": TEST_AUTH_TOKEN},
        json={
            "agent_name": "nova",
            "log_level": "ERROR",
            "message": "ValueError: malformed",
            "metadata": {
                "traceback": 'Traceback (most recent call last):\n  File "/app/nova_agent.py", line 42, in run\n    raise ValueError("malformed")\nValueError: malformed'
            },
        },
    )

    assert response.status_code == 200
    assert response.json()["logged_id"] == 92


async def test_ingest_log_requires_authentication(app_client):
    response = await app_client.post(
        "/api/v1/logs",
        json={"agent_name": "nova", "log_level": "INFO", "message": "m"},
    )

    assert response.status_code == 401


async def test_ingest_log_deduplicates_event_id(
    app_module, app_client, db_pool_factory
):
    pool, _, cursors = db_pool_factory(fetchone_sequence=[(77,)])
    app_module.app.state.db_pool = pool

    response = await app_client.post(
        "/api/v1/logs",
        headers={"X-Helix-Auth-Token": TEST_AUTH_TOKEN},
        json={
            "agent_name": "nova",
            "log_level": "INFO",
            "message": "Nova run started.",
            "metadata": {"event_id": "event-1", "run_id": "run-1"},
        },
    )

    assert response.status_code == 200
    assert response.json() == {"status": "duplicate", "logged_id": 77}
    assert len(cursors[0].executed) == 1


async def test_get_latest_log_empty_and_success(
    app_module, app_client, db_pool_factory
):
    pool_empty, _, _ = db_pool_factory(fetchone_sequence=[None])
    app_module.app.state.db_pool = pool_empty

    empty = await app_client.get(
        "/api/v1/logs/latest?agent=nova",
        headers={"X-Inbound-Key": TEST_AUTH_TOKEN},
    )
    assert empty.status_code == 200
    assert empty.json()["status"] == "empty"

    ts = datetime.datetime(2026, 1, 1, 12, 0, 0)
    pool_row, _, _ = db_pool_factory(
        fetchone_sequence=[
            HybridMockRow(
                {
                    "id": 99,
                    "agent_name": "nova",
                    "log_level": "INFO",
                    "message": "ok",
                    "metadata": {},
                    "timestamp": ts,
                }
            )
        ]
    )
    app_module.app.state.db_pool = pool_row

    success = await app_client.get(
        "/api/v1/logs/latest?agent=nova",
        headers={"X-Inbound-Key": TEST_AUTH_TOKEN},
    )
    assert success.status_code == 200
    assert success.json()["timestamp"] == ts.isoformat()


async def test_scheduler_current_404(app_module, app_client, db_pool_factory):
    pool, _, _ = db_pool_factory(fetchone_sequence=[None])
    app_module.app.state.db_pool = pool

    response = await app_client.get(
        "/api/v1/scheduler/current",
        headers={"X-Inbound-Key": TEST_AUTH_TOKEN},
    )

    assert response.status_code == 404


async def test_oracle_vendor_health_success(app_module, app_client, db_pool_factory):
    pool, _, _ = db_pool_factory(
        fetchall_sequence=[
            [
                {
                    "vendor_name": "A",
                    "credits_remaining": 1,
                    "is_circuit_broken": False,
                },
                {
                    "vendor_name": "B",
                    "credits_remaining": 2.5,
                    "is_circuit_broken": True,
                },
            ]
        ]
    )
    app_module.app.state.db_pool = pool

    response = await app_client.get(
        "/api/v1/oracle/vendor-health",
        headers={"X-Inbound-Key": TEST_AUTH_TOKEN},
    )

    assert response.status_code == 200
    assert response.json() == {
        "A": {"credits": 1.0, "isolated": False},
        "B": {"credits": 2.5, "isolated": True},
    }


async def test_optimize_now_success_and_failure(
    app_module, app_client, db_pool_factory, monkeypatch
):
    pool, _, _ = db_pool_factory()
    app_module.app.state.db_pool = pool

    run_opt = AsyncMock()
    monkeypatch.setattr(app_module, "run_daily_prompt_optimization", run_opt)

    ok = await app_client.post(
        "/api/v1/orchestrator/optimize-now",
        headers={"X-Helix-Auth-Token": TEST_AUTH_TOKEN},
    )
    assert ok.status_code == 200
    run_opt.assert_awaited()

    run_opt_fail = AsyncMock(side_effect=RuntimeError("boom"))
    monkeypatch.setattr(app_module, "run_daily_prompt_optimization", run_opt_fail)

    bad = await app_client.post(
        "/api/v1/orchestrator/optimize-now",
        headers={"X-Helix-Auth-Token": TEST_AUTH_TOKEN},
    )
    assert bad.status_code == 500
    assert "Forced prompt optimization failure" in bad.json()["detail"]


async def test_run_cycle_idle_and_active(
    app_module, app_client, db_pool_factory, monkeypatch
):
    pool_idle, _, _ = db_pool_factory(fetchone_sequence=[None])
    app_module.app.state.db_pool = pool_idle

    idle = await app_client.post(
        "/api/v1/orchestrator/run-cycle",
        headers={"X-Helix-Auth-Token": TEST_AUTH_TOKEN},
    )
    assert idle.status_code == 200
    assert idle.json()["status"] == "idle"

    strategy = HybridMockRow(
        {
            "industry_id": 10,
            "industry_name": "Finance and Banking",
            "moltset_category": "Finance and Banking",
            "bottleneck_profile": "slow handoff",
            "pain_tokens": ["delay"],
            "target_region": "US-East",
        }
    )
    pool_active, connection, cursors = db_pool_factory(fetchone_sequence=[strategy])
    app_module.app.state.db_pool = pool_active
    monkeypatch.setattr(
        app_module, "load_vendor_isolation_matrix", AsyncMock(return_value={})
    )

    active = await app_client.post(
        "/api/v1/orchestrator/run-cycle",
        headers={"X-Helix-Auth-Token": TEST_AUTH_TOKEN},
    )

    assert active.status_code == 200
    assert active.json()["status"] == "orchestration_cycle_pushed"
    assert connection.commit_calls >= 1
    assert len(cursors[0].executed) >= 2


async def test_run_cycle_unassigned_day_blocks_execution(
    app_module, app_client, db_pool_factory, monkeypatch
):
    """Verify orchestrator refuses to run cycles on unassigned days"""
    schedule_row = HybridMockRow(
        {
            "id": 1,
            "day_of_week": "unassigned",
            "assigned_agent": None,
            "is_active": False,
            "created_at": datetime.datetime(2026, 1, 1, 0, 0, 0),
        }
    )
    pool, _, cursors = db_pool_factory(fetchone_sequence=[schedule_row, None])
    app_module.app.state.db_pool = pool

    response = await app_client.post(
        "/api/v1/orchestrator/run-cycle",
        headers={"X-Helix-Auth-Token": TEST_AUTH_TOKEN},
    )

    assert response.status_code == 200
    assert response.json()["status"] == "standdown"
    assert "unassigned day" in response.json()["message"].lower()
