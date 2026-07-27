from unittest.mock import AsyncMock, MagicMock

import pytest

from core.auditor import (
    dispatch_chiron_optimization,
    evaluate_agent_performance,
    get_chiron_audit_candidates,
    run_agent_performance_audit_pass,
)


class ChironResponseStub:
    def __init__(self, status: str):
        self.status_code = 202 if status == "optimization_analysis_queued" else 200
        self.content = b"{}"
        self.text = status
        self._status = status

    def json(self):
        return {"status": self._status}


class ChironClientStub:
    def __init__(self, status: str):
        self.status = status

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        return False

    async def post(self, *args, **kwargs):
        return ChironResponseStub(self.status)


@pytest.mark.anyio
async def test_beacon_without_engagement_metrics_is_insufficient_data(db_pool_factory):
    pool, _, _ = db_pool_factory(
        fetchall_sequence=[
            [{"log_level": "INFO", "count": 17}],
            [{"metadata": {"records_found": 1}} for _ in range(17)],
        ]
    )

    audit = await evaluate_agent_performance(pool, "beacon")

    assert audit["status"] == "INSUFFICIENT_DATA_WITH_GAPS"
    assert audit["performance_signals"]["info_logs_analyzed"] == 17
    assert audit["performance_signals"]["kpi_rows_analyzed"] == 0
    assert audit["performance_signals"]["posts_analyzed"] == 0


@pytest.mark.anyio
async def test_beacon_with_real_low_engagement_is_underperforming(db_pool_factory):
    pool, _, _ = db_pool_factory(
        fetchall_sequence=[
            [{"log_level": "INFO", "count": 2}],
            [
                {"metadata": {"impressions": 50, "clicks": 1}},
                {"metadata": {"impressions": 70, "clicks": 1}},
            ],
        ]
    )

    audit = await evaluate_agent_performance(pool, "beacon")

    assert audit["status"] == "UNDERPERFORMING"
    assert audit["performance_signals"]["avg_impressions"] == 60


@pytest.mark.anyio
async def test_beacon_with_lifecycle_publish_metrics_is_measurable(db_pool_factory):
    pool, _, _ = db_pool_factory(
        fetchall_sequence=[
            [{"log_level": "INFO", "count": 2}],
            [
                {
                    "metadata": {
                        "posts_published": 1,
                        "lifecycle_event": "RUN_COMPLETED",
                    }
                },
                {
                    "metadata": {
                        "posts_published": 1,
                        "lifecycle_event": "RUN_COMPLETED",
                    }
                },
            ],
        ]
    )

    audit = await evaluate_agent_performance(pool, "beacon")

    assert audit["status"] == "HEALTHY"
    assert audit["performance_signals"]["posts_analyzed"] == 0
    assert audit["performance_signals"]["posts_published"] == 2
    assert audit["performance_signals"]["kpi_rows_analyzed"] == 2


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("agent_name", "metadata_rows"),
    [
        (
            "vanguard",
            [{"metadata": {"cold_leads_total": 20, "processed_leads_total": 0}}],
        ),
        (
            "nova",
            [
                {
                    "metadata": {
                        "lifecycle_event": "RUN_COMPLETED",
                        "records_found": 2,
                    }
                }
            ],
        ),
        (
            "radar",
            [
                {
                    "metadata": {
                        "stage": "batch_complete",
                        "requested_leads": 10,
                        "processed_leads": 0,
                    }
                },
                {
                    "metadata": {
                        "stage": "batch_complete",
                        "requested_leads": 10,
                        "processed_leads": 0,
                    }
                },
            ],
        ),
        (
            "pulsar",
            [
                {"metadata": {"dms_skipped": 5, "records_found": 0}},
                {"metadata": {"dms_skipped": 3, "records_found": 0}},
            ],
        ),
        (
            "nexus",
            [
                {"metadata": {"status": "SUCCESS", "records_found": 0}},
                {"metadata": {"status": "SUCCESS", "records_found": 0}},
            ],
        ),
    ],
)
async def test_non_beacon_agents_can_be_targeted_from_emitted_kpis(
    db_pool_factory, agent_name, metadata_rows
):
    pool, _, _ = db_pool_factory(
        fetchall_sequence=[
            [{"log_level": "INFO", "count": len(metadata_rows)}],
            metadata_rows,
        ]
    )

    audit = await evaluate_agent_performance(pool, agent_name)

    assert audit["status"] == "UNDERPERFORMING"


@pytest.mark.anyio
@pytest.mark.parametrize(
    "agent_name",
    ["vanguard", "radar", "pulsar", "nexus"],
)
async def test_agents_with_info_logs_but_no_kpi_rows_are_marked_as_data_gaps(
    db_pool_factory, agent_name
):
    pool, _, _ = db_pool_factory(
        fetchall_sequence=[
            [{"log_level": "INFO", "count": 4}],
            [{"metadata": {"records_found": 2}} for _ in range(4)],
        ]
    )

    audit = await evaluate_agent_performance(pool, agent_name)

    assert audit["status"] == "INSUFFICIENT_DATA_WITH_GAPS"
    assert audit["performance_signals"]["info_logs_analyzed"] == 4
    assert audit["performance_signals"]["kpi_rows_analyzed"] == 0
    assert any("KPI schema drift detected" in issue for issue in audit["issues"])


@pytest.mark.anyio
async def test_vanguard_partial_snapshot_is_marked_as_data_gap(db_pool_factory):
    pool, _, _ = db_pool_factory(
        fetchall_sequence=[
            [{"log_level": "INFO", "count": 1}],
            [{"metadata": {"processed_leads_total": 4}}],
        ]
    )

    audit = await evaluate_agent_performance(pool, "vanguard")

    assert audit["status"] == "INSUFFICIENT_DATA_WITH_GAPS"
    assert audit["performance_signals"]["kpi_rows_analyzed"] == 0


@pytest.mark.anyio
async def test_candidates_are_distinct_and_cooldown_filtered_by_query(
    db_pool_factory, monkeypatch
):
    pool, _, cursors = db_pool_factory(
        fetchall_sequence=[[{"agent_name": "pulsar"}, {"agent_name": "vanguard"}]]
    )
    monkeypatch.setenv("CHIRON_OPTIMIZATION_COOLDOWN_HOURS", "168")

    candidates = await get_chiron_audit_candidates(pool)

    query, params = cursors[0].executed[0]
    assert "GROUP BY LOWER(agent_name)" in query
    assert "QUEUED" in query and "RUNNING" in query
    assert params == (168,)
    assert candidates == ["pulsar", "vanguard"]


@pytest.mark.anyio
async def test_audit_pass_dispatches_only_first_underperformer(monkeypatch):
    monkeypatch.setattr(
        "core.auditor.get_chiron_audit_candidates",
        AsyncMock(return_value=["pulsar", "vanguard"]),
    )
    monkeypatch.setattr(
        "core.auditor.evaluate_agent_performance",
        AsyncMock(return_value={"status": "UNDERPERFORMING", "issues": ["low yield"]}),
    )
    dispatch = AsyncMock(return_value=True)
    monkeypatch.setattr("core.auditor.dispatch_chiron_optimization", dispatch)

    result = await run_agent_performance_audit_pass(object())

    dispatch.assert_awaited_once()
    assert dispatch.await_args is not None
    assert dispatch.await_args.args[1] == "pulsar"
    assert result["target_agent"] == "pulsar"


@pytest.mark.anyio
async def test_audit_pass_emits_warning_for_kpi_data_gap_and_skips_dispatch(
    monkeypatch,
):
    monkeypatch.setattr(
        "core.auditor.get_chiron_audit_candidates",
        AsyncMock(return_value=["beacon"]),
    )
    monkeypatch.setattr(
        "core.auditor.evaluate_agent_performance",
        AsyncMock(
            return_value={
                "status": "INSUFFICIENT_DATA_WITH_GAPS",
                "issues": ["KPI schema drift detected: missing impressions/clicks"],
                "performance_signals": {
                    "info_logs_analyzed": 7,
                    "kpi_rows_analyzed": 0,
                },
            }
        ),
    )
    dispatch = AsyncMock(return_value=True)
    notify = MagicMock()
    monkeypatch.setattr("core.auditor.dispatch_chiron_optimization", dispatch)
    monkeypatch.setattr("core.auditor.emit_executive_notification", notify)

    result = await run_agent_performance_audit_pass(object())

    dispatch.assert_not_awaited()
    notify.assert_called_once()
    notification = notify.call_args.args[0]
    assert notification.source_agent == "beacon"
    assert notification.metadata["agent"] == "beacon"
    assert result["target_agent"] is None
    assert result["audit_status"] == "NO_ELIGIBLE_UNDERPERFORMER"


@pytest.mark.anyio
async def test_dispatch_marks_only_new_chiron_queue_entry(db_pool_factory, monkeypatch):
    pool, _, cursors = db_pool_factory()
    monkeypatch.setattr(
        "core.auditor.is_vendor_circuit_healthy", AsyncMock(return_value=True)
    )
    monkeypatch.setattr(
        "core.auditor.httpx.AsyncClient",
        lambda *args, **kwargs: ChironClientStub("optimization_analysis_queued"),
    )
    monkeypatch.setattr("core.auditor.emit_executive_notification", MagicMock())

    dispatched = await dispatch_chiron_optimization(
        pool, "pulsar", {"issues": ["zero DM yield"], "performance_signals": {}}
    )

    assert dispatched is True
    executed_sql = " ".join(query for query, _ in cursors[0].executed)
    assert "last_optimized_at = NOW()" in executed_sql
    assert "optimization_status = 'QUEUED'" in executed_sql


@pytest.mark.anyio
async def test_dispatch_does_not_timestamp_duplicate_queue_entry(
    db_pool_factory, monkeypatch
):
    pool, _, cursors = db_pool_factory()
    monkeypatch.setattr(
        "core.auditor.is_vendor_circuit_healthy", AsyncMock(return_value=True)
    )
    monkeypatch.setattr(
        "core.auditor.httpx.AsyncClient",
        lambda *args, **kwargs: ChironClientStub(
            "optimization_analysis_already_queued"
        ),
    )

    dispatched = await dispatch_chiron_optimization(
        pool, "beacon", {"issues": ["low reach"], "performance_signals": {}}
    )

    assert dispatched is False
    assert cursors[0].executed == []
