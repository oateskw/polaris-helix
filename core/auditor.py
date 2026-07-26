import asyncio
import os
import httpx
from typing import Dict, Any
from psycopg.rows import dict_row

from config.settings import CHIRON_API_URL
from services.telemetry import (
    emit_executive_notification,
    ExecutiveNotificationEvent,
    is_vendor_circuit_healthy,
)

# Baseline KPI thresholds for agent performance audits
AGENT_PERFORMANCE_THRESHOLDS = {
    "beacon": {
        "min_avg_impressions": 150,
        "min_link_clicks": 5,
        "min_posts_published": 1,
        "lookback_days": 7,
    },
    "vanguard": {
        "min_open_rate_pct": 35.0,
        "max_bounce_rate_pct": 3.0,
        "lookback_days": 7,
    },
    "nova": {
        "min_harvest_yield_per_run": 10,
        "lookback_days": 3,
    },
    "radar": {"min_processed_rate": 0.10, "lookback_days": 3},
    "pulsar": {"min_dms_per_batch": 1, "lookback_days": 3},
    "nexus": {"min_records_per_run": 1, "lookback_days": 3},
}

DEFAULT_CHIRON_COOLDOWN_HOURS = 7 * 24
INSUFFICIENT_DATA_WITH_GAPS = "INSUFFICIENT_DATA_WITH_GAPS"


def _flag_kpi_coverage_gap(
    metrics: Dict[str, Any],
    kpi_rows_analyzed: int,
    expected_signals: str,
) -> None:
    """Mark a data-contract gap when INFO logs exist but no KPI-compatible rows were found."""
    performance_signals = metrics.setdefault("performance_signals", {})
    performance_signals["info_logs_analyzed"] = metrics.get("info_logs", 0)
    performance_signals["kpi_rows_analyzed"] = kpi_rows_analyzed

    if metrics.get("info_logs", 0) <= 0 or kpi_rows_analyzed > 0:
        return

    metrics["status"] = INSUFFICIENT_DATA_WITH_GAPS
    metrics["issues"].append(
        "KPI schema drift detected: "
        f"observed {metrics['info_logs']} INFO logs but 0 KPI-compatible rows. "
        f"Expected signals: {expected_signals}."
    )


async def get_chiron_audit_candidates(db_pool) -> list[str]:
    cooldown_hours = max(
        1,
        int(
            os.getenv(
                "CHIRON_OPTIMIZATION_COOLDOWN_HOURS",
                str(DEFAULT_CHIRON_COOLDOWN_HOURS),
            )
        ),
    )
    async with db_pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT LOWER(agent_name) AS agent_name,
                       MAX(last_optimized_at) AS last_optimized_at
                FROM helix_agent_manifests
                WHERE is_active = TRUE
                GROUP BY LOWER(agent_name)
                HAVING (
                    MAX(last_optimized_at) IS NULL
                    OR MAX(last_optimized_at) <= NOW() - (%s || ' hours')::INTERVAL
                )
                AND NOT BOOL_OR(
                    COALESCE(optimization_status, 'IDLE') IN ('QUEUED', 'RUNNING')
                )
                ORDER BY MAX(last_optimized_at) ASC NULLS FIRST, LOWER(agent_name);
                """,
                (cooldown_hours,),
            )
            rows = await cur.fetchall()
    return [row["agent_name"] for row in rows]


async def evaluate_agent_performance(db_pool, agent_name: str) -> Dict[str, Any]:
    """
    Analyzes an agent's recent execution metrics and logs from Postgres
    to determine if the agent is performing, idling, or underperforming.
    """
    normalized_agent = agent_name.lower().strip()
    thresholds = AGENT_PERFORMANCE_THRESHOLDS.get(normalized_agent, {})
    lookback_days = thresholds.get("lookback_days", 7)

    metrics = {
        "agent": normalized_agent,
        "status": "INSUFFICIENT_DATA",
        "total_runs": 0,
        "info_logs": 0,
        "error_count": 0,
        "performance_signals": {},
        "issues": [],
    }

    try:
        async with db_pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                # 1. Evaluate log health & error density for ALL agents
                await cur.execute(
                    """
                    SELECT log_level, COUNT(*) as count
                    FROM agent_system_logs
                    WHERE LOWER(agent_name) = %s
                    AND timestamp >= NOW() - (%s || ' days')::INTERVAL
                    GROUP BY log_level;
                    """,
                    (normalized_agent, lookback_days),
                )
                log_counts = await cur.fetchall()
                for row in log_counts:
                    if row["log_level"] == "ERROR":
                        metrics["error_count"] = row["count"]
                    if row["log_level"] == "INFO":
                        metrics["info_logs"] = row["count"]
                    metrics["total_runs"] += row["count"]

                # 2. Agent-Specific KPI Evaluation
                if normalized_agent == "beacon":
                    await cur.execute("""
                        SELECT metadata
                        FROM agent_system_logs
                        WHERE LOWER(agent_name) = 'beacon'
                        AND log_level = 'INFO'
                        AND timestamp >= NOW() - INTERVAL '7 days';
                        """)
                    beacon_logs = await cur.fetchall()

                    total_impressions = 0
                    total_clicks = 0
                    post_count = 0
                    published_posts_total = 0

                    for log in beacon_logs:
                        meta = log.get("metadata") or {}
                        if isinstance(meta, dict):
                            published_posts_total += int(meta.get("posts_published", 0))
                        if isinstance(meta, dict) and (
                            "impressions" in meta or "clicks" in meta
                        ):
                            total_impressions += int(meta.get("impressions", 0))
                            total_clicks += int(meta.get("clicks", 0))
                            post_count += 1

                    avg_impressions = total_impressions / max(post_count, 1)
                    metrics["performance_signals"] = {
                        "posts_analyzed": post_count,
                        "posts_published": published_posts_total,
                        "total_impressions": total_impressions,
                        "avg_impressions": avg_impressions,
                        "total_clicks": total_clicks,
                    }
                    _flag_kpi_coverage_gap(
                        metrics,
                        max(post_count, published_posts_total),
                        "impressions/clicks or posts_published lifecycle summary",
                    )

                    if post_count > 0:
                        metrics["status"] = "HEALTHY"
                        min_impressions = thresholds.get("min_avg_impressions", 150)
                        if avg_impressions < min_impressions:
                            metrics["status"] = "UNDERPERFORMING"
                            metrics["issues"].append(
                                f"Average post impressions ({avg_impressions:.1f}) fell below target baseline ({min_impressions}). Post reach is severely throttled."
                            )

                        if total_clicks < thresholds.get("min_link_clicks", 5):
                            metrics["status"] = "UNDERPERFORMING"
                            metrics["issues"].append(
                                f"Total referral traffic clicks ({total_clicks}) in the last 7 days is too low. CTA / hook alignment is failing."
                            )
                    elif published_posts_total > 0:
                        metrics["status"] = "HEALTHY"
                        min_posts = thresholds.get("min_posts_published", 1)
                        if published_posts_total < min_posts:
                            metrics["status"] = "UNDERPERFORMING"
                            metrics["issues"].append(
                                "Beacon published too few campaigns in the current lookback window."
                            )

                elif normalized_agent == "vanguard":
                    await cur.execute("""
                        SELECT metadata
                        FROM agent_system_logs
                        WHERE LOWER(agent_name) = 'vanguard'
                        AND log_level = 'INFO'
                        AND timestamp >= NOW() - INTERVAL '7 days'
                        ORDER BY id DESC LIMIT 10;
                    """)
                    snapshots = [
                        row.get("metadata") or {}
                        for row in await cur.fetchall()
                        if isinstance(row.get("metadata"), dict)
                        and "processed_leads_total" in row.get("metadata", {})
                    ]
                    metrics["performance_signals"] = {
                        "snapshots_analyzed": len(snapshots),
                        "latest_processed_leads_total": (
                            int(snapshots[0].get("processed_leads_total", 0))
                            if snapshots
                            else 0
                        ),
                        "latest_cold_leads_total": (
                            int(snapshots[0].get("cold_leads_total", 0))
                            if snapshots
                            else 0
                        ),
                    }
                    _flag_kpi_coverage_gap(
                        metrics,
                        len(snapshots),
                        "processed_leads_total and cold_leads_total snapshots",
                    )
                    if snapshots:
                        metrics["status"] = "HEALTHY"
                        latest = snapshots[0]
                        if (
                            int(latest.get("cold_leads_total", 0)) > 0
                            and int(latest.get("processed_leads_total", 0)) == 0
                        ):
                            metrics["status"] = "UNDERPERFORMING"
                            metrics["issues"].append(
                                "Vanguard has available cold leads but no processed leads."
                            )

                elif normalized_agent == "nova":
                    # Nova evaluation: check average contacts/leads harvested per run
                    await cur.execute("""
                        SELECT metadata
                        FROM agent_system_logs
                        WHERE LOWER(agent_name) = 'nova'
                        AND log_level = 'INFO'
                        AND timestamp >= NOW() - INTERVAL '3 days';
                        """)
                    nova_logs = await cur.fetchall()

                    harvested_leads = 0
                    nova_runs = 0
                    for log in nova_logs:
                        meta = log.get("metadata") or {}
                        if isinstance(meta, dict) and (
                            "harvested_count" in meta
                            or meta.get("lifecycle_event") == "RUN_COMPLETED"
                        ):
                            harvested_leads += int(
                                meta.get(
                                    "harvested_count", meta.get("records_found", 0)
                                )
                            )
                            nova_runs += 1

                    avg_harvest = harvested_leads / max(nova_runs, 1)
                    metrics["performance_signals"] = {
                        "runs_analyzed": nova_runs,
                        "total_harvested": harvested_leads,
                        "avg_harvest_per_run": avg_harvest,
                    }
                    _flag_kpi_coverage_gap(
                        metrics,
                        nova_runs,
                        "harvested_count or lifecycle_event=RUN_COMPLETED",
                    )

                    if nova_runs > 0:
                        metrics["status"] = "HEALTHY"
                        min_yield = thresholds.get("min_harvest_yield_per_run", 10)
                        if avg_harvest < min_yield:
                            metrics["status"] = "UNDERPERFORMING"
                            metrics["issues"].append(
                                f"Nova average lead harvest yield ({avg_harvest:.1f}/run) fell below threshold ({min_yield}). Targeting scope may be exhausted."
                            )

                elif normalized_agent in {"nexus", "radar", "pulsar"}:
                    await cur.execute(
                        """
                        SELECT metadata
                        FROM agent_system_logs
                        WHERE LOWER(agent_name) = %s
                        AND log_level = 'INFO'
                        AND timestamp >= NOW() - (%s || ' days')::INTERVAL
                        ORDER BY id DESC LIMIT 10;
                        """,
                        (normalized_agent, lookback_days),
                    )
                    info_metadata = [
                        row.get("metadata") or {}
                        for row in await cur.fetchall()
                        if isinstance(row.get("metadata"), dict)
                    ]

                    if normalized_agent == "radar":
                        completed = [
                            meta
                            for meta in info_metadata
                            if meta.get("stage") == "batch_complete"
                            and "requested_leads" in meta
                            and "processed_leads" in meta
                        ]
                        requested = sum(
                            int(meta["requested_leads"]) for meta in completed
                        )
                        processed = sum(
                            int(meta["processed_leads"]) for meta in completed
                        )
                        processed_rate = processed / max(requested, 1)
                        metrics["performance_signals"] = {
                            "batches_analyzed": len(completed),
                            "requested_leads": requested,
                            "processed_leads": processed,
                            "processed_rate": processed_rate,
                        }
                        _flag_kpi_coverage_gap(
                            metrics,
                            len(completed),
                            "stage=batch_complete with requested_leads and processed_leads",
                        )
                        if completed:
                            metrics["status"] = "HEALTHY"
                            if (
                                len(completed) >= 2
                                and requested > 0
                                and processed_rate
                                < thresholds.get("min_processed_rate", 0.10)
                            ):
                                metrics["status"] = "UNDERPERFORMING"
                                metrics["issues"].append(
                                    f"Radar processed-rate ({processed_rate:.1%}) is below the minimum target."
                                )
                    elif normalized_agent == "pulsar":
                        completed = [
                            meta for meta in info_metadata if "dms_skipped" in meta
                        ]
                        sent = sum(
                            int(meta.get("records_found", 0)) for meta in completed
                        )
                        metrics["performance_signals"] = {
                            "batches_analyzed": len(completed),
                            "dms_sent": sent,
                        }
                        _flag_kpi_coverage_gap(
                            metrics,
                            len(completed),
                            "dms_skipped marker with records_found",
                        )
                        if completed:
                            metrics["status"] = "HEALTHY"
                            if len(completed) >= 2 and sent < thresholds.get(
                                "min_dms_per_batch", 1
                            ):
                                metrics["status"] = "UNDERPERFORMING"
                                metrics["issues"].append(
                                    "Pulsar completed multiple batches without sending a DM."
                                )
                    else:
                        completed = [
                            meta
                            for meta in info_metadata
                            if meta.get("status") in {"SUCCESS", "RESCUE_RUN"}
                            and "records_found" in meta
                        ]
                        records = sum(
                            int(meta.get("records_found", 0)) for meta in completed
                        )
                        metrics["performance_signals"] = {
                            "runs_analyzed": len(completed),
                            "records_found": records,
                        }
                        _flag_kpi_coverage_gap(
                            metrics,
                            len(completed),
                            "status in {SUCCESS, RESCUE_RUN} with records_found",
                        )
                        if completed:
                            metrics["status"] = "HEALTHY"
                            if len(completed) >= 2 and records < thresholds.get(
                                "min_records_per_run", 1
                            ):
                                metrics["status"] = "UNDERPERFORMING"
                                metrics["issues"].append(
                                    "Nexus completed multiple runs without producing records."
                                )

                # 3. Universal Error Density Threshold (Applies to ALL Agents)
                if metrics["error_count"] > 5:
                    metrics["status"] = "UNDERPERFORMING"
                    metrics["issues"].append(
                        f"High error density detected: {metrics['error_count']} ERROR logs in the last {lookback_days} days."
                    )

    except Exception as e:
        print(f"⚠️ Performance evaluation error for {agent_name}: {e}")
        metrics["status"] = "UNKNOWN"
        metrics["issues"].append(f"Evaluation query exception: {str(e)}")

    return metrics


async def dispatch_chiron_optimization(
    db_pool, agent_name: str, audit_results: Dict[str, Any]
) -> bool:
    """
    Formulates a technical specification and fires an optimization request
    to Chiron's `/api/v1/chiron/optimize` endpoint.
    """
    helix_token = os.getenv("HELIX_AUTH_TOKEN", "")
    if not helix_token:
        print("⚠️ HELIX_AUTH_TOKEN missing. Cannot dispatch optimization to Chiron.")
        return False

    if not await is_vendor_circuit_healthy(db_pool, "Anthropic"):
        print("🛑 Anthropic circuit broken. Aborting Chiron dispatch.")
        return False

    issues_summary = "\n".join(
        [f"• {issue}" for issue in audit_results.get("issues", [])]
    )

    refactor_instructions = {
        "beacon": (
            "REFACTORING GOAL: Optimize LinkedIn engagement, algorithm reach, and inbound conversions.\n"
            "1. Update beacon_agent.py prompt structures to use high-converting curiosity gap hooks.\n"
            "2. Remove outbound website links from post bodies to avoid LinkedIn algorithm suppression. Move CTAs to comment keyword triggers.\n"
            "3. Format text copy with mobile-friendly short paragraphs and bullet points.\n"
            "4. Align copy to drive users directly to Prism and ScopingPage inbound tools."
        ),
        "vanguard": (
            "REFACTORING GOAL: Improve email open rates and deliverability.\n"
            "1. Shorten subject lines to under 4 words.\n"
            "2. Ensure exact 4-paragraph Markdown-free body layouts."
        ),
        "nova": (
            "REFACTORING GOAL: Optimize enrichment yields and target discovery.\n"
            "1. Broaden search criteria when encountering depleted pool metrics.\n"
            "2. Optimize payload parameters passed to MoltSet."
        ),
    }

    target_instruction = refactor_instructions.get(
        agent_name.lower(),
        f"Optimize system prompts and logic for {agent_name} to resolve underperformance issues.",
    )

    optimization_payload = {
        "target_agent": agent_name.lower(),
        "reason": f"Performance audit flagged agent as UNDERPERFORMING: {audit_results.get('issues')}",
        "issues_detected": audit_results.get("issues", []),
        "optimization_specification": f"{target_instruction}\n\nDETECTED ISSUES:\n{issues_summary}",
        "metrics_context": audit_results.get("performance_signals", {}),
    }

    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            res = await client.post(
                f"{CHIRON_API_URL.rstrip('/')}/api/v1/chiron/optimize",
                headers={
                    "X-Chiron-Key": helix_token,
                    "Content-Type": "application/json",
                },
                json=optimization_payload,
            )

            response_payload = res.json() if res.content else {}
            response_status = response_payload.get("status")
            if (
                res.status_code in [200, 201, 202]
                and response_status == "optimization_analysis_queued"
            ):
                # 🚀 UPDATE DB TIMESTAMP: Mark manifest as optimized
                try:
                    async with db_pool.connection() as conn:
                        async with conn.cursor() as cur:
                            await cur.execute(
                                """
                                UPDATE helix_agent_manifests 
                                SET last_optimized_at = NOW(),
                                    optimization_status = 'QUEUED'
                                WHERE LOWER(agent_name) = %s;
                                """,
                                (agent_name.lower(),),
                            )
                        await conn.commit()
                except Exception as db_err:
                    print(
                        f"⚠️ Failed to update last_optimized_at for {agent_name}: {db_err}"
                    )

                emit_executive_notification(
                    ExecutiveNotificationEvent(
                        source_agent="helix",
                        event_type="pipeline_milestone",
                        severity="success",
                        headline="Chiron Optimization Dispatched",
                        outcome=f"Helix detected underperformance in `{agent_name}` and dispatched Chiron optimization orders.",
                        impact=f"Issues: {', '.join(audit_results.get('issues', []))}",
                        next_action="Chiron is running sandboxed tests & creating GitHub PR.",
                    )
                )
                return True

            if response_status == "optimization_analysis_already_queued":
                print(f"ℹ️ Chiron optimization already queued for {agent_name}.")
                return False

            print(
                f"❌ Chiron optimization dispatch rejected: {res.status_code} - {res.text}"
            )
            return False

    except Exception as e:
        print(f"💥 Failed to dispatch Chiron optimization: {e}")
        return False


async def run_preflight_audit(db_pool) -> dict:
    """
    Executes a pre-flight sanity check across active agents BEFORE
    the main morning dispatch cycle fires.
    """
    print("🛫 [AUDITOR] Running pre-flight system diagnostics...")
    # Clean up logs before evaluating preflight metrics
    await run_routine_log_maintenance(db_pool, retention_days=30)

    result = await run_agent_performance_audit_pass(db_pool)
    return {
        "status": "completed",
        "issues_found": int(result["audit_status"] == "UNDERPERFORMING"),
        **result,
    }


async def run_agent_performance_audit_pass(db_pool) -> dict[str, Any]:
    candidates = await get_chiron_audit_candidates(db_pool)
    for agent_name in candidates:
        audit = await evaluate_agent_performance(db_pool, agent_name)
        if audit.get("status") == INSUFFICIENT_DATA_WITH_GAPS:
            emit_executive_notification(
                ExecutiveNotificationEvent(
                    source_agent="helix",
                    event_type="telemetry_gap",
                    severity="warning",
                    headline=f"{agent_name.capitalize()} KPI Telemetry Gap Detected",
                    outcome=(
                        "Audit could not evaluate underperformance because KPI-compatible "
                        "payloads were missing from recent INFO logs."
                    ),
                    impact="; ".join(audit.get("issues", [])) or None,
                    next_action=(
                        "Align agent log metadata to Helix KPI contracts so Chiron can make "
                        "optimization decisions with complete evidence."
                    ),
                    metadata={
                        "agent": agent_name,
                        "performance_signals": audit.get("performance_signals", {}),
                    },
                )
            )
            continue

        if audit.get("status") != "UNDERPERFORMING":
            continue

        print(
            f"⚠️ [AUDITOR] Agent '{agent_name}' is UNDERPERFORMING. Triggering Chiron..."
        )
        dispatched = await dispatch_chiron_optimization(db_pool, agent_name, audit)
        return {
            "target_agent": agent_name,
            "audit_status": audit["status"],
            "dispatched": dispatched,
        }

    return {
        "target_agent": None,
        "audit_status": "NO_ELIGIBLE_UNDERPERFORMER",
        "dispatched": False,
    }


async def run_agent_performance_audit_loop(db_pool):
    """
    Background loop running inside Helix that audits key agents from helix_agent_manifests
    and automatically triggers Chiron when performance degrades.
    """
    print("🧐 [EXECUTIVE AUDITOR] Performance audit loop initialized.")
    while True:
        try:
            await run_routine_log_maintenance(db_pool, retention_days=30)

            await run_agent_performance_audit_pass(db_pool)
        except Exception as e:
            print(f"⚠️ [AUDITOR] Loop iteration anomaly: {e}")

        await asyncio.sleep(1800)


async def run_routine_log_maintenance(db_pool, retention_days: int = 30) -> None:
    """Purge system logs older than the retention threshold."""
    try:
        async with db_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    DELETE FROM agent_system_logs 
                    WHERE timestamp < NOW() - (%s || ' days')::INTERVAL;
                    """,
                    (retention_days,),
                )
            await conn.commit()
            print(
                f"🧹 [MAINTENANCE] Stale system logs older than {retention_days} days purged."
            )
    except Exception as e:
        print(f"⚠️ [MAINTENANCE] Failed to purge stale logs: {e}")
