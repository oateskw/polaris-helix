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
}


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
        "status": "HEALTHY",
        "total_runs": 0,
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

                    for log in beacon_logs:
                        meta = log.get("metadata") or {}
                        if isinstance(meta, dict):
                            total_impressions += int(meta.get("impressions", 0))
                            total_clicks += int(meta.get("clicks", 0))
                            post_count += 1

                    avg_impressions = total_impressions / max(post_count, 1)
                    metrics["performance_signals"] = {
                        "posts_analyzed": post_count,
                        "total_impressions": total_impressions,
                        "avg_impressions": avg_impressions,
                        "total_clicks": total_clicks,
                    }

                    min_impressions = thresholds.get("min_avg_impressions", 150)
                    if post_count > 0 and avg_impressions < min_impressions:
                        metrics["status"] = "UNDERPERFORMING"
                        metrics["issues"].append(
                            f"Average post impressions ({avg_impressions:.1f}) fell below target baseline ({min_impressions}). Post reach is severely throttled."
                        )

                    if post_count > 0 and total_clicks < thresholds.get(
                        "min_link_clicks", 5
                    ):
                        metrics["status"] = "UNDERPERFORMING"
                        metrics["issues"].append(
                            f"Total referral traffic clicks ({total_clicks}) in the last 7 days is too low. CTA / hook alignment is failing."
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
                        if isinstance(meta, dict) and "harvested_count" in meta:
                            harvested_leads += int(meta.get("harvested_count", 0))
                            nova_runs += 1

                    avg_harvest = harvested_leads / max(nova_runs, 1)
                    metrics["performance_signals"] = {
                        "runs_analyzed": nova_runs,
                        "total_harvested": harvested_leads,
                        "avg_harvest_per_run": avg_harvest,
                    }

                    min_yield = thresholds.get("min_harvest_yield_per_run", 10)
                    if nova_runs > 0 and avg_harvest < min_yield:
                        metrics["status"] = "UNDERPERFORMING"
                        metrics["issues"].append(
                            f"Nova average lead harvest yield ({avg_harvest:.1f}/run) fell below threshold ({min_yield}). Targeting scope may be exhausted."
                        )

                # 3. Universal Error Density Threshold (Applies to ALL Agents)
                if metrics["error_count"] > 5:
                    metrics["status"] = "UNDERPERFORMING"
                    metrics["issues"].append(
                        f"High error density detected: {metrics['error_count']} ERROR logs in the last {lookback_days} days."
                    )

                # Add zero-yield inspection for lead generation agents (nexus / nova / radar)
                if normalized_agent in ["nexus", "nova", "radar"]:
                    await cur.execute(
                        """
                        SELECT metadata
                        FROM agent_system_logs
                        WHERE LOWER(agent_name) = %s
                        AND timestamp >= NOW() - INTERVAL '24 hours'
                        ORDER BY id DESC LIMIT 5;
                    """,
                        (normalized_agent,),
                    )
                    recent_logs = await cur.fetchall()

                    zero_yield_runs = 0
                    for r in recent_logs:
                        meta = r.get("metadata") or {}
                        if (
                            isinstance(meta, dict)
                            and meta.get("requested", 0) > 0
                            and meta.get("processed", 0) == 0
                        ):
                            zero_yield_runs += 1

                    if zero_yield_runs >= 2:
                        metrics["status"] = "UNDERPERFORMING"
                        metrics["issues"].append(
                            f"Zero-yield burnout detected: Last {zero_yield_runs} sweeps yielded 0 new processed leads due to duplicate saturation."
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

            if res.status_code in [200, 201, 202]:
                # 🚀 UPDATE DB TIMESTAMP: Mark manifest as optimized
                try:
                    async with db_pool.connection() as conn:
                        async with conn.cursor() as cur:
                            await cur.execute(
                                """
                                UPDATE helix_agent_manifests 
                                SET last_optimized_at = NOW() 
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

    async with db_pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("""
                SELECT LOWER(agent_name) as agent_name 
                FROM helix_agent_manifests 
                WHERE is_active = TRUE;
            """)
            active_agents = [r["agent_name"] for r in await cur.fetchall()]

    preflight_issues = []
    for agent in active_agents or ["beacon", "vanguard", "nova", "nexus"]:
        audit = await evaluate_agent_performance(db_pool, agent)
        if audit.get("status") == "UNDERPERFORMING":
            preflight_issues.append((agent, audit))

    # Trigger Chiron proactively if pre-flight flags issues
    for agent_name, audit in preflight_issues:
        print(f"⚠️ [PRE-FLIGHT] Proactively dispatching Chiron for '{agent_name}'")
        await dispatch_chiron_optimization(db_pool, agent_name, audit)

    return {"status": "completed", "issues_found": len(preflight_issues)}


async def run_agent_performance_audit_loop(db_pool):
    """
    Background loop running inside Helix that audits key agents from helix_agent_manifests
    and automatically triggers Chiron when performance degrades.
    """
    print("🧐 [EXECUTIVE AUDITOR] Performance audit loop initialized.")
    while True:
        try:
            await run_routine_log_maintenance(db_pool, retention_days=30)

            async with db_pool.connection() as conn:
                async with conn.cursor(row_factory=dict_row) as cur:
                    await cur.execute("""
                        SELECT LOWER(agent_name) as agent_name
                        FROM helix_agent_manifests
                        WHERE is_active = TRUE;
                    """)
                    manifest_agents = await cur.fetchall()

            active_agent_names = [row["agent_name"] for row in manifest_agents]
            if not active_agent_names:
                active_agent_names = ["beacon", "vanguard", "nova"]

            for agent_name in active_agent_names:
                audit = await evaluate_agent_performance(db_pool, agent_name)
                if audit.get("status") == "UNDERPERFORMING":
                    print(
                        f"⚠️ [AUDITOR] Agent '{agent_name}' is UNDERPERFORMING. Triggering Chiron..."
                    )
                    await dispatch_chiron_optimization(db_pool, agent_name, audit)
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
