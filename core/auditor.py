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
                # 1. Evaluate log health & error density
                await cur.execute(
                    """
                    SELECT log_level, COUNT(*) as count
                    FROM agent_system_logs
                    WHERE agent_name = %s
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

                # 2. Agent-Specific Metric Evaluation
                if normalized_agent == "beacon":
                    # Check impressions and referral clicks logged in metadata
                    await cur.execute("""
                        SELECT metadata
                        FROM agent_system_logs
                        WHERE agent_name = 'beacon'
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

                    # Evaluate against threshold
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

    # Verify Anthropic health before waking Chiron
    if not await is_vendor_circuit_healthy(db_pool, "Anthropic"):
        print("🛑 Anthropic circuit broken. Aborting Chiron dispatch.")
        return False

    issues_summary = "\n".join(
        [f"• {issue}" for issue in audit_results.get("issues", [])]
    )

    # Agent-specific refactoring directives for Chiron
    refactor_instructions = {
        "beacon": (
            "REFACTORING GOAL: Optimize LinkedIn engagement, algorithm reach, and inbound conversions.\n"
            "1. Update beacon_agent.py prompt structures to use high-converting curiosity gap hooks.\n"
            "2. Remove outbound website links from post bodies to avoid LinkedIn algorithm suppression. Move CTAs to comment keyword triggers (e.g. 'Comment BLUEPRINT below').\n"
            "3. Format text copy with mobile-friendly short paragraphs and bullet points.\n"
            "4. Align copy to drive users directly to Prism and ScopingPage inbound tools."
        ),
        "vanguard": (
            "REFACTORING GOAL: Improve email open rates and deliverability.\n"
            "1. Shorten subject lines to under 4 words.\n"
            "2. Ensure exact 4-paragraph Markdown-free body layouts."
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


async def run_agent_performance_audit_loop(db_pool):
    """
    Background loop running inside Helix that audits key agents on a daily schedule
    and automatically triggers Chiron when performance degrades.
    """
    from config.settings import should_helix_workers_operate

    print("🧐 [EXECUTIVE AUDITOR] Performance audit loop initialized.")
    while True:
        try:
            if should_helix_workers_operate():
                for agent_name in ["beacon", "vanguard"]:
                    audit = await evaluate_agent_performance(db_pool, agent_name)
                    if audit.get("status") == "UNDERPERFORMING":
                        print(
                            f"⚠️ [AUDITOR] Agent '{agent_name}' is UNDERPERFORMING. Triggering Chiron..."
                        )
                        await dispatch_chiron_optimization(db_pool, agent_name, audit)
            else:
                print("⏸️ [AUDITOR] Standby mode - unassigned day. Audit skipped.")
        except Exception as e:
            print(f"⚠️ [AUDITOR] Loop iteration anomaly: {e}")

        # Run performance audit every 12 hours
        await asyncio.sleep(12 * 3600)
