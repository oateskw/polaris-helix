import os
import json
import asyncio
import traceback
import hashlib
import httpx
import requests
import datetime
from dataclasses import dataclass
from typing import Any, Optional
from psycopg.types.json import Jsonb
from psycopg.rows import dict_row
from collections import deque
import time

from config.settings import NEW_YORK_TZ, CHIRON_API_URL, ANTHROPIC_MODEL

# Reimport to prevent circular loops
from services.mesh_client import trigger_downstream_mesh_with_payload

# Rate limiting windows
_GLOBAL_NOTIFICATION_HISTORY = deque()
_MAX_NOTIFICATIONS_PER_MINUTE = (
    6  # Caps output strictly to roughly 1 alert per 10 seconds
)


@dataclass
class ExecutiveNotificationEvent:
    source_agent: str
    event_type: str
    severity: str
    headline: str
    outcome: str
    impact: str | None = None
    next_action: str | None = None
    metadata: dict[str, Any] | None = None


_NOTIFICATION_FINGERPRINT_LAST_SENT: dict[str, datetime.datetime] = {}
_NOTIFICATION_DEDUPE_WINDOW_SECONDS = int(
    os.getenv("HELIX_NOTIFICATION_DEDUPE_SECONDS", "300")
)
_IMMEDIATE_ALERT_EVENT_TYPES = {
    "critical_failure",
    "vendor_circuit_breaker",
    "pipeline_milestone",
    "manual_command_ack",
}


def get_new_york_now() -> datetime.datetime:
    return datetime.datetime.now(NEW_YORK_TZ)


async def is_vendor_circuit_healthy(db_pool, vendor_name: str) -> bool:
    """Checks the infrastructure ledger to see if a vendor's circuit is broken."""
    try:
        async with db_pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    """
                    SELECT is_circuit_broken 
                    FROM helix_infrastructure_oracle_ledger 
                    WHERE vendor_name = %s LIMIT 1;
                    """,
                    (vendor_name,),
                )
                record = await cur.fetchone()
                # If no record exists, assume safe; if it exists, return the inverse of the trip flag
                if record:
                    return not record.get("is_circuit_broken", False)
                return True
    except Exception as e:
        print(f"⚠️ Circuit ledger lookup failed for {vendor_name}: {e}")
        return True  # Fallback to true so a DB hiccup doesn't paralyze the system


# Telegram and Command Logging
def send_telegram_broadcast(message: str, notification_type: str = "verbose"):
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        return

    prefixes = {
        "critical": "🚨 [CRITICAL INFRASTRUCTURE]",
        "success": "✅ [PIPELINE SUCCESS]",
        "orchestration": "🧠 [HELIX ORCHESTRATOR]",
        "verbose": "📡 [TELEMETRY]",
    }
    prefix = prefixes.get(notification_type, "📡")
    extra_details = ""
    if notification_type == "critical":
        tb_str = traceback.format_exc()
        if tb_str and "NoneType: None" not in tb_str:
            extra_details = f"\n\n**🔍 Traceback:**\n```python\n{tb_str[-1500:]}\n```"

    formatted_message = f"{prefix}\n{message}{extra_details}"

    async def _send() -> None:
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                await client.post(
                    f"https://api.telegram.org/bot{token}/sendMessage",
                    json={
                        "chat_id": chat_id,
                        "text": formatted_message,
                        "parse_mode": "Markdown",
                    },
                )
        except Exception as e:
            print(f"⚠️ Telegram broadcast async failed: {e}")

    # FIX 1: Explicitly run or schedule the async routine
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(_send())
    except RuntimeError:
        # Fallback for non-async calling threads
        try:
            requests.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={
                    "chat_id": chat_id,
                    "text": formatted_message,
                    "parse_mode": "Markdown",
                },
                timeout=5,
            )
        except Exception as e:
            print(f"⚠️ Telegram broadcast sync fallback failed: {e}")


def _event_severity_to_channel(severity: str) -> str:
    normalized = (severity or "info").lower()
    if normalized in {"critical", "error"}:
        return "critical"
    if normalized in {"success", "ok"}:
        return "success"
    return "orchestration"


def _event_severity_to_emoji(severity: str) -> str:
    normalized = (severity or "info").lower()
    if normalized in {"critical", "error"}:
        return "🚨"
    if normalized in {"warning", "warn"}:
        return "⚠️"
    if normalized in {"success", "ok"}:
        return "✅"
    return "🧠"


def _event_fingerprint(event: ExecutiveNotificationEvent) -> str:
    payload = {
        "source_agent": event.source_agent,
        "event_type": event.event_type,
        "severity": event.severity,
        "headline": event.headline,
        "outcome": event.outcome,
    }
    digest_src = json.dumps(payload, sort_keys=True)
    return hashlib.sha256(digest_src.encode("utf-8")).hexdigest()


def _should_emit_event(fingerprint: str) -> bool:
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    current_timestamp = time.time()

    # 1. Clean old entries outside the sliding 60-second execution window
    while (
        _GLOBAL_NOTIFICATION_HISTORY
        and _GLOBAL_NOTIFICATION_HISTORY[0] < current_timestamp - 60
    ):
        _GLOBAL_NOTIFICATION_HISTORY.popleft()

    # 2. Strict Rate Guard Capacity check
    if len(_GLOBAL_NOTIFICATION_HISTORY) >= _MAX_NOTIFICATIONS_PER_MINUTE:
        print(f"⚠️ [RATE LIMIT ENGAGED] Coalescing alert drop to prevent channel spam.")
        return False

    # 3. Text String Standard Fingerprint Deduplication check
    last_sent = _NOTIFICATION_FINGERPRINT_LAST_SENT.get(fingerprint)
    if last_sent is not None:
        elapsed = (now_utc - last_sent).total_seconds()
        if elapsed < _NOTIFICATION_DEDUPE_WINDOW_SECONDS:
            return False

    # Append confirmation tracking flags
    _NOTIFICATION_FINGERPRINT_LAST_SENT[fingerprint] = now_utc
    _GLOBAL_NOTIFICATION_HISTORY.append(current_timestamp)
    return True


# Example of the structured format used across the codebase (from services/telemetry.py):
def _format_executive_summary_card(event: ExecutiveNotificationEvent) -> str:
    severity_emoji = _event_severity_to_emoji(event.severity)
    lines = [
        f"{severity_emoji} *{event.headline}*",
        f"• Agent: `{event.source_agent}`",
        f"• Outcome: {event.outcome}",
    ]
    if event.impact:
        lines.append(f"• Impact: {event.impact}")
    if event.next_action:
        lines.append(f"• Next: {event.next_action}")
    return "\n".join(lines)


def _is_immediate_alert(event: ExecutiveNotificationEvent) -> bool:
    severity = (event.severity or "info").lower()
    if severity in {"critical", "error"}:
        return True
    return event.event_type in _IMMEDIATE_ALERT_EVENT_TYPES


def emit_executive_notification(event: ExecutiveNotificationEvent) -> None:
    fingerprint = _event_fingerprint(event)
    if not _should_emit_event(fingerprint):
        return

    notification_channel = _event_severity_to_channel(event.severity)
    if not _is_immediate_alert(event):
        # Event-driven executive summaries still route through orchestration channel
        notification_channel = "orchestration"

    send_telegram_broadcast(
        _format_executive_summary_card(event),
        notification_channel,
    )


async def send_executive_summary_from_logs(
    db_pool,
    lookback_minutes: int = 180,
    ai_client=None,  # Pass the app.state.ai_client here
) -> None:
    """Builds an AI-powered executive summary directly from Helix log telemetry."""
    try:
        async with db_pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    """
                    SELECT agent_name, log_level, message, metadata, timestamp
                    FROM agent_system_logs
                    WHERE timestamp >= NOW() - (%s || ' minutes')::INTERVAL
                    ORDER BY timestamp DESC;
                    """,
                    (lookback_minutes,),
                )
                rows = await cur.fetchall()

        if not rows:
            emit_executive_notification(
                ExecutiveNotificationEvent(
                    source_agent="helix",
                    event_type="manual_command_ack",
                    severity="info",
                    headline="Helix Executive Summary",
                    outcome="No agent activity detected in the selected window.",
                    next_action="No immediate action required.",
                )
            )
            return

        # Prepare clear data for Claude to digest
        compact_logs = [
            {
                "time": r["timestamp"].strftime("%H:%M:%S"),
                "agent": r["agent_name"],
                "level": r["log_level"],
                "msg": r["message"],
                "meta": r["metadata"],
            }
            for r in rows[:40]  # Analyze the 40 most recent log milestones
        ]

        # Check the global safety gate before proceeding with Claude
        anthropic_healthy = await is_vendor_circuit_healthy(db_pool, "Anthropic")

        # FIX 2: Check for AI Client availability AND circuit health
        if ai_client and anthropic_healthy:
            prompt = (
                f"You are the Helix Executive Analytics Engine. Analyze the following recent system logs "
                f"from the last {lookback_minutes} minutes and provide an executive summary scorecard. "
                f"Group findings by agent actions. Use markdown lists and specific emojis. Keep it brief, "
                f"highly professional, scannable, and omit mundane technical details.\n\n"
                f"LOGS:\n{json.dumps(compact_logs, indent=2)}"
            )

            response = ai_client.messages.create(
                model="claude-haiku-4-5",  # <-- Downgraded from 'claude-3-5-sonnet-20241022' to protect your budget
                max_tokens=1000,
                temperature=0.3,
                messages=[{"role": "user", "content": prompt}],
            )
            ai_summary = response.content[0].text

            # Send via our broadcast engine directly
            send_telegram_broadcast(ai_summary, "orchestration")
        else:  # Fallback to structural metrics block if AI client isn't available or circuit is broken
            if not anthropic_healthy:
                print(
                    "🛑 [CIRCUIT BREAKER] Anthropic is currently tripped in ledger. Routing to local metrics."
                )

            # Fallback to structural metrics block if AI client isn't available
            summary_lines = []
            for agent in set(r["agent_name"] for r in rows):
                total = sum(1 for r in rows if r["agent_name"] == agent)
                errors = sum(
                    1
                    for r in rows
                    if r["agent_name"] == agent and r["log_level"] == "ERROR"
                )
                marker = "🚨" if errors > 0 else "✅"
                summary_lines.append(
                    f"{marker} `{agent}`: {total} events ({errors} errors)"
                )

            emit_executive_notification(
                ExecutiveNotificationEvent(
                    source_agent="helix",
                    event_type="manual_command_ack",
                    severity="info",
                    headline="Helix Executive Metrics (Fallback)",
                    outcome="; ".join(summary_lines),
                    next_action="Review log database directly.",
                )
            )
    except Exception as exc:
        emit_executive_notification(
            ExecutiveNotificationEvent(
                source_agent="helix",
                event_type="critical_failure",
                severity="critical",
                headline="AI Executive Summary Build Failed",
                outcome="Helix could not process log data arrays into an executive summary.",
                impact=str(exc),
                next_action="Validate database connectivity and Anthropic API key status.",
            )
        )


async def write_helix_command_center_log(
    db_pool,
    log_level: str,
    message: str,
    action_taken: str,
    target_niche: str = "System Core",
    extra_metadata: Optional[dict] = None,
) -> None:
    metadata_payload = {
        "command_center_view": {
            "action_taken": action_taken,
            "target_niche": target_niche,
            "execution_timestamp": get_new_york_now().isoformat(),
        }
    }
    if extra_metadata:
        metadata_payload.update(extra_metadata)

    try:
        async with db_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "INSERT INTO agent_system_logs (agent_name, log_level, message, metadata) VALUES ('helix', %s, %s, %s);",
                    (log_level, message, Jsonb(metadata_payload)),
                )
            await conn.commit()
    except Exception as e:
        print(f"⚠️ Failed to write command center log: {e}")


async def log_helix_state(
    cur, strategic_mode: str, geo_lock: str, buying_signal: str
) -> None:
    metadata_payload = {
        "strategic_mode": strategic_mode,
        "geo_lock": geo_lock,
        "buying_signal": buying_signal,
    }
    await cur.execute(
        "INSERT INTO agent_system_logs (agent_name, log_level, message, metadata) VALUES ('helix', 'INFO', 'Global system parameters synchronized.', %s);",
        (Jsonb(metadata_payload),),
    )


# Lead density thresholds & closed loops
async def evaluate_lead_density_and_escalate(
    industry_name: str, influx_count: int, db_pool, background_tasks, app_state
):
    if not industry_name or industry_name == "Unassigned":
        return

    try:
        async with db_pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                boost_weight = min(influx_count // 5 + 1, 10)
                await cur.execute(
                    """
                    UPDATE helix_brain_scheduler 
                    SET priority_score = priority_score + %s, is_active = TRUE 
                    WHERE industry_name = %s
                    RETURNING industry_id, industry_name, moltset_category, bottleneck_profile, pain_tokens, target_region, priority_score;
                    """,
                    (boost_weight, industry_name),
                )
                updated_strategy = await cur.fetchone()
                await conn.commit()

        if updated_strategy:
            emit_executive_notification(
                ExecutiveNotificationEvent(
                    source_agent="helix",
                    event_type="pipeline_milestone",
                    severity="info",
                    headline="Lead Ingestion Spike",
                    outcome=f"{industry_name} received {influx_count} leads.",
                    impact=f"Priority boosted to {updated_strategy['priority_score']}.",
                    next_action="Monitor follow-on mesh dispatch readiness.",
                )
            )

            if influx_count >= 10:
                emit_executive_notification(
                    ExecutiveNotificationEvent(
                        source_agent="helix",
                        event_type="pipeline_milestone",
                        severity="success",
                        headline="High Density Trigger",
                        outcome=f"{industry_name} crossed high-volume threshold.",
                        impact="Proxy task fanout initiated.",
                        next_action="Track completion of dispatched agents.",
                    )
                )
                from core.engine import load_vendor_isolation_matrix

                vendor_isolation_matrix = await load_vendor_isolation_matrix(db_pool)

                background_tasks.add_task(
                    trigger_downstream_mesh_with_payload,
                    db_pool,
                    app_state,
                    updated_strategy,
                    ["nexus", "beacon", "nova", "vanguard", "pulsar"],
                    vendor_isolation_matrix,
                )
    except Exception as e:
        print(f"⚠️ Adaptive intake trigger anomaly: {e}")


async def process_roi_attribution_boost(email: str, status: str, db_pool):
    normalized_status = status.lower().strip()
    reward_weight = 0
    if "reply" in normalized_status:
        reward_weight = 15
    elif "meeting" in normalized_status or "booked" in normalized_status:
        reward_weight = 40
    elif "won" in normalized_status or "converted" in normalized_status:
        reward_weight = 100

    if reward_weight == 0:
        return

    try:
        async with db_pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    "SELECT industry FROM cold_leads WHERE email = %s LIMIT 1;",
                    (email,),
                )
                lead_row = await cur.fetchone()

                if lead_row and lead_row["industry"]:
                    target_industry = lead_row["industry"]
                    await cur.execute(
                        """
                        UPDATE helix_brain_scheduler SET priority_score = priority_score + %s, is_active = TRUE
                        WHERE industry_name = %s RETURNING industry_name, priority_score;
                        """,
                        (reward_weight, target_industry),
                    )
                    updated_row = await cur.fetchone()
                    await conn.commit()

                    if updated_row:
                        emit_executive_notification(
                            ExecutiveNotificationEvent(
                                source_agent="helix",
                                event_type="pipeline_milestone",
                                severity="success",
                                headline="ROI Attribution Boost Applied",
                                outcome=f"Lead milestone '{status.upper()}' mapped to {target_industry}.",
                                impact=f"Priority increased by +{reward_weight}.",
                                next_action="Continue monitoring conversion progression.",
                            )
                        )
    except Exception as e:
        print(f"⚠️ Attribution processing anomaly: {e}")


# PhantomBuster policy triggers
def is_phantombuster_zero_result_signal(message: str, metadata: dict[str, Any]) -> bool:
    combined = f"{message} {json.dumps(metadata)}".lower()
    if "phantombuster" not in combined:
        return False
    if any(token in combined for token in ("0 results", "zero results", "no results")):
        return True
    results_count = metadata.get("results_count")
    return isinstance(results_count, int) and results_count == 0


def apply_phantombuster_zero_result_policy(
    app_state, industry_name: str | None
) -> dict[str, Any] | None:
    if not industry_name:
        return None
    key = industry_name.lower().strip()
    if not key:
        return None

    remediation_state = getattr(app_state, "phantombuster_remediation", {})
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    entry = remediation_state.get(
        key,
        {
            "state": "healthy",
            "zero_streak": 0,
            "keyword_cap": 25,
            "cooldown_until": None,
        },
    )

    cooldown_until = entry.get("cooldown_until")
    if isinstance(cooldown_until, datetime.datetime) and now_utc < cooldown_until:
        return {
            "state": entry.get("state", "watch"),
            "industry": industry_name,
            "keyword_cap": entry.get("keyword_cap", 25),
            "cooldown_active": True,
        }

    entry["zero_streak"] = int(entry.get("zero_streak", 0)) + 1
    current_cap = int(entry.get("keyword_cap", 25))

    if entry["zero_streak"] < 2:
        entry["state"] = "watch"
        remediation_state[key] = entry
        return {
            "state": "watch",
            "industry": industry_name,
            "keyword_cap": current_cap,
            "zero_streak": entry["zero_streak"],
            "cooldown_active": False,
        }

    next_cap = current_cap
    for stage_cap in [25, 12, 8, 5]:
        if stage_cap < current_cap:
            next_cap = stage_cap
            break

    if next_cap < current_cap:
        entry["state"] = "constrained"
        entry["keyword_cap"] = next_cap
        entry["zero_streak"] = 0
        entry["cooldown_until"] = now_utc + datetime.timedelta(minutes=60)
    else:
        entry["state"] = "isolated"
        entry["keyword_cap"] = 5
        entry["zero_streak"] = 0
        entry["cooldown_until"] = now_utc + datetime.timedelta(minutes=60)

    remediation_state[key] = entry
    return {
        "state": entry["state"],
        "industry": industry_name,
        "keyword_cap": entry["keyword_cap"],
    }


def reset_phantombuster_policy_on_success(app_state, industry_name: str | None) -> None:
    if not industry_name:
        return
    remediation_state = getattr(app_state, "phantombuster_remediation", None)
    if not remediation_state:
        return
    key = industry_name.lower().strip()
    if key in remediation_state:
        remediation_state[key] = {
            "state": "healthy",
            "zero_streak": 0,
            "keyword_cap": remediation_state[key].get("keyword_cap", 25),
            "cooldown_until": None,
        }


# The Core Self-Healing Oracle Analysis Loop
async def dispatch_chiron_heal_request(
    db_pool,
    log_id: int,
    agent_name: str,
    error_msg: str,
    traceback_str: str,
    metadata: dict,
) -> None:
    helix_token = os.getenv("HELIX_AUTH_TOKEN", "")
    if not helix_token:
        return

    try:
        async with db_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "INSERT INTO chiron_healing_sessions (triggering_log_id, target_agent, error_message, full_traceback, status) VALUES (%s, %s, %s, %s, 'triggered') ON CONFLICT DO NOTHING;",
                    (log_id, agent_name, error_msg, traceback_str),
                )
            await conn.commit()

        async with httpx.AsyncClient(timeout=15.0) as client:
            await client.post(
                f"{CHIRON_API_URL}/api/v1/chiron/heal",
                headers={
                    "X-Chiron-Key": helix_token,
                    "Content-Type": "application/json",
                },
                json={
                    "log_id": log_id,
                    "target_agent": agent_name,
                    "error_message": error_msg[:500],
                    "full_traceback": traceback_str,
                    "metadata": metadata,
                },
            )
    except Exception as exc:
        print(f"💥 Failed to dispatch self-healing request: {exc}")


async def analyze_and_orchestrate_log(
    db_pool,
    app_state,
    log_id: int,
    agent_name: str,
    log_level: str,
    message: str,
    metadata: dict | None,
) -> None:
    norm_msg = message.lower()
    normalized_log_level = str(log_level or "INFO").upper()
    meta = metadata if isinstance(metadata, dict) else {}
    industry = meta.get("industry_name") or meta.get("industry")
    meta_string_dump = json.dumps(meta).lower()
    extended_error_context = f"{norm_msg} {meta_string_dump}"

    try:
        async with db_pool.connection() as conn:
            async with conn.cursor() as cur:
                if normalized_log_level == "INFO":
                    # success check
                    if "execution status: success" in norm_msg:
                        emit_executive_notification(
                            ExecutiveNotificationEvent(
                                source_agent=agent_name,
                                event_type="pipeline_milestone",
                                severity="success",
                                headline="Agent Execution Success",
                                outcome=f"{agent_name} reported successful execution.",
                                next_action="Monitor downstream dependencies for completion.",
                            )
                        )
                        if agent_name in ["beacon", "nova"]:
                            master_token = os.getenv("HELIX_AUTH_TOKEN", "")
                            validation_payload = {
                                "agent_validated": agent_name,
                                "industry_name": (
                                    industry if industry else "General Operations"
                                ),
                                "target_region": meta.get("target_region")
                                or meta.get("region")
                                or "National",
                            }
                            asyncio.create_task(
                                async_http_post_proxy(
                                    f"{os.getenv('NEXUS_NERVOUS_IMPULSE_URL')}/api/v1/nexus/validate",
                                    {
                                        "X-Nexus-Auth-Token": master_token,
                                        "Content-Type": "application/json",
                                    },
                                    validation_payload,
                                )
                            )

                    # nova feed loops
                    if agent_name == "nova" and (
                        "finished batch run" in norm_msg
                        or "successfully processed batch" in norm_msg
                    ):
                        records_found = int(meta.get("records_found", 0))
                        status_flag = str(meta.get("status", "")).upper()

                        if status_flag == "PROVIDER_DOWN_RETRY":
                            emit_executive_notification(
                                ExecutiveNotificationEvent(
                                    source_agent="nova",
                                    event_type="pipeline_milestone",
                                    severity="warning",
                                    headline="Nova Provider Retry",
                                    outcome=f"Provider drop detected for {industry}; retry path active.",
                                    next_action="Watch for recovery or escalation.",
                                )
                            )
                            await cur.execute(
                                "UPDATE helix_brain_scheduler SET last_targeted_at = NOW() - (minimum_cooldown_hours || ' hours')::INTERVAL + INTERVAL '1 hour' WHERE industry_name = %s;",
                                (industry,),
                            )
                            await conn.commit()
                            return
                        elif status_flag == "RATE_LIMITED":
                            emit_executive_notification(
                                ExecutiveNotificationEvent(
                                    source_agent="nova",
                                    event_type="vendor_circuit_breaker",
                                    severity="critical",
                                    headline="Nova Rate Limited",
                                    outcome=f"Sequential queue frozen for {industry}.",
                                    next_action="Increase quota or delay rerun.",
                                )
                            )
                            await cur.execute(
                                "UPDATE helix_brain_scheduler SET last_targeted_at = NOW() - (minimum_cooldown_hours || ' hours')::INTERVAL WHERE industry_name = %s;",
                                (industry,),
                            )
                            await conn.commit()
                            return
                        elif status_flag == "EXHAUSTED" or (
                            status_flag == "COMPLETED" and records_found == 0
                        ):
                            emit_executive_notification(
                                ExecutiveNotificationEvent(
                                    source_agent="nova",
                                    event_type="critical_failure",
                                    severity="critical",
                                    headline="Campaign Exhaustion Guard Triggered",
                                    outcome=f"{industry} campaign has been deactivated.",
                                    next_action="Rotate strategy target before rerun.",
                                )
                            )
                            await cur.execute(
                                "UPDATE helix_brain_scheduler SET is_active = FALSE WHERE industry_name = %s;",
                                (industry,),
                            )
                            await conn.commit()
                            return
                        elif status_flag == "COMPLETED" and records_found > 0:
                            emit_executive_notification(
                                ExecutiveNotificationEvent(
                                    source_agent="nova",
                                    event_type="pipeline_milestone",
                                    severity="success",
                                    headline="Nova Batch Completed",
                                    outcome=f"Secured {records_found} leads for {industry}.",
                                    next_action="Continue sequential enrichment pipeline.",
                                )
                            )
                            from core.engine import load_vendor_isolation_matrix

                            vendor_isolation_matrix = (
                                await load_vendor_isolation_matrix(db_pool)
                            )

                            async with conn.cursor(row_factory=dict_row) as cascade_cur:
                                await cascade_cur.execute("""
                                    SELECT industry_id, industry_name, moltset_category, bottleneck_profile, pain_tokens, target_region
                                    FROM helix_brain_scheduler WHERE is_active = TRUE AND (last_targeted_at IS NULL OR last_targeted_at <= NOW() - (minimum_cooldown_hours || ' hours')::INTERVAL)
                                    ORDER BY priority_score DESC, last_targeted_at ASC NULLS FIRST LIMIT 1;
                                """)
                                next_strategy = await cascade_cur.fetchone()
                                if next_strategy:
                                    await cascade_cur.execute(
                                        "UPDATE helix_brain_scheduler SET last_targeted_at = NOW() WHERE industry_id = %s;",
                                        (next_strategy["industry_id"],),
                                    )
                                    asyncio.create_task(
                                        trigger_downstream_mesh_with_payload(
                                            db_pool,
                                            app_state,
                                            next_strategy,
                                            ["nova"],
                                            vendor_isolation_matrix,
                                        )
                                    )

                    # Vanguard
                    if (
                        agent_name == "vanguard"
                        and "campaign sweep completed" in norm_msg
                    ):
                        leads_processed = int(
                            meta.get("leads_sent", 0) or meta.get("records_found", 0)
                        )
                        await cur.execute(
                            "INSERT INTO agent_system_logs (agent_name, log_level, message, metadata) VALUES ('helix', 'INFO', %s, %s);",
                            (
                                f"Vanguard processed {leads_processed} outbound messages.",
                                Jsonb(
                                    {
                                        "command_center_view": {
                                            "action_taken": "VANGUARD_SWEEP_SUCCESS",
                                            "target_niche": industry
                                            or "Staging Reservoir",
                                            "execution_timestamp": get_new_york_now().isoformat(),
                                        }
                                    }
                                ),
                            ),
                        )
                        emit_executive_notification(
                            ExecutiveNotificationEvent(
                                source_agent="vanguard",
                                event_type="pipeline_milestone",
                                severity="success",
                                headline="Vanguard Sweep Completed",
                                outcome=f"Verified outbound processing for {leads_processed} leads.",
                                next_action="Track reply and meeting conversion signals.",
                            )
                        )

                    # Vector
                    if (
                        agent_name == "vector"
                        and "vector batch validation successful" in norm_msg
                    ):
                        from core.engine import load_vendor_isolation_matrix

                        vendor_isolation_matrix = await load_vendor_isolation_matrix(
                            db_pool
                        )
                        async with conn.cursor(row_factory=dict_row) as cascade_cur:
                            await cascade_cur.execute(
                                "SELECT industry_id, industry_name, moltset_category, bottleneck_profile, pain_tokens, target_region FROM helix_brain_scheduler WHERE is_active = TRUE ORDER BY priority_score DESC, last_targeted_at ASC NULLS FIRST LIMIT 1;"
                            )
                            active_strategy = await cascade_cur.fetchone()
                        if active_strategy:
                            asyncio.create_task(
                                trigger_downstream_mesh_with_payload(
                                    db_pool,
                                    app_state,
                                    active_strategy,
                                    ["vanguard"],
                                    vendor_isolation_matrix,
                                )
                            )

                    if "phantombuster" in extended_error_context:
                        if is_phantombuster_zero_result_signal(message, meta):
                            apply_phantombuster_zero_result_policy(app_state, industry)
                        else:
                            reset_phantombuster_policy_on_success(app_state, industry)

                if normalized_log_level == "ERROR":
                    if any(
                        t in extended_error_context
                        for t in (
                            "phantombuster",
                            "credit limit",
                            "exhausted",
                            "api_key_expired",
                        )
                    ):
                        await cur.execute(
                            "UPDATE helix_infrastructure_oracle_ledger SET is_circuit_broken = TRUE, credits_remaining = 0, last_updated_at = NOW() WHERE vendor_name = 'PhantomBusters';"
                        )
                        if industry:
                            await cur.execute(
                                "UPDATE helix_brain_scheduler SET is_active = FALSE WHERE industry_name = %s;",
                                (industry,),
                            )
                        await conn.commit()
                        return

                    if any(
                        t in extended_error_context
                        for t in (
                            "myemailverifier",
                            "insufficient credits",
                            "out of tokens",
                        )
                    ):
                        await cur.execute(
                            "UPDATE helix_infrastructure_oracle_ledger SET is_circuit_broken = TRUE, credits_remaining = 0, last_updated_at = NOW() WHERE vendor_name = 'MyEmailVerifier';"
                        )
                        await conn.commit()
                        return

                    traceback_str = meta.get("traceback") or meta.get("traceback_str")
                    if not traceback_str and "traceback" in norm_msg:
                        traceback_str = message

                    if traceback_str:
                        asyncio.create_task(
                            dispatch_chiron_heal_request(
                                db_pool,
                                log_id,
                                agent_name,
                                message,
                                traceback_str,
                                meta,
                            )
                        )

                    if industry:
                        await cur.execute(
                            "SELECT industry_id, industry_name, moltset_category, bottleneck_profile, pain_tokens, target_region FROM helix_brain_scheduler WHERE industry_name = %s LIMIT 1;",
                            (industry,),
                        )
                        strategy = await cur.fetchone()
                        if strategy and agent_name in ["nexus", "nova"]:
                            master_token = os.getenv("HELIX_AUTH_TOKEN", "")
                            if agent_name == "nexus":
                                asyncio.create_task(
                                    async_http_post_proxy(
                                        f"{os.getenv('NEXUS_NERVOUS_IMPULSE_URL')}/api/v1/nexus/awaken",
                                        {
                                            "X-Nexus-Auth-Token": master_token,
                                            "Content-Type": "application/json",
                                        },
                                        {
                                            "trigger_reason": "helix_auto_rescue_escalation",
                                            "batch_size": 15,
                                            "rescue_mode": True,
                                            **strategy,
                                        },
                                    )
                                )
                            elif agent_name == "nova":
                                asyncio.create_task(
                                    async_http_post_proxy(
                                        f"{os.getenv('NOVA_API_URL')}/api/v1/nova/ignite",
                                        {
                                            "Authorization": f"Bearer {master_token}",
                                            "Content-Type": "application/json",
                                        },
                                        {
                                            "target_region": strategy["target_region"],
                                            "daily_quota_limit": 150,
                                            "target_company_limit": 35,
                                            "industries": [
                                                {
                                                    "industry_id": strategy[
                                                        "industry_id"
                                                    ],
                                                    "industry_name": strategy[
                                                        "industry_name"
                                                    ],
                                                    "moltset_category": strategy[
                                                        "moltset_category"
                                                    ],
                                                    "bottleneck_profile": strategy[
                                                        "bottleneck_profile"
                                                    ],
                                                }
                                            ],
                                        },
                                    )
                                )
            await conn.commit()
    except Exception as exc:
        print(f"Background healer exception: {exc}")


async def async_http_post_proxy(url: str, headers: dict, json_data: dict) -> bool:
    # Basic lightweight wrapper to avoid circular reference dependencies
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            res = await client.post(url, headers=headers, json=json_data)
            return 200 <= res.status_code < 300
    except Exception:
        return False
