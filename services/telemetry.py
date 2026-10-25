import os
import json
import asyncio
import traceback
import hashlib
import re
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
from helix_memory import record_experience
from services.outcomes import record_outcome_event, stage_from_status

# Rate limiting windows
_GLOBAL_NOTIFICATION_HISTORY = deque()
_MAX_NOTIFICATIONS_PER_MINUTE = (
    6  # Caps output strictly to roughly 1 alert per 10 seconds
)
_NOTIFICATION_COUNTERS: dict[str, int] = {
    "immediate_sent": 0,
    "summary_sent": 0,
    "dedupe_suppressed": 0,
    "cooldown_suppressed": 0,
    "rate_suppressed": 0,
    "policy_suppressed": 0,
}


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
_CRITICAL_REPEAT_COOLDOWN_SECONDS = int(
    os.getenv("HELIX_CRITICAL_REPEAT_COOLDOWN_SECONDS", "900")
)
_NOTIFICATION_POLICY_MODE = (
    os.getenv("HELIX_NOTIFICATION_POLICY_MODE", "strict").strip().lower() or "strict"
)


def get_new_york_now() -> datetime.datetime:
    return datetime.datetime.now(NEW_YORK_TZ)


async def is_vendor_circuit_healthy(db_pool, vendor_name: str) -> bool:
    """Checks the infrastructure ledger to see if a vendor's circuit is broken."""
    normalized_vendor_name = str(vendor_name or "").strip()
    if not normalized_vendor_name:
        print("Circuit ledger lookup blocked because vendor_name is empty.")
        return False

    try:
        async with db_pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    """
                    SELECT is_circuit_broken 
                    FROM helix_infrastructure_oracle_ledger 
                    WHERE LOWER(vendor_name) = LOWER(%s) LIMIT 1;
                    """,
                    (normalized_vendor_name,),
                )
                record = await cur.fetchone()
                if not record or not isinstance(record, dict):
                    print(
                        f"Circuit ledger lookup blocked for {normalized_vendor_name}: no valid ledger state."
                    )
                    return False

                is_circuit_broken = record.get("is_circuit_broken")
                if not isinstance(is_circuit_broken, bool):
                    print(
                        f"Circuit ledger lookup blocked for {normalized_vendor_name}: invalid circuit state."
                    )
                    return False
                return not is_circuit_broken
    except Exception as e:
        print(f"Circuit ledger lookup failed closed for {normalized_vendor_name}: {e}")
        return False


# Telegram and Command Logging
def _resolve_telegram_destination(notification_type: str) -> tuple[str, str]:
    if notification_type == "critical":
        token = os.getenv("TELEGRAM_CRITICAL_BOT_TOKEN") or os.getenv(
            "TELEGRAM_BOT_TOKEN"
        )
        chat_id = os.getenv("TELEGRAM_CRITICAL_CHAT_ID") or os.getenv(
            "TELEGRAM_CHAT_ID"
        )
        return token or "", chat_id or ""

    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_SUMMARY_CHAT_ID") or os.getenv("TELEGRAM_CHAT_ID")
    return token or "", chat_id or ""


def send_telegram_broadcast(message: str, notification_type: str = "verbose"):
    token, chat_id = _resolve_telegram_destination(notification_type)
    if not token or not chat_id:
        return

    prefixes = {
        "critical": "🚨 <b>[CRITICAL INFRASTRUCTURE]</b>",
        "success": "✅ <b>[PIPELINE SUCCESS]</b>",
        "orchestration": "🧠 <b>[HELIX ORCHESTRATOR]</b>",
        "verbose": "📡 <b>[TELEMETRY]</b>",
    }
    prefix = prefixes.get(notification_type, "📡")
    extra_details = ""

    if notification_type == "critical":
        tb_str = traceback.format_exc()
        if tb_str and "NoneType: None" not in tb_str:
            # Escape HTML characters in raw tracebacks
            clean_tb = (
                tb_str[-1500:]
                .replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
            )
            extra_details = f'\n\n<b>🔍 Traceback:</b>\n<pre><code class="language-python">{clean_tb}</code></pre>'

    formatted_message = f"{prefix}\n{message}{extra_details}"

    async def _send() -> None:
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.post(
                    f"https://api.telegram.org/bot{token}/sendMessage",
                    json={
                        "chat_id": chat_id,
                        "text": formatted_message,
                        "parse_mode": "HTML",  # Switch to HTML parse mode
                    },
                )
                if response.status_code != 200:
                    print(
                        f"⚠️ Telegram broadcast rejected with HTTP {response.status_code}: {response.text[:300]}"
                    )
        except Exception as e:
            print(f"⚠️ Telegram broadcast async failed: {e}")

    try:
        loop = asyncio.get_running_loop()
        loop.create_task(_send())
    except RuntimeError:
        try:
            response = requests.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={
                    "chat_id": chat_id,
                    "text": formatted_message,
                    "parse_mode": "HTML",  # Switch to HTML parse mode
                },
                timeout=5,
            )
            if response.status_code != 200:
                print(
                    f"⚠️ Telegram broadcast rejected with HTTP {response.status_code}: {response.text[:300]}"
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


def _should_emit_event(fingerprint: str, immediate: bool) -> bool:
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
        _NOTIFICATION_COUNTERS["rate_suppressed"] += 1
        return False

    # 3. Text String Standard Fingerprint Deduplication check
    last_sent = _NOTIFICATION_FINGERPRINT_LAST_SENT.get(fingerprint)
    if last_sent is not None:
        elapsed = (now_utc - last_sent).total_seconds()
        if elapsed < _NOTIFICATION_DEDUPE_WINDOW_SECONDS:
            _NOTIFICATION_COUNTERS["dedupe_suppressed"] += 1
            return False

    if immediate and last_sent is not None:
        elapsed = (now_utc - last_sent).total_seconds()
        if elapsed < _CRITICAL_REPEAT_COOLDOWN_SECONDS:
            _NOTIFICATION_COUNTERS["cooldown_suppressed"] += 1
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
    return severity in {"critical", "error"}


def _is_command_event(event: ExecutiveNotificationEvent) -> bool:
    return (event.event_type or "").lower() in {"manual_command_ack", "strategy_review"}


def get_notification_quiet_score() -> dict[str, int]:
    return dict(_NOTIFICATION_COUNTERS)


def _escape_html(value: str) -> str:
    return (
        str(value or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    )


def _is_material_log(level: str, message: str) -> bool:
    normalized_level = (level or "").upper()
    normalized_message = (message or "").lower()
    if normalized_level in {"CRITICAL", "ERROR", "WARNING"}:
        return True
    material_markers = (
        "circuit",
        "defer",
        "budget",
        "quota",
        "failed",
        "stalled",
        "recovered",
        "tripped",
        "exhaust",
        "isolation",
    )
    return any(marker in normalized_message for marker in material_markers)


def _looks_resolved(message: str) -> bool:
    normalized = (message or "").lower()
    return any(
        marker in normalized
        for marker in ("recovered", "restored", "cleared", "resolved", "healthy")
    )


def _looks_constraint(message: str) -> bool:
    normalized = (message or "").lower()
    return any(
        marker in normalized
        for marker in (
            "circuit",
            "defer",
            "budget",
            "quota",
            "isolation",
            "tripped",
            "exhaust",
        )
    )


def _shorten_message(message: str, max_len: int = 120) -> str:
    raw = " ".join(str(message or "").split())
    if len(raw) <= max_len:
        return raw
    return raw[: max_len - 3].rstrip() + "..."


def _render_executive_delta_summary(
    *,
    summary_label: str,
    window_start: datetime.datetime,
    window_end: datetime.datetime,
    previous_summary_at: datetime.datetime | None,
    rows: list[dict],
) -> str:
    quiet_score = get_notification_quiet_score()
    comparison_start = previous_summary_at or window_start
    delta_rows = [
        row
        for row in rows
        if isinstance(row.get("timestamp"), datetime.datetime)
        and row["timestamp"] >= comparison_start
    ]

    material_delta = [
        row
        for row in delta_rows
        if _is_material_log(row.get("log_level", ""), row.get("message", ""))
    ]
    resolved_delta = [
        row for row in material_delta if _looks_resolved(str(row.get("message") or ""))
    ]

    unique_changes: list[dict] = []
    seen = set()
    for row in material_delta:
        key = (
            str(row.get("agent_name") or "").lower(),
            str(row.get("log_level") or "").upper(),
            _shorten_message(str(row.get("message") or ""), 90).lower(),
        )
        if key in seen:
            continue
        seen.add(key)
        unique_changes.append(row)

    unique_resolved: list[dict] = []
    seen_resolved = set()
    for row in resolved_delta:
        key = (
            str(row.get("agent_name") or "").lower(),
            _shorten_message(str(row.get("message") or ""), 90).lower(),
        )
        if key in seen_resolved:
            continue
        seen_resolved.add(key)
        unique_resolved.append(row)

    active_constraints = []
    seen_constraints = set()
    for row in rows:
        message = str(row.get("message") or "")
        if not _looks_constraint(message):
            continue
        constraint_key = (
            str(row.get("agent_name") or "").lower(),
            _shorten_message(message, 100).lower(),
        )
        if constraint_key in seen_constraints:
            continue
        seen_constraints.add(constraint_key)
        active_constraints.append(row)
        if len(active_constraints) >= 2:
            break

    critical_count = sum(
        1 for row in rows if str(row.get("log_level") or "").upper() == "CRITICAL"
    )
    error_count = sum(
        1 for row in rows if str(row.get("log_level") or "").upper() == "ERROR"
    )
    unique_agents = len(
        {
            str(row.get("agent_name") or "").lower()
            for row in rows
            if str(row.get("agent_name") or "").strip()
        }
    )

    health = (
        "CRITICAL"
        if critical_count > 0
        else "DEGRADED" if error_count > 0 else "HEALTHY"
    )
    if active_constraints:
        primary_driver = _shorten_message(
            str(active_constraints[0].get("message") or "Constraint active"),
            70,
        )
    else:
        primary_driver = "No active constraints detected"

    lines = [
        f"🧾 <b>HELIX EXECUTIVE DELTA</b> - <b>{_escape_html(summary_label)}</b>",
        (
            f"Window: {_escape_html(window_start.astimezone(NEW_YORK_TZ).strftime('%H:%M'))}"
            f" -> {_escape_html(window_end.astimezone(NEW_YORK_TZ).strftime('%H:%M'))} ET"
        ),
        (
            f"Since Last Summary: {len(unique_changes)} new | "
            f"{len(unique_resolved)} resolved | "
            f"{quiet_score['policy_suppressed']} unchanged suppressed"
        ),
        "",
        "<b>1) Status</b>",
        f"System: <b>{health}</b> | Primary Driver: {_escape_html(primary_driver)}",
        "",
        "<b>2) New Since Last Summary</b>",
    ]

    if unique_changes:
        for row in unique_changes[:3]:
            sev = str(row.get("log_level") or "INFO").upper()
            agent = _escape_html(str(row.get("agent_name") or "helix"))
            message = _escape_html(_shorten_message(str(row.get("message") or ""), 110))
            lines.append(f"- {sev} {agent}: {message}")
    else:
        lines.append("- No new material changes in this window.")

    lines.extend(["", "<b>3) Resolved Since Last Summary</b>"])
    if unique_resolved:
        for row in unique_resolved[:2]:
            agent = _escape_html(str(row.get("agent_name") or "helix"))
            message = _escape_html(_shorten_message(str(row.get("message") or ""), 90))
            lines.append(f"- {agent}: {message}")
    else:
        lines.append("- None")

    lines.extend(["", "<b>4) Active Constraints</b>"])
    if active_constraints:
        for row in active_constraints:
            agent = _escape_html(str(row.get("agent_name") or "helix"))
            message = _escape_html(_shorten_message(str(row.get("message") or ""), 100))
            lines.append(f"- {agent}: {message}")
    else:
        lines.append("- None")

    lines.extend(
        [
            "",
            "<b>5) Business Impact</b>",
            f"- Throughput signal: {len(rows)} logged events across {unique_agents} agents",
            f"- Reliability signal: CRITICAL={critical_count}, ERROR={error_count}",
            "",
            "<b>6) Noise Control Snapshot</b>",
            (
                f"Immediate: {quiet_score['immediate_sent']} | "
                f"Summaries: {quiet_score['summary_sent']} | "
                f"Suppressed: {quiet_score['policy_suppressed']} "
                f"(dedupe={quiet_score['dedupe_suppressed']}, "
                f"cooldown={quiet_score['cooldown_suppressed']}, "
                f"rate={quiet_score['rate_suppressed']})"
            ),
        ]
    )

    return "\n".join(lines)


def emit_executive_notification(event: ExecutiveNotificationEvent) -> None:
    fingerprint = _event_fingerprint(event)
    immediate = _is_immediate_alert(event)
    if not _should_emit_event(fingerprint, immediate=immediate):
        return

    if not immediate:
        # Strict mode suppresses system-level non-critical events; only command replies are sent immediately.
        if _NOTIFICATION_POLICY_MODE != "legacy" and not _is_command_event(event):
            _NOTIFICATION_COUNTERS["policy_suppressed"] += 1
            return
        send_telegram_broadcast(_format_executive_summary_card(event), "orchestration")
        _NOTIFICATION_COUNTERS["summary_sent"] += 1
        return

    send_telegram_broadcast(
        _format_executive_summary_card(event),
        _event_severity_to_channel(event.severity),
    )
    _NOTIFICATION_COUNTERS["immediate_sent"] += 1


async def send_executive_summary_from_logs(
    db_pool,
    lookback_minutes: int = 180,
    ai_client=None,  # Pass the app.state.ai_client here
    window_start: datetime.datetime | None = None,
    window_end: datetime.datetime | None = None,
    summary_label: str = "Helix Executive Summary",
) -> None:
    """Build and send a concise executive delta summary from Helix log telemetry."""
    try:
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        resolved_window_end = window_end or now_utc
        resolved_window_start = window_start or (
            resolved_window_end - datetime.timedelta(minutes=max(1, lookback_minutes))
        )

        async with db_pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    """
                    SELECT MAX(timestamp) AS last_summary_at
                    FROM agent_system_logs
                    WHERE agent_name = 'helix'
                      AND metadata->'command_center_view'->>'action_taken' = 'HELIX_EXEC_SUMMARY_SENT'
                      AND timestamp < %s;
                    """,
                    (resolved_window_end,),
                )
                marker_row = await cur.fetchone()
                previous_summary_at = (
                    marker_row.get("last_summary_at") if marker_row else None
                )

                await cur.execute(
                    """
                    SELECT agent_name, log_level, message, metadata, timestamp
                    FROM agent_system_logs
                    WHERE timestamp >= %s AND timestamp < %s
                    ORDER BY timestamp DESC;
                    """,
                    (resolved_window_start, resolved_window_end),
                )
                rows = await cur.fetchall()

        if not rows:
            send_telegram_broadcast(
                (
                    f"🧾 <b>HELIX EXECUTIVE DELTA</b> - <b>{summary_label}</b>\n"
                    "No agent activity detected in the selected window."
                ),
                "orchestration",
            )
            _NOTIFICATION_COUNTERS["summary_sent"] += 1
            return

        summary_text = _render_executive_delta_summary(
            summary_label=summary_label,
            window_start=resolved_window_start,
            window_end=resolved_window_end,
            previous_summary_at=previous_summary_at,
            rows=rows,
        )
        send_telegram_broadcast(summary_text, "orchestration")
        _NOTIFICATION_COUNTERS["summary_sent"] += 1
        await write_helix_command_center_log(
            db_pool,
            "INFO",
            f"Executive delta summary sent for {summary_label}.",
            "HELIX_EXEC_SUMMARY_SENT",
            "helix-summary",
            {
                "summary_label": summary_label,
                "window_start_utc": resolved_window_start.isoformat(),
                "window_end_utc": resolved_window_end.isoformat(),
                "previous_summary_at_utc": (
                    previous_summary_at.isoformat()
                    if isinstance(previous_summary_at, datetime.datetime)
                    else None
                ),
            },
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
    normalized_level = str(log_level or "INFO").upper()
    if normalized_level not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
        print(
            f"[WARNING] [HELIX] Unsupported log level '{log_level}' normalized to WARNING."
        )
        normalized_level = "WARNING"
    traceback_text = (
        extra_metadata.get("traceback") if isinstance(extra_metadata, dict) else None
    )
    if normalized_level == "ERROR" and not (
        isinstance(traceback_text, str)
        and re.search(r'File "[^"]+", line \d+', traceback_text)
    ):
        normalized_level = "WARNING"
    print(f"[{normalized_level}] [HELIX] [{action_taken}] {message}")
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
                    (normalized_level, message, Jsonb(metadata_payload)),
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


async def process_roi_attribution_boost(
    email: str, status: str, db_pool, record_ledger: bool = True
):
    normalized_status = status.lower().strip()
    if record_ledger:
        ledger_stage = stage_from_status(normalized_status)
        if ledger_stage:
            await record_outcome_event(
                db_pool,
                lead_email=email,
                stage=ledger_stage,
                source="vanguard_status",
            )
    reward_weight = 0
    outcome_type = "conversion"

    if "reply" in normalized_status:
        reward_weight = 15
        outcome_type = "positive_reply"
    elif "meeting" in normalized_status or "booked" in normalized_status:
        reward_weight = 40
        outcome_type = "meeting_booked"
    elif "won" in normalized_status or "converted" in normalized_status:
        reward_weight = 100
        outcome_type = "deal_won"

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
                        # Persist learning synchronously so attribution signals are not silently dropped.
                        memory_saved = await record_experience(
                            agent_name="vanguard",
                            outcome_type=outcome_type,
                            narrative_summary=(
                                f"Lead ({email}) hit status '{status}' in {target_industry}. "
                                "Campaign messaging verified high-converting."
                            ),
                            industry=target_industry,
                            metadata={
                                "source_event_id": f"roi:{email.lower()}:{normalized_status}",
                                "email": email.lower(),
                                "status": normalized_status,
                            },
                        )
                        if not memory_saved:
                            print(
                                f"⚠️ [Helix Memory] ROI memory capture failed for {email} ({normalized_status})."
                            )

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
    normalized_agent = str(agent_name or "").lower().strip()
    normalized_log_level = str(log_level or "INFO").upper()
    meta = metadata if isinstance(metadata, dict) else {}
    industry = meta.get("industry_name") or meta.get("industry")
    lifecycle_event = str(meta.get("lifecycle_event") or "").upper().strip()
    status_flag = str(meta.get("status") or "").upper().strip()
    records_found = int(meta.get("records_found", 0) or 0)
    meta_string_dump = json.dumps(meta).lower()
    extended_error_context = f"{norm_msg} {meta_string_dump}"

    try:
        if normalized_agent == "nova" and (
            "429" in extended_error_context
            or "rate limit" in extended_error_context
            or meta.get("status") == "RATE_LIMITED"
        ):
            async with db_pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("""
                        UPDATE helix_infrastructure_oracle_ledger 
                        SET is_circuit_broken = TRUE, 
                            credits_remaining = 0, 
                            last_updated_at = NOW() 
                        WHERE LOWER(vendor_name) = 'moltset';
                    """)
                await conn.commit()

            if hasattr(app_state, "disabled_circuits"):
                app_state.disabled_circuits.add("moltset")

            emit_executive_notification(
                ExecutiveNotificationEvent(
                    source_agent="helix",
                    event_type="vendor_circuit_breaker",
                    severity="critical",
                    headline="MoltSet Circuit Broken (Nova 429 Captured)",
                    outcome="Nova hit MoltSet rate limit/429. Set MoltSet is_circuit_broken = TRUE.",
                    next_action="Nova dispatches halted. Engine will probe get_account every 30m.",
                )
            )

        async with db_pool.connection() as conn:
            async with conn.cursor() as cur:
                if normalized_log_level == "INFO":
                    if normalized_agent == "nova" and lifecycle_event in {
                        "RUN_COMPLETED",
                        "RUN_FAILED",
                        "RUN_RATE_LIMITED",
                    }:
                        memory_outcome = None
                        memory_summary = None

                        if lifecycle_event == "RUN_COMPLETED" and records_found > 0:
                            memory_outcome = "successful_harvest"
                            memory_summary = f"Nova successfully mined {records_found} high-value leads for {industry or 'General Operations'}."
                        elif lifecycle_event == "RUN_COMPLETED" and records_found == 0:
                            memory_outcome = "unresponsive"
                            memory_summary = (
                                f"MoltSet profile harvesting for {industry or 'General Operations'} returned 0 records. "
                                "Target criteria may be exhausted or too narrow."
                            )
                        elif lifecycle_event == "RUN_RATE_LIMITED":
                            memory_outcome = "failure"
                            memory_summary = f"Nova run for {industry or 'General Operations'} was rate-limited by provider constraints."
                        elif lifecycle_event == "RUN_FAILED" or status_flag == "FAILED":
                            memory_outcome = "failure"
                            memory_summary = f"Nova run for {industry or 'General Operations'} failed and required remediation."

                        if memory_outcome and memory_summary:
                            saved = await record_experience(
                                agent_name="nova",
                                outcome_type=memory_outcome,
                                narrative_summary=memory_summary,
                                industry=industry,
                                campaign_id=str(meta.get("run_id") or "") or None,
                                metadata={
                                    "source_event_id": (
                                        str(meta.get("event_id") or "")
                                        or f"log:{log_id}:nova:{lifecycle_event or status_flag or 'INFO'}"
                                    ),
                                    "log_id": log_id,
                                    "status": status_flag,
                                    "lifecycle_event": lifecycle_event,
                                    "records_found": records_found,
                                },
                            )
                            if not saved:
                                print(
                                    f"⚠️ [Helix Memory] Structured Nova learning capture failed for log_id={log_id}."
                                )

                    # Hook zero-yield remediation directly into telemetry processing
                    if isinstance(meta, dict) and industry:
                        asyncio.create_task(
                            handle_zero_yield_remediation(db_pool, industry, meta)
                        )

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
                    if normalized_agent == "nova" and (
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
                            if meta.get("lifecycle_event") != "RUN_RATE_LIMITED":
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
                            # Capture exhaustion memory with source metadata for dedupe safety.
                            await record_experience(
                                agent_name="nova",
                                outcome_type="unresponsive",
                                narrative_summary=f"MoltSet profile harvesting for {industry} returned 0 records. Target criteria exhausted or too narrow.",
                                industry=industry,
                                campaign_id=str(meta.get("run_id") or "") or None,
                                metadata={
                                    "source_event_id": (
                                        str(meta.get("event_id") or "")
                                        or f"log:{log_id}:nova:exhausted"
                                    ),
                                    "log_id": log_id,
                                    "status": status_flag,
                                    "lifecycle_event": str(
                                        meta.get("lifecycle_event") or ""
                                    ),
                                },
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

                            await record_experience(
                                agent_name="nova",
                                outcome_type="successful_harvest",
                                narrative_summary=f"Nova successfully mined {records_found} high-value leads for {industry}.",
                                industry=industry,
                                campaign_id=str(meta.get("run_id") or "") or None,
                                metadata={
                                    "source_event_id": (
                                        str(meta.get("event_id") or "")
                                        or f"log:{log_id}:nova:completed"
                                    ),
                                    "log_id": log_id,
                                    "status": status_flag,
                                    "lifecycle_event": str(
                                        meta.get("lifecycle_event") or ""
                                    ),
                                },
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
                        normalized_agent == "vanguard"
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
                        normalized_agent == "vector"
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
                        if strategy and agent_name == "nexus":
                            master_token = os.getenv("HELIX_AUTH_TOKEN", "")
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


async def handle_zero_yield_remediation(
    db_pool, industry_name: str, clean_metadata: dict
):
    """
    Triggers automatically when a sweep processes 0 leads due to deduplication.
    Rotates search parameters in helix_brain_scheduler to prevent query stagnation.
    """
    processed = clean_metadata.get(
        "processed", clean_metadata.get("Total Leads Staged", -1)
    )
    skipped = clean_metadata.get("skipped", clean_metadata.get("Skipped", 0))

    if processed == 0 and skipped > 0:
        emit_executive_notification(
            ExecutiveNotificationEvent(
                source_agent="helix",
                event_type="pipeline_milestone",
                severity="warning",
                headline="Zero-Yield Sweep Detected",
                outcome=f"Sweep for '{industry_name}' yielded 0 new leads ({skipped} duplicates skipped).",
                next_action="Auto-rotating target query parameters & incrementing requested batch size.",
            )
        )

        # Proactively bump requested count or reset target timestamp to pull fresh angles
        async with db_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    UPDATE helix_brain_scheduler
                    SET priority_score = GREATEST(priority_score - 1, 1),
                        last_targeted_at = NOW()
                    WHERE industry_name = %s;
                """,
                    (industry_name,),
                )
            await conn.commit()
