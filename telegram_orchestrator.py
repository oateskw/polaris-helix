import os
import asyncio
import hmac
import html
import datetime
from zoneinfo import ZoneInfo
import httpx
from fastapi import APIRouter, Request, HTTPException
from psycopg.rows import dict_row
from helix_optimizer import run_daily_prompt_optimization
from services.telemetry import (
    ExecutiveNotificationEvent,
    send_executive_summary_from_logs,
    is_vendor_circuit_healthy,
)

router = APIRouter(prefix="/api/v1/telegram", tags=["telegram-control"])

# Bind directly to your existing environment variable string
TELEGRAM_ADMIN_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
EASTERN_TZ = ZoneInfo("America/New_York")
DISPATCHABLE_AGENTS = frozenset(
    {"nexus", "nova", "beacon", "radar", "pulsar", "vanguard", "aura"}
)
SCHEDULABLE_AGENTS = DISPATCHABLE_AGENTS | {"helix"}


async def _send_command_notification(event: ExecutiveNotificationEvent) -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        print("Telegram command response skipped: credentials are not configured.")
        return

    lines = [
        f"<b>{html.escape(event.headline)}</b>",
        f"Agent: <code>{html.escape(event.source_agent)}</code>",
        f"Outcome: {html.escape(event.outcome)}",
    ]
    if event.impact:
        lines.append(f"Impact: {html.escape(event.impact)}")
    if event.next_action:
        lines.append(f"Next: {html.escape(event.next_action)}")

    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            response = await client.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={
                    "chat_id": chat_id,
                    "text": "\n".join(lines),
                    "parse_mode": "HTML",
                },
            )
            if response.status_code != 200:
                print(
                    "Telegram command response rejected "
                    f"with HTTP {response.status_code}: {response.text[:300]}"
                )
    except Exception as exc:
        print(f"Telegram command response failed: {exc}")


def emit_executive_notification(event: ExecutiveNotificationEvent) -> None:
    asyncio.create_task(_send_command_notification(event))


def _parse_command(text: str) -> tuple[str, str | None]:
    parts = text.strip().split(maxsplit=1)
    if not parts:
        return "", None
    command = parts[0].split("@", 1)[0].lower()
    argument = parts[1].strip().lower() if len(parts) > 1 else None
    return command, argument


async def _claim_update(db_pool, update_id: int, command: str) -> bool:
    async with db_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO telegram_processed_updates (update_id, command)
                VALUES (%s, %s)
                ON CONFLICT (update_id) DO NOTHING
                RETURNING update_id;
                """,
                (update_id, command),
            )
            claimed = await cur.fetchone()
            await cur.execute(
                "DELETE FROM telegram_processed_updates WHERE processed_at < NOW() - INTERVAL '30 days';"
            )
        await conn.commit()
    return claimed is not None


def _worker_status(task) -> str:
    if task is None:
        return "not initialized"
    if task.cancelled():
        return "cancelled"
    if not task.done():
        return "running"
    exception = task.exception()
    return f"crashed: {str(exception)[:80]}" if exception else "stopped"


@router.post("/webhook")
async def telegram_webhook_handler(request: Request):
    """
    Secure incoming webhook endpoint handling interactive commands
    sent to @Polaris_Inbound_Lead_bot.
    """
    expected_secret = os.getenv("HELIX_AUTH_TOKEN", "")
    provided_secret = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
    if (
        not expected_secret
        or not provided_secret
        or not hmac.compare_digest(provided_secret, expected_secret)
    ):
        raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    message = payload.get("message")
    if not isinstance(message, dict) or not isinstance(message.get("text"), str):
        return {"status": "ignored"}

    sender = message.get("from")
    chat = message.get("chat")
    if not isinstance(sender, dict) or not isinstance(chat, dict):
        return {"status": "ignored"}

    user_id = str(sender.get("id", "")).strip()
    chat_id = str(chat.get("id", "")).strip()
    text = message.get("text", "").strip()

    admin_chat_id = os.getenv("TELEGRAM_CHAT_ID", TELEGRAM_ADMIN_CHAT_ID).strip()
    if not admin_chat_id or user_id != admin_chat_id or chat_id != admin_chat_id:
        # Ignore unauthorized users silently to prevent exploit scanning
        return {"status": "unauthorized_user_dropped"}

    command, argument = _parse_command(text)
    if not command.startswith("/"):
        return {"status": "ignored"}

    update_id = payload.get("update_id")
    if not isinstance(update_id, int):
        raise HTTPException(status_code=400, detail="Missing Telegram update_id")

    db_pool = request.app.state.db_pool
    if not await _claim_update(db_pool, update_id, command):
        return {"status": "duplicate"}

    # 2. Command Processing Matrix
    if command == "/optimize":
        # 🆕 PREEMPTIVE CHECK
        db_pool = request.app.state.db_pool
        if not await is_vendor_circuit_healthy(db_pool, "Anthropic"):
            emit_executive_notification(
                ExecutiveNotificationEvent(
                    source_agent="helix",
                    event_type="action_blocked",
                    severity="warning",
                    headline="Optimization Aborted",
                    outcome="❌ Command rejected: Anthropic circuit breaker is TRIPPED in infrastructure ledger.",
                    next_action="Replenish API wallet balance and reset circuit ledger record.",
                )
            )
            return {"status": "processed"}
        # 🆕 END PREEMPTIVE CHECK

        emit_executive_notification(
            ExecutiveNotificationEvent(
                source_agent="helix",
                event_type="manual_command_ack",
                severity="info",
                headline="Optimization Command Received",
                outcome="Manual prompt optimization override started.",
                next_action="Await completion confirmation.",
            )
        )
        try:
            # Import your underlying optimization service dynamically
            from helix_optimizer import run_daily_prompt_optimization

            db_pool = request.app.state.db_pool

            # Force the prompt tuning loop to bypass the standard anomaly thresholds
            await run_daily_prompt_optimization(db_pool, force=True)

            emit_executive_notification(
                ExecutiveNotificationEvent(
                    source_agent="helix",
                    event_type="manual_command_ack",
                    severity="success",
                    headline="Optimization Command Completed",
                    outcome="Prompt calibration sweep completed successfully.",
                    next_action="Continue tracking agent quality signals.",
                )
            )
        except Exception as err:
            emit_executive_notification(
                ExecutiveNotificationEvent(
                    source_agent="helix",
                    event_type="critical_failure",
                    severity="critical",
                    headline="Optimization Command Failed",
                    outcome="Manual optimization crashed during execution.",
                    impact=str(err),
                    next_action="Inspect Helix optimizer logs and retry.",
                )
            )

    elif command == "/status":
        try:
            app_state = request.app.state
            db_pool = getattr(app_state, "db_pool", None)

            # 1. Dynamically verify NeonDB Pool status
            db_status = (
                "Stable 🟢" if db_pool and not db_pool.closed else "Disconnected ❌"
            )

            worker_states = {
                worker_name: _worker_status(getattr(app_state, state_name, None))
                for worker_name, state_name in (
                    ("auditor", "auditor_task"),
                    ("awareness", "awareness_task"),
                    ("credit sync", "credit_sync_task"),
                    ("optimizer", "optimizer_task"),
                    ("digest", "digest_task"),
                    ("watchdog", "watchdog_task"),
                )
            }
            workers_summary = "; ".join(
                f"{name}: {status}" for name, status in worker_states.items()
            )
            degraded = any(status != "running" for status in worker_states.values())

            # 3. Compile and broadcast
            emit_executive_notification(
                ExecutiveNotificationEvent(
                    source_agent="helix",
                    event_type="manual_command_ack",
                    severity="warning" if degraded else "info",
                    headline="Helix Status Snapshot",
                    outcome=f"Database: {db_status}; Workers: {workers_summary}",
                    next_action=(
                        "Inspect stopped or crashed workers."
                        if degraded
                        else "No action needed."
                    ),
                )
            )

        except Exception as err:
            emit_executive_notification(
                ExecutiveNotificationEvent(
                    source_agent="helix",
                    event_type="critical_failure",
                    severity="critical",
                    headline="Status Command Failed",
                    outcome="Could not compile Helix diagnostics.",
                    impact=str(err),
                    next_action="Inspect API and DB health.",
                )
            )

    elif command == "/history":
        # 🆕 PREEMPTIVE CHECK
        db_pool = request.app.state.db_pool
        if not await is_vendor_circuit_healthy(db_pool, "Anthropic"):
            print(
                "⚠️ Telegram requested /history while Claude is tripped. Forcing local metrics fallback."
            )
            # Explicitly clear ai_client to force structural text generation safely
            await send_executive_summary_from_logs(
                db_pool, lookback_minutes=180, ai_client=None
            )
            return {"status": "processed"}
        # 🆕 END PREEMPTIVE CHECK

        try:
            db_pool = request.app.state.db_pool
            ai_client = request.app.state.ai_client
            await send_executive_summary_from_logs(
                db_pool, lookback_minutes=180, ai_client=ai_client
            )

        except Exception as err:
            emit_executive_notification(
                ExecutiveNotificationEvent(
                    source_agent="helix",
                    event_type="critical_failure",
                    severity="critical",
                    headline="History Command Failed",
                    outcome="Could not generate executive history summary.",
                    impact=str(err),
                    next_action="Inspect log query path and DB health.",
                )
            )

    elif command == "/execute":
        try:
            target_agent = argument

            # Validate if a single agent was specified
            if target_agent and target_agent not in DISPATCHABLE_AGENTS:
                emit_executive_notification(
                    ExecutiveNotificationEvent(
                        source_agent="helix",
                        event_type="manual_command_ack",
                        severity="warning",
                        headline="Execution Command Rejected",
                        outcome=f"Unknown agent '{target_agent}' was requested.",
                        next_action="Use a valid agent identifier.",
                    )
                )
                return {"status": "processed"}

            # 1. Feedback via Markdown
            if target_agent:
                emit_executive_notification(
                    ExecutiveNotificationEvent(
                        source_agent="helix",
                        event_type="manual_command_ack",
                        severity="info",
                        headline="Targeted Execution Requested",
                        outcome=f"Single-agent run initiated for {target_agent}.",
                        next_action="Await completion milestone.",
                    )
                )
            else:
                emit_executive_notification(
                    ExecutiveNotificationEvent(
                        source_agent="helix",
                        event_type="manual_command_ack",
                        severity="info",
                        headline="Full Mesh Execution Requested",
                        outcome="Synchronized multi-agent cycle initiated.",
                        next_action="Monitor milestone updates for completion.",
                    )
                )

            # 2. Extract active strategy from DB
            db_pool = request.app.state.db_pool
            query = """
                SELECT industry_id, industry_name, moltset_category, bottleneck_profile, pain_tokens, target_region, campaign_strategy
                FROM helix_brain_scheduler 
                WHERE is_active = TRUE 
                ORDER BY last_targeted_at ASC NULLS FIRST, priority_score DESC 
                LIMIT 1;
            """

            async with db_pool.connection() as conn:
                async with conn.cursor(row_factory=dict_row) as cur:
                    await cur.execute(query)
                    strategy = await cur.fetchone()

            if not strategy:
                emit_executive_notification(
                    ExecutiveNotificationEvent(
                        source_agent="helix",
                        event_type="manual_command_ack",
                        severity="warning",
                        headline="Execution Halted",
                        outcome="No active scheduler targets found.",
                        next_action="Activate at least one scheduler strategy.",
                    )
                )
                return {"status": "processed"}

            # 3. Pull isolation parameters & set specific agent payload
            from core.engine import load_vendor_isolation_matrix
            from services.mesh_client import trigger_downstream_mesh_with_payload

            vendor_matrix = await load_vendor_isolation_matrix(db_pool)

            # Set specific agent payload or default master stack
            agents_to_run = (
                [target_agent]
                if target_agent
                else ["nexus", "beacon", "nova", "vanguard", "pulsar"]
            )

            dispatch_results = await trigger_downstream_mesh_with_payload(
                db_pool,
                request.app.state,
                strategy,
                agents_to_run,
                vendor_matrix,
            )

            accepted_agents = sorted(
                agent for agent, accepted in dispatch_results.items() if accepted
            )
            failed_agents = sorted(
                agent for agent, accepted in dispatch_results.items() if not accepted
            )

            if accepted_agents:
                async with db_pool.connection() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute(
                            "UPDATE helix_brain_scheduler SET last_targeted_at = NOW() WHERE industry_id = %s;",
                            (strategy["industry_id"],),
                        )
                    await conn.commit()

            target_region_str = strategy["target_region"] or "Global"
            accepted_string = ", ".join(accepted_agents) or "none"
            failed_string = ", ".join(failed_agents) or "none"

            emit_executive_notification(
                ExecutiveNotificationEvent(
                    source_agent="helix",
                    event_type="pipeline_milestone",
                    severity=(
                        "success"
                        if accepted_agents and not failed_agents
                        else "warning"
                    ),
                    headline=(
                        "Mesh Sequence Deployed"
                        if accepted_agents
                        else "Mesh Dispatch Failed"
                    ),
                    outcome=(
                        f"Accepted: {accepted_string}; failed or blocked: {failed_string}."
                    ),
                    impact=(
                        f"Industry: {strategy['industry_name']}; region: {target_region_str}"
                    ),
                    next_action=(
                        "Track accepted agent completions in Helix logs."
                        if accepted_agents
                        else "Inspect downstream availability and circuit breakers."
                    ),
                )
            )

        except Exception as err:
            emit_executive_notification(
                ExecutiveNotificationEvent(
                    source_agent="helix",
                    event_type="critical_failure",
                    severity="critical",
                    headline="Execution Command Failed",
                    outcome="Manual mesh trigger failed before completion.",
                    impact=str(err),
                    next_action="Inspect scheduler and mesh dispatch dependencies.",
                )
            )

    elif command in {"/enable", "/disable"}:
        try:
            db_pool = request.app.state.db_pool
            target_agent = argument or ""

            if target_agent not in SCHEDULABLE_AGENTS:
                emit_executive_notification(
                    ExecutiveNotificationEvent(
                        source_agent="helix",
                        event_type="action_blocked",
                        severity="warning",
                        headline="Command Rejected",
                        outcome=(
                            f"Unknown or unschedulable agent '{target_agent}'. Valid agents: "
                            f"{', '.join(sorted(SCHEDULABLE_AGENTS))}"
                        ),
                        next_action="Provide a valid agent identifier.",
                    )
                )
                return {"status": "processed"}

            # 🛑 2. Fail-Safe: Helix can NEVER be disabled
            if target_agent == "helix" and command == "/disable":
                emit_executive_notification(
                    ExecutiveNotificationEvent(
                        source_agent="helix",
                        event_type="action_blocked",
                        severity="warning",
                        headline="Operation Blocked",
                        outcome="❌ Cannot disable `helix`. It is the core operational brain.",
                        next_action="System protection active.",
                    )
                )
                return {"status": "processed"}

            enable_state = True if command == "/enable" else False
            status_verb = "Enabled" if enable_state else "Disabled"
            status_emoji = "🟢" if enable_state else "🔴"

            # 💾 3. Corrected Database Transaction with explicit commit
            async with db_pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        INSERT INTO helix_agent_schedule (agent_name, weekday_int, is_enabled)
                        SELECT %s, w, %s
                        FROM generate_series(0, 6) AS w
                        ON CONFLICT (agent_name, weekday_int) 
                        DO UPDATE SET is_enabled = EXCLUDED.is_enabled;
                        """,
                        (target_agent, enable_state),
                    )
                await conn.commit()

            emit_executive_notification(
                ExecutiveNotificationEvent(
                    source_agent="helix",
                    event_type="schedule_modifier",
                    severity="info",
                    headline="Schedule Strategy Updated",
                    outcome=f"{status_emoji} `{target_agent}` has been *{status_verb}* globally.",
                    next_action="Engine will reference the updated matrix on the next tick interval.",
                )
            )

        except Exception as err:
            emit_executive_notification(
                ExecutiveNotificationEvent(
                    source_agent="helix",
                    event_type="critical_failure",
                    severity="critical",
                    headline="Schedule Command Failed",
                    outcome=f"Could not update schedule state for target agent.",
                    impact=str(err),
                    next_action="Inspect database connectivity and query parameters.",
                )
            )

    # 2. Smart /today Fetching (Supports Multiple Runs)
    elif command == "/today":
        day_name = datetime.datetime.now(EASTERN_TZ).strftime("%A")
        try:
            db_pool = request.app.state.db_pool

            est_now = datetime.datetime.now(EASTERN_TZ)
            current_weekday = int(est_now.strftime("%w"))
            day_name = est_now.strftime("%A")

            # Dedicated emoji mapping based on infrastructure layout
            EMOJI_MATRIX = {
                "helix": "⚙️",
                "nova": "🚀",
                "pulsar": "📡",
                "radar": "🎯",
                "vanguard": "🛡️",
                "nexus": "🧠",
                "beacon": "🚨",
            }

            # 1. Gather today's completed execution counts from system logs
            async with db_pool.connection() as conn:
                async with conn.cursor(row_factory=dict_row) as cur:
                    await cur.execute("""
                        SELECT
                            LOWER(agent_name) AS agent,
                            COUNT(DISTINCT COALESCE(
                                metadata->>'run_id',
                                metadata->>'event_id',
                                id::text
                            )) AS run_count
                        FROM agent_system_logs 
                        WHERE timestamp >= (
                            date_trunc('day', NOW() AT TIME ZONE 'America/New_York')
                            AT TIME ZONE 'America/New_York'
                        )
                          AND (
                              UPPER(COALESCE(metadata->>'status', '')) = 'COMPLETED'
                              OR UPPER(COALESCE(metadata->>'lifecycle_event', '')) IN ('COMPLETED', 'RUN_COMPLETED')
                              OR LOWER(message) LIKE '%execution status: success%'
                          )
                        GROUP BY LOWER(agent_name);
                    """)
                    log_rows = await cur.fetchall()
                    run_counts = {r["agent"]: r["run_count"] for r in log_rows}

            helix_runs = run_counts.get("helix", 0)
            completed_lines = []
            if helix_runs:
                completed_lines.append(
                    f"  {EMOJI_MATRIX.get('helix', '⚙️')} *helix:* {helix_runs} completed"
                )
            pending_lines = []

            # 3. Query the database table for ALL OTHER agents
            async with db_pool.connection() as conn:
                async with conn.cursor(row_factory=dict_row) as cur:
                    await cur.execute(
                        """
                        SELECT agent_name, max_daily_runs, scheduled_time 
                        FROM helix_agent_schedule 
                        WHERE weekday_int = %s AND is_enabled = TRUE AND LOWER(agent_name) != 'helix';
                    """,
                        [current_weekday],
                    )
                    sched_rows = await cur.fetchall()

            # 4. Process the rest of the agents dynamically into separate lines
            for row in sched_rows:
                agent = row["agent_name"].lower()
                max_runs = row["max_daily_runs"]
                sched_time = row["scheduled_time"]
                actual_runs = run_counts.get(agent, 0)
                emoji = EMOJI_MATRIX.get(agent, "🤖")

                time_suffix = (
                    f" @ {sched_time.strftime('%I:%M %p')}" if sched_time else ""
                )

                if actual_runs > 0:
                    completed_lines.append(
                        f"  {emoji} *{agent}:* {actual_runs} / {max_runs}x runs"
                    )

                if actual_runs < max_runs:
                    if sched_time:
                        pending_lines.append(f"  ⏳ *{agent}* ({time_suffix})")
                    else:
                        pending_lines.append(
                            f"  ⏳ *{agent}* ({max_runs - actual_runs} left)"
                        )

            completed_str = (
                "\n" + "\n".join(completed_lines)
                if completed_lines
                else "\n  _No completed runs recorded._"
            )
            pending_str = (
                ("\n" + "\n".join(pending_lines))
                if pending_lines
                else "\n  _All clear! 🎉_"
            )

            emit_executive_notification(
                ExecutiveNotificationEvent(
                    source_agent="helix",
                    event_type="manual_command_ack",
                    severity="info",
                    headline=f"Time-Aware Schedule ({day_name})",
                    outcome=f"✅ *Executed Today:*{completed_str}\n⏳ *Remaining / Upcoming:*{pending_str}",
                    next_action="Monitoring scheduled precision check-ins.",
                )
            )
        except Exception as err:
            # Fallback emergency reporting if DB breaks entirely
            emit_executive_notification(
                ExecutiveNotificationEvent(
                    source_agent="helix",
                    event_type="manual_command_ack",
                    severity="warning",
                    headline=f"Schedule Snapshot ({day_name})",
                    outcome="✅ *Executed Today:*\n  ⚙️ *helix:* Active\n⚠️ *Notice:* Database schedule table unavailable.",
                    next_action="Verify database connectivity.",
                )
            )

    elif command == "/help":
        emit_executive_notification(
            ExecutiveNotificationEvent(
                source_agent="helix",
                event_type="manual_command_ack",
                severity="info",
                headline="Helix Command Reference",
                outcome=(
                    "/status, /today, /history, /optimize, /execute [agent], "
                    "/enable <agent>, /disable <agent>"
                ),
                next_action="Choose a command from the available control surface.",
            )
        )
    else:
        emit_executive_notification(
            ExecutiveNotificationEvent(
                source_agent="helix",
                event_type="manual_command_ack",
                severity="warning",
                headline="Unknown Telegram Command",
                outcome=f"Command '{command}' is not supported.",
                next_action="Use /help to view available commands.",
            )
        )

    return {"status": "processed"}
