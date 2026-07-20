import os
import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo
from fastapi import APIRouter, Request, HTTPException
from psycopg.rows import dict_row
from helix_optimizer import run_daily_prompt_optimization
from services.telemetry import (
    send_telegram_broadcast,
    ExecutiveNotificationEvent,
    emit_executive_notification,
    send_executive_summary_from_logs,
)

router = APIRouter(prefix="/api/v1/telegram", tags=["telegram-control"])

# Bind directly to your existing environment variable string
TELEGRAM_ADMIN_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
EASTERN_TZ = ZoneInfo("America/New_York")


@router.post("/webhook")
async def telegram_webhook_handler(request: Request):
    """
    Secure incoming webhook endpoint handling interactive commands
    sent to @Polaris_Inbound_Lead_bot.
    """
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    if "message" not in payload or "text" not in payload["message"]:
        return {"status": "ignored"}

    message = payload["message"]
    user_id = str(message["from"]["id"]).strip()
    text = message.get("text", "").strip()

    # 1. Strict Security Perimeter Check matching your variable
    if user_id != TELEGRAM_ADMIN_CHAT_ID:
        # Ignore unauthorized users silently to prevent exploit scanning
        return {"status": "unauthorized_user_dropped"}

    # 2. Command Processing Matrix
    if text == "/optimize":
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

    elif text == "/status":
        try:
            app_state = request.app.state
            db_pool = getattr(app_state, "db_pool", None)

            # 1. Dynamically verify NeonDB Pool status
            db_status = (
                "Stable 🟢" if db_pool and not db_pool.closed else "Disconnected ❌"
            )

            # 2. Verify if the autonomous background tasks are still alive or crashed
            optimizer_task = getattr(app_state, "optimizer_task", None)
            if optimizer_task and not optimizer_task.done():
                optimizer_status = "Autonomous Background Daemon Active 🟢"
            elif optimizer_task and optimizer_task.done():
                # Check if it exited with an unhandled exception
                exc = optimizer_task.exception()
                optimizer_status = (
                    f"Crashed ❌ (`{str(exc)[:30]}`)" if exc else "Stopped ⚠️"
                )
            else:
                optimizer_status = "Not Initialized ⚠️"

            # 3. Compile and broadcast
            emit_executive_notification(
                ExecutiveNotificationEvent(
                    source_agent="helix",
                    event_type="manual_command_ack",
                    severity="info",
                    headline="Helix Status Snapshot",
                    outcome=f"Database: {db_status}; Optimizer: {optimizer_status}",
                    next_action="No action needed unless health is degraded.",
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

    elif text == "/history":
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

    elif text.startswith("/execute"):
        try:
            # Split out the argument if one exists (e.g., "/execute nexus")
            parts = text.split(maxsplit=1)
            target_agent = parts[1].lower().strip() if len(parts) > 1 else None

            # Dynamically extract allowed agent names directly from your main.py schema
            from main import LogIngestSchema
            from typing import get_args

            agent_field_type = LogIngestSchema.__annotations__["agent_name"]
            valid_agents = get_args(agent_field_type)

            # Validate if a single agent was specified
            if target_agent and target_agent not in valid_agents:
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
                SELECT industry_id, industry_name, moltset_category, bottleneck_profile, pain_tokens, target_region
                FROM helix_brain_scheduler 
                WHERE is_active = TRUE 
                ORDER BY priority_score DESC, last_targeted_at ASC NULLS FIRST 
                LIMIT 1;
            """

            async with db_pool.connection() as conn:
                async with conn.cursor(row_factory=dict_row) as cur:
                    await cur.execute(query)
                    strategy = await cur.fetchone()

                    if strategy:
                        await cur.execute(
                            "UPDATE helix_brain_scheduler SET last_targeted_at = NOW() WHERE industry_id = %s;",
                            (strategy["industry_id"],),
                        )
                await conn.commit()

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

            # 4. Handoff to background mesh
            from fastapi import BackgroundTasks

            bg_tasks = BackgroundTasks()

            bg_tasks.add_task(
                trigger_downstream_mesh_with_payload,
                db_pool,
                request.app.state,
                strategy,
                agents_to_run,
                vendor_matrix,
            )

            await bg_tasks()

            # Clean up array representation for clean markdown printing
            agents_string = ", ".join(agents_to_run)
            target_region_str = strategy["target_region"] or "Global"

            emit_executive_notification(
                ExecutiveNotificationEvent(
                    source_agent="helix",
                    event_type="pipeline_milestone",
                    severity="success",
                    headline="Mesh Sequence Deployed",
                    outcome=f"Agents [{agents_string}] dispatched for {strategy['industry_name']}.",
                    impact=f"Region: {target_region_str}",
                    next_action="Track agent completions in Helix logs.",
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

    elif text.startswith(("/enable ", "/disable ")):
        try:
            db_pool = request.app.state.db_pool
            command, target_agent = text.split(maxsplit=1)
            target_agent = target_agent.strip().lower()

            # 🛑 FAIL-SAFE: Helix can NEVER be disabled
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

            async with db_pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        INSERT INTO helix_agent_schedule (agent_name, weekday_int, is_enabled)
                        VALUES (%s, 0, %s), (%s, 1, %s), (%s, 2, %s), (%s, 3, %s), (%s, 4, %s), (%s, 5, %s), (%s, 6, %s)
                        ON CONFLICT (agent_name, weekday_int) 
                        DO UPDATE SET is_enabled = EXCLUDED.is_enabled;
                    """,
                        [
                            target_agent,
                            enable_state,
                            target_agent,
                            enable_state,
                            target_agent,
                            enable_state,
                            target_agent,
                            enable_state,
                            target_agent,
                            enable_state,
                            target_agent,
                            enable_state,
                            target_agent,
                            enable_state,
                        ],
                    )

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
            # Error handling here...
            pass

    # 2. Smart /today Fetching (Supports Multiple Runs)
    elif text == "/today":
        try:
            db_pool = request.app.state.db_pool

            from core.engine import NEW_YORK_TZ
            import datetime

            est_now = datetime.datetime.now(NEW_YORK_TZ)
            current_weekday = est_now.weekday()
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
                        SELECT LOWER(agent_name) as agent, COUNT(*) as run_count
                        FROM agent_system_logs 
                        WHERE timestamp >= (NOW() AT TIME ZONE 'America/New_York')::date
                        GROUP BY LOWER(agent_name);
                    """)
                    log_rows = await cur.fetchall()
                    run_counts = {r["agent"]: r["run_count"] for r in log_rows}

            # 2. Hard-lock Helix metrics so it bypasses DB table data entirely
            helix_runs = run_counts.get("helix", 0)
            completed_lines = [
                f"  {EMOJI_MATRIX.get('helix', '⚙️')} *helix:* {max(helix_runs, 1)} / 1x runs"
            ]
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

            completed_str = "\n" + "\n".join(completed_lines)
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

    return {"status": "processed"}
