import os
import asyncio
import html
from fastapi import APIRouter, Request, HTTPException
from helix_optimizer import run_daily_prompt_optimization
from services.telemetry import send_telegram_broadcast

router = APIRouter(prefix="/api/v1/telegram", tags=["telegram-control"])

# Bind directly to your existing environment variable string
TELEGRAM_ADMIN_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()


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
        send_telegram_broadcast(
            "⏳ *Manual Telegram Trigger:* Initializing Claude prompt optimization override...",
            "orchestration",
        )
        try:
            # Import your underlying optimization service dynamically
            from helix_optimizer import run_daily_prompt_optimization

            db_pool = request.app.state.db_pool

            # Force the prompt tuning loop to bypass the standard anomaly thresholds
            await run_daily_prompt_optimization(db_pool, force=True)

            send_telegram_broadcast(
                "✅ *Success:* Calibration sweep completed cleanly via Telegram command.",
                "orchestration",
            )
        except Exception as err:
            send_telegram_broadcast(
                f"❌ *Failure:* Telegram forced optimization crashed: `{str(err)}`",
                "orchestration",
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
            status_message = (
                f"📊 *Polaris-Helix Health Status:*\n"
                f"• *Environment:* Production (Render)\n"
                f"• *Database Pool:* {db_status}\n"
                f"• *Optimizer Loop:* {optimizer_status}"
            )
            send_telegram_broadcast(status_message, "orchestration")

        except Exception as err:
            send_telegram_broadcast(
                f"❌ *Status Diagnostics Failure:* `{str(err)}`", "orchestration"
            )

    elif text == "/history":
        try:
            db_pool = request.app.state.db_pool

            # Query the absolute latest log entry for each agent today, ordered chronologically
            query = """
                WITH ranked_logs AS (
                    SELECT 
                        timestamp, 
                        agent_name, 
                        log_level, 
                        message,
                        ROW_NUMBER() OVER (PARTITION BY agent_name ORDER BY timestamp DESC) as rn
                    FROM agent_system_logs
                    WHERE timestamp >= CURRENT_DATE
                )
                SELECT timestamp, agent_name, log_level, message
                FROM ranked_logs
                WHERE rn = 1
                ORDER BY timestamp ASC;
            """

            async with db_pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(query)
                    records = await cur.fetchall()

            if not records:
                send_telegram_broadcast(
                    "📆 *System Alert:* No active agent runs recorded today.",
                    "orchestration",
                )
                return {"status": "processed"}

            summary_lines = ["📋 *Polaris-Helix Agent Timelines (Latest State):*\n"]

            for rec in records:
                timestamp, agent_name, log_level, message = rec
                time_str = timestamp.strftime("%I:%M %p")

                msg_lower = message.lower()
                if log_level.upper() in ["ERROR", "CRITICAL"]:
                    status = "failed ❌"
                elif "ratelimited" in msg_lower:
                    status = "rate-limited ⚠️"
                elif (
                    "finalized" in msg_lower
                    or "successful" in msg_lower
                    or "flush" in msg_lower
                ):
                    status = "completed run ✅"
                else:
                    status = "active 🔹"

                display_name = str(agent_name).capitalize()

                # Formatted with Markdown instead of HTML
                summary_lines.append(
                    f"• *{display_name}* active at `{time_str}` ({status})"
                )

            final_summary = "\n".join(summary_lines)
            send_telegram_broadcast(final_summary, "orchestration")

        except Exception as err:
            send_telegram_broadcast(
                f"❌ *Summary Engine Failure:* `{str(err)}`", "orchestration"
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
                send_telegram_broadcast(
                    f"⚠️ *Invalid Agent:* `{target_agent}` is not part of the ecosystem.",
                    "orchestration",
                )
                return {"status": "processed"}

            # 1. Feedback via Markdown
            if target_agent:
                send_telegram_broadcast(
                    f"🔌 *Targeted Override:* Isolating mesh to execute *{target_agent.capitalize()}* only...",
                    "orchestration",
                )
            else:
                send_telegram_broadcast(
                    "🔌 *Master Override:* Initiating synchronized ecosystem cycle...",
                    "orchestration",
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
                send_telegram_broadcast(
                    "⚠️ *Execution Halted:* No active targets configured in the scheduler table.",
                    "orchestration",
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

            send_telegram_broadcast(
                f"🚀 *Mesh Sequence Deployed!*\n"
                f"• Target Niche: `{strategy['industry_name']}`\n"
                f"• Target Region: `{target_region_str}`\n"
                f"• Dispatched Payload: `[{agents_string}]`",
                "orchestration",
            )

        except Exception as err:
            send_telegram_broadcast(
                f"❌ *Execution Trigger Failed:* `{str(err)}`", "orchestration"
            )

    return {"status": "processed"}
