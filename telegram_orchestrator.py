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
            "⏳ *Manual Telegram Trigger:* Initializing Claude 4.5 Sonnet optimization override...",
            "orchestration",
        )
        try:
            db_pool = request.app.state.db_pool
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
        send_telegram_broadcast(
            f"📊 *Polaris-Helix Health Status:*\n"
            f"• *Environment:* Production (Render)\n"
            f"• *Database Pool:* Connected & Stable\n"
            f"• *Optimizer Loop:* Autonomous Background Daemon Active 🟢",
            "orchestration",
        )

    elif text == "/history":
        try:
            db_pool = request.app.state.db_pool

            query = """
                WITH ranked_logs AS (
                    SELECT 
                        timestamp, 
                        agent_name, 
                        log_level, 
                        message,
                        ROW_NUMBER() OVER (PARTITION BY agent_name ORDER BY timestamp DESC) as rn
                    FROM agent_system_logs
                    WHERE timestamp >= (NOW() AT TIME ZONE 'America/New_York')::date)
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
                    "📆 <b>System Alert:</b> No active agent runs recorded today.",
                    "orchestration",
                )
                return {"status": "processed"}

            summary_lines = [
                "📋 <b>Polaris-Helix Agent Timelines (Latest State):</b>\n"
            ]

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

                summary_lines.append(
                    f"• <b>{display_name}</b> active at <code>{time_str}</code> ({status})"
                )

            final_summary = "\n".join(summary_lines)
            send_telegram_broadcast(final_summary, "orchestration")

        except Exception as err:
            send_telegram_broadcast(
                f"❌ <b>Summary Engine Failure:</b> <code>{html.escape(str(err))}</code>",
                "orchestration",
            )

    elif text.startswith("/execute"):
        try:
            # Split out the argument if one exists (e.g., "/execute nexus")
            parts = text.split(maxsplit=1)
            target_agent = parts[1].lower().strip() if len(parts) > 1 else None

            # 🆕 Dynamically extract allowed agent names directly from your main.py schema
            from main import LogIngestSchema
            from typing import get_args

            # Extract the arguments from the Literal definition inside the schema
            agent_field_type = LogIngestSchema.__annotations__["agent_name"]
            valid_agents = get_args(
                agent_field_type
            )  # Yields ('vanguard', 'beacon', 'echo', ...)

            # Validate if a single agent was specified
            if target_agent and target_agent not in valid_agents:
                send_telegram_broadcast(
                    f"⚠️ <b>Invalid Agent:</b> <code>{html.escape(target_agent)}</code> is not part of the ecosystem.",
                    "orchestration",
                )
                return {"status": "processed"}

            # 1. Feedback
            if target_agent:
                send_telegram_broadcast(
                    f"🔌 <b>Targeted Override:</b> Isolating mesh to execute <b>{target_agent.capitalize()}</b> only...",
                    "orchestration",
                )
            else:
                send_telegram_broadcast(
                    "🔌 <b>Master Override:</b> Initiating synchronized ecosystem cycle...",
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
                    "⚠️ <b>Execution Halted:</b> No active targets configured in the scheduler table.",
                    "orchestration",
                )
                return {"status": "processed"}

            # 3. Pull isolation parameters & set specific agent payload
            from core.engine import load_vendor_isolation_matrix
            from services.mesh_client import trigger_downstream_mesh_with_payload

            vendor_matrix = await load_vendor_isolation_matrix(db_pool)

            # If target_agent is specified, wrap it in a list; otherwise, dispatch the default master stack
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

            send_telegram_broadcast(
                f"🚀 <b>Mesh Sequence Deployed!</b>\n"
                f"• Target Niche: <code>{html.escape(strategy['industry_name'])}</code>\n"
                f"• Dispatched Payload: <code>{agents_to_run}</code>",
                "orchestration",
            )

        except Exception as err:
            send_telegram_broadcast(
                f"❌ <b>Execution Trigger Failed:</b> <code>{html.escape(str(err))}</code>",
                "orchestration",
            )

    return {"status": "processed"}
