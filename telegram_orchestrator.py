import os
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

            # Query for events that occurred today (using UTC to align with Neon/Render DB defaults)
            query = """
                SELECT timestamp, agent_name, log_level, message, metadata
                FROM agent_system_logs
                WHERE timestamp >= CURRENT_DATE
                ORDER BY timestamp DESC
                LIMIT 10;
            """

            async with db_pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(query)
                    records = await cur.fetchall()

            if not records:
                send_telegram_broadcast(
                    "📆 *Today's Logs:* No agent system logs recorded yet today.",
                    "orchestration",
                )
                return {"status": "processed"}

            # Render a clean feed of today's agent actions
            history_text = "📆 *Today's Agent Logs (Latest 10):*\n\n"
            for rec in records:
                timestamp, agent_name, log_level, message, metadata = rec
                time_str = timestamp.strftime("%H:%M:%S")

                # Visual hierarchy based on log severity
                if log_level.upper() in ["ERROR", "CRITICAL"]:
                    icon = "❌"
                elif log_level.upper() == "WARNING":
                    icon = "⚠️"
                else:
                    icon = "🔹"

                history_text += f"{icon} *[{time_str}] {agent_name}* (`{log_level}`)\n"
                history_text += f"• {message}\n"

                # Check for useful telemetry tucked inside your JSONB metadata field
                if metadata and isinstance(metadata, dict):
                    meta_details = ", ".join(
                        [f"{k}: `{v}`" for k, v in metadata.items()]
                    )
                    history_text += f"• _Meta:_ {meta_details}\n"

                history_text += "\n"

            send_telegram_broadcast(history_text, "orchestration")

        except Exception as err:
            send_telegram_broadcast(
                f"❌ *History Query Failed:* `{str(err)}`", "orchestration"
            )

    return {"status": "processed"}
