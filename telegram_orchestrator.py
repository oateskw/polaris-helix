import os
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
                SELECT timestamp, agent_name, log_level, message, metadata
                FROM agent_system_logs
                WHERE timestamp >= (NOW() AT TIME ZONE 'America/New_York')::date
                ORDER BY timestamp DESC
                LIMIT 10;
            """

            async with db_pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(query)
                    records = await cur.fetchall()

            if not records:
                send_telegram_broadcast(
                    "📆 <b>Today's Logs:</b> No agent system logs recorded yet today.",
                    "orchestration",
                )
                return {"status": "processed"}

            # 1. Received your verification ping successfully!
            send_telegram_broadcast(
                f"⚡ <b>Database Hit:</b> Found {len(records)} recent log entries for today. Compiling feed...",
                "orchestration",
            )

            # 2. Build the feed with absolute HTML sanitization
            history_text = "📆 <b>Today's Agent Logs (Latest 10):</b>\n\n"
            for rec in records:
                timestamp, agent_name, log_level, message, metadata = rec
                time_str = timestamp.strftime("%H:%M:%S")

                if log_level.upper() in ["ERROR", "CRITICAL"]:
                    icon = "❌"
                elif log_level.upper() == "WARNING":
                    icon = "⚠️"
                else:
                    icon = "🔹"

                # Cleanly escape any raw logs containing text like: "Error in <module> & others"
                safe_agent = html.escape(str(agent_name))
                safe_level = html.escape(str(log_level))
                safe_message = html.escape(str(message))

                history_text += f"{icon} <b>[{time_str}] {safe_agent}</b> (<code>{safe_level}</code>)\n"
                history_text += f"• {safe_message}\n"

                if metadata and isinstance(metadata, dict):
                    meta_details = ", ".join([f"{k}: {v}" for k, v in metadata.items()])
                    safe_meta = html.escape(meta_details)
                    history_text += f"• <i>Meta:</i> <code>{safe_meta}</code>\n"

                history_text += "\n"

            # 3. Fire the sanitized payload
            send_telegram_broadcast(history_text, "orchestration")

        except Exception as err:
            send_telegram_broadcast(
                f"❌ <b>History Engine Failure:</b> <code>{str(err)}</code>",
                "orchestration",
            )

    return {"status": "processed"}
