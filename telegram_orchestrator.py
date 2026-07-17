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
                    "📆 <b>System Alert:</b> No operational events recorded today.",
                    "orchestration",
                )
                return {"status": "processed"}

            summary_lines = ["📋 <b>Polaris-Helix Activity Summary:</b>\n"]

            for rec in records:
                timestamp, agent_name, log_level, message = rec
                time_str = timestamp.strftime("%I:%M %p")  # e.g., "02:50 AM"

                # Determine simple status text based on log level or message context
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
                    status = "successful_run ✅"
                else:
                    status = "processed status 🔹"

                # Clean up names to match your styling preference
                display_name = str(agent_name).capitalize()

                summary_lines.append(
                    f"• <b>{display_name}</b> ran at <code>{time_str}</code> ({status})"
                )

            # Combine all lines into a single message
            final_summary = "\n".join(summary_lines)
            send_telegram_broadcast(final_summary, "orchestration")

        except Exception as err:
            send_telegram_broadcast(
                f"❌ <b>History Engine Failure:</b> <code>{html.escape(str(err))}</code>",
                "orchestration",
            )

    return {"status": "processed"}
