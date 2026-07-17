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

            # Fetch actions executed today (UTC time zone aligned with NeonDB/Render)
            query = """
                SELECT created_at, action_type, status, details 
                FROM optimization_logs 
                WHERE created_at >= CURRENT_DATE 
                ORDER BY created_at DESC;
            """

            async with db_pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(query)
                    records = await cur.fetchall()

            if not records:
                send_telegram_broadcast(
                    "📆 *Today's Execution Log:* No automated or manual runs recorded yet today.",
                    "orchestration",
                )
                return {"status": "processed"}

            # Format the output beautifully for Telegram
            history_text = "📆 *Today's Execution History:*\n\n"
            for rec in records:
                # Format timestamps to look clean (e.g., 14:32:10)
                timestamp = rec[0].strftime("%H:%M:%S")
                icon = "✅" if rec[2].lower() == "success" else "❌"
                history_text += f"{icon} *[{timestamp}] {rec[1]}*\n• Status: `{rec[2]}`\n• Detail: {rec[3]}\n\n"

            send_telegram_broadcast(history_text, "orchestration")

        except Exception as err:
            send_telegram_broadcast(
                f"❌ *History Query Failed:* `{str(err)}`", "orchestration"
            )

    return {"status": "processed"}
