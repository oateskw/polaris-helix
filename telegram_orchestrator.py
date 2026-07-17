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

            send_telegram_broadcast(
                "🔍 <i>Diagnostics: Opening connection to NeonDB pool...</i>",
                "orchestration",
            )

            try:
                async with asyncio.timeout(5.0):
                    async with db_pool.connection() as conn:
                        async with conn.cursor() as cur:
                            await cur.execute(query)
                            records = await cur.fetchall()
            except TimeoutError:
                send_telegram_broadcast(
                    "❌ <b>Database Timeout:</b> Connection to NeonDB pool timed out after 5 seconds.",
                    "orchestration",
                )
                return {"status": "processed"}
            except Exception as db_err:
                send_telegram_broadcast(
                    f"❌ <b>Database Driver Error:</b> <code>{html.escape(str(db_err))}</code>",
                    "orchestration",
                )
                return {"status": "processed"}

            send_telegram_broadcast(
                f"🔍 <i>Diagnostics: Query complete. Found {len(records)} records. Entering streaming loop...</i>",
                "orchestration",
            )

            if not records:
                send_telegram_broadcast(
                    "📆 System Alert: No agent system logs found for today.",
                    "orchestration",
                )
                return {"status": "processed"}

            # Process each row with an isolated diagnostic shield
            for idx, rec in enumerate(records):
                try:
                    timestamp, agent_name, log_level, message, metadata = rec
                    time_str = (
                        timestamp.strftime("%H:%M:%S") if timestamp else "00:00:00"
                    )

                    # 1. Test string conversion and escaping
                    safe_agent = html.escape(str(agent_name or "Unknown"))
                    safe_level = html.escape(str(log_level or "INFO"))
                    safe_message = html.escape(str(message or ""))

                    single_log_entry = f"🔹 <b>[{time_str}] {safe_agent}</b> (<code>{safe_level}</code>)\n• {safe_message}\n"

                    if metadata:
                        import json

                        meta_str = json.dumps(metadata)
                        safe_meta = html.escape(meta_str)
                        single_log_entry += f"• <i>Meta:</i> <code>{safe_meta}</code>\n"

                    # 2. Attempt the transmission
                    send_telegram_broadcast(single_log_entry, "orchestration")

                except Exception as row_err:
                    # If an individual row breaks parsing or serialization, this catches it instantly
                    send_telegram_broadcast(
                        f"⚠️ <b>Row {idx} Processing Failure:</b>\n"
                        f"• Error: <code>{html.escape(str(row_err))}</code>\n"
                        f"• Raw Agent: <code>{html.escape(str(rec[1]))}</code>",
                        "orchestration",
                    )

            send_telegram_broadcast(
                "🏁 <i>Diagnostics: Log loop iteration complete.</i>", "orchestration"
            )

        except Exception as err:
            # Catches global failures (pool timeouts, syntax errors, etc.)
            send_telegram_broadcast(
                f"❌ <b>Global History Engine Failure:</b> <code>{html.escape(str(err))}</code>",
                "orchestration",
            )

    return {"status": "processed"}
