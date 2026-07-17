import asyncio
import datetime
import os
from zoneinfo import ZoneInfo
from psycopg.rows import dict_row

from services.telemetry import send_telegram_broadcast, write_helix_command_center_log
from services.mesh_client import trigger_downstream_mesh_with_payload
from helix_optimizer import run_daily_prompt_optimization

NEW_YORK_TZ = ZoneInfo("America/New_York")
VENDOR_MYEMAILVERIFIER = "MyEmailVerifier"
VENDOR_PHANTOMBUSTERS = "PhantomBusters"
VENDOR_MOLTSET = "Moltset"
VENDOR_ANTHROPIC = "Anthropic"

DAILY_EXECUTION_ROUTERS = {
    0: ["nexus", "nova", "vanguard", "radar", "pulsar"],
    1: ["nexus", "nova", "vanguard", "radar", "pulsar"],
    2: ["nexus", "nova", "vanguard", "radar", "pulsar"],
    3: ["nexus", "nova", "vanguard", "radar", "pulsar"],
    4: ["nexus", "nova", "vanguard", "radar", "pulsar"],
    5: ["nexus", "nova", "radar", "pulsar"],
    6: ["nexus", "nova", "radar", "pulsar"],
}

PUBLER_BEACON_SCHEDULE = {
    1: [datetime.time(7, 0), datetime.time(8, 0)],
    2: [datetime.time(7, 0), datetime.time(14, 0), datetime.time(19, 0)],
    3: [datetime.time(16, 0), datetime.time(18, 0)],
}


async def load_vendor_isolation_matrix(db_pool) -> dict:
    vendor_isolation_matrix = {}
    try:
        async with db_pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    "SELECT vendor_name, is_circuit_broken FROM helix_infrastructure_oracle_ledger;"
                )
                rows = await cur.fetchall()
                for row in rows:
                    vendor_isolation_matrix[row["vendor_name"]] = bool(
                        row["is_circuit_broken"]
                    )
    except Exception as exc:
        print(f"⚠️ Vendor isolation matrix load failed inside engine loop: {exc}")
    return vendor_isolation_matrix


async def perpetual_awareness_loop(app_instance):
    print("🚀 [PERPETUAL BRAIN ENGINE] Staggered drip pipeline loop active...")
    db_pool = app_instance.state.db_pool
    last_standard_drip_at = None

    while True:
        try:
            est_now = datetime.datetime.now(NEW_YORK_TZ)
            current_weekday = est_now.weekday()
            current_date_str = est_now.strftime("%Y-%m-%d")

            # =====================================================================
            # 🌟 1. TRIGGER BEACON & DEPENDENT NEXUS OUT-OF-BAND
            # =====================================================================
            should_trigger_beacon = False
            if current_weekday in PUBLER_BEACON_SCHEDULE:
                target_times = PUBLER_BEACON_SCHEDULE[current_weekday]
                for target_time in target_times:
                    target_dt = datetime.datetime.combine(
                        est_now.date(), target_time, tzinfo=NEW_YORK_TZ
                    )
                    if (
                        target_dt
                        <= est_now
                        <= (target_dt + datetime.timedelta(minutes=15))
                    ):
                        should_trigger_beacon = True
                        break

            if should_trigger_beacon:
                async with db_pool.connection() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute("""
                            SELECT COUNT(*) FROM agent_system_logs 
                            WHERE agent_name = 'beacon' 
                            AND log_level = 'INFO'
                            AND timestamp >= NOW() - INTERVAL '45 minutes';
                        """)
                        recent_runs = (await cur.fetchone())[0]

                if recent_runs == 0:
                    async with db_pool.connection() as conn:
                        async with conn.cursor(row_factory=dict_row) as cur:
                            await cur.execute("""
                                SELECT industry_id, industry_name, moltset_category, bottleneck_profile, pain_tokens, target_region
                                FROM helix_brain_scheduler 
                                WHERE is_active = TRUE 
                                ORDER BY priority_score DESC, last_targeted_at ASC NULLS FIRST LIMIT 1;
                            """)
                            active_strategy = await cur.fetchone()

                    if active_strategy:
                        send_telegram_broadcast(
                            f"📡 [PROACTIVE SEQUENCE] Deploying dependencies out-of-band for strategy: *{active_strategy['industry_name']}*.\n"
                            f"Running `nexus` structural mapping first, then invoking `beacon`.",
                            "orchestration",
                        )
                        vendor_isolation_matrix = await load_vendor_isolation_matrix(
                            db_pool
                        )

                        # Fix the gap proactively right here
                        await trigger_downstream_mesh_with_payload(
                            db_pool,
                            app_instance.state,
                            active_strategy,
                            ["nexus", "beacon"],
                            vendor_isolation_matrix,
                        )

            # =====================================================================
            # 🧠 2. DAILY PROMPT OPTIMIZATION TRIGGER
            # =====================================================================
            if est_now.hour == 23:
                if (
                    getattr(app_instance.state, "last_optimization_date", None)
                    != current_date_str
                ):
                    app_instance.state.last_optimization_date = current_date_str
                    send_telegram_broadcast(
                        "🧠 *DAILY SYSTEM OPTIMIZATION ENGINE STARTING*\nAnalyzing execution logs...",
                        "orchestration",
                    )
                    await run_daily_prompt_optimization(db_pool)

            # =====================================================================
            # 🔄 3. STANDARD SEQUENTIAL DRIP
            # =====================================================================
            now_utc = datetime.datetime.now(datetime.timezone.utc)
            if last_standard_drip_at is None or (
                now_utc - last_standard_drip_at
            ) >= datetime.timedelta(minutes=45):
                scheduled_agents_list = DAILY_EXECUTION_ROUTERS.get(current_weekday, [])

                if scheduled_agents_list:
                    async with db_pool.connection() as conn:
                        async with conn.cursor(row_factory=dict_row) as cur:
                            await cur.execute("""
                                SELECT DISTINCT LOWER(agent_name) as executed_agent
                                FROM agent_system_logs 
                                WHERE timestamp >= (NOW() AT TIME ZONE 'America/New_York')::date;
                            """)
                            completed_today_set = {
                                r["executed_agent"] for r in await cur.fetchall()
                            }

                    pending_agents_for_today = [
                        agent
                        for agent in scheduled_agents_list
                        if agent not in completed_today_set
                    ]

                    if "nova" not in pending_agents_for_today:
                        async with db_pool.connection() as conn:
                            async with conn.cursor() as retry_cur:
                                await retry_cur.execute("""
                                    SELECT COUNT(*) FROM helix_brain_scheduler 
                                    WHERE is_active = TRUE 
                                    AND (last_targeted_at IS NULL OR last_targeted_at <= NOW() - (minimum_cooldown_hours || ' hours')::INTERVAL);
                                """)
                                if (await retry_cur.fetchone())[0] > 0:
                                    pending_agents_for_today.append("nova")

                    if pending_agents_for_today:
                        vendor_isolation_matrix = await load_vendor_isolation_matrix(
                            db_pool
                        )
                        next_agent_up = None

                        for candidate_agent in pending_agents_for_today:
                            is_isolated = False
                            if (
                                candidate_agent == "vanguard"
                                and vendor_isolation_matrix.get(
                                    VENDOR_MYEMAILVERIFIER, False
                                )
                            ):
                                is_isolated = True
                            elif (
                                candidate_agent == "radar"
                                and vendor_isolation_matrix.get(
                                    VENDOR_PHANTOMBUSTERS, False
                                )
                            ):
                                is_isolated = True
                            elif (
                                candidate_agent == "nova"
                                and vendor_isolation_matrix.get(VENDOR_MOLTSET, False)
                            ):
                                is_isolated = True
                            elif (
                                candidate_agent == "nexus"
                                and vendor_isolation_matrix.get(VENDOR_ANTHROPIC, False)
                            ):
                                is_isolated = True

                            if is_isolated:
                                await write_helix_command_center_log(
                                    db_pool,
                                    "WARNING",
                                    f"Bypassed agent '{candidate_agent}' due to circuit isolation.",
                                    "QUEUE_BYPASS",
                                )
                                continue
                            next_agent_up = candidate_agent
                            break

                        if next_agent_up:
                            async with db_pool.connection() as conn:
                                async with conn.cursor(row_factory=dict_row) as cur:
                                    if next_agent_up in ["radar", "pulsar"]:
                                        await cur.execute("""
                                            SELECT industry_id, industry_name, moltset_category, bottleneck_profile, pain_tokens, target_region
                                            FROM helix_brain_scheduler WHERE is_active = TRUE 
                                            ORDER BY last_targeted_at DESC NULLS LAST LIMIT 1;
                                        """)
                                    else:
                                        await cur.execute("""
                                            SELECT industry_id, industry_name, moltset_category, bottleneck_profile, pain_tokens, target_region
                                            FROM helix_brain_scheduler WHERE is_active = TRUE 
                                            AND (last_targeted_at IS NULL OR last_targeted_at <= NOW() - (minimum_cooldown_hours || ' hours')::INTERVAL)
                                            ORDER BY priority_score DESC, last_targeted_at ASC NULLS FIRST LIMIT 1;
                                        """)
                                    active_strategy = await cur.fetchone()
                                    if active_strategy:
                                        await cur.execute(
                                            "UPDATE helix_brain_scheduler SET last_targeted_at = NOW() WHERE industry_id = %s;",
                                            (active_strategy["industry_id"],),
                                        )
                                await conn.commit()

                            if active_strategy:
                                send_telegram_broadcast(
                                    f"⏳ [STAGGERED DRIP] Deploying `{next_agent_up}` for strategy: *{active_strategy['industry_name']}*",
                                    "orchestration",
                                )
                                await write_helix_command_center_log(
                                    db_pool,
                                    "INFO",
                                    f"Deploying worker agent '{next_agent_up}'.",
                                    f"LAUNCH_{next_agent_up.upper()}",
                                    active_strategy["industry_name"],
                                    {"strategy_id": active_strategy["industry_id"]},
                                )

                                await trigger_downstream_mesh_with_payload(
                                    db_pool,
                                    app_instance.state,
                                    active_strategy,
                                    [next_agent_up],
                                    vendor_isolation_matrix,
                                )
                                last_standard_drip_at = now_utc
                        else:
                            await write_helix_command_center_log(
                                db_pool,
                                "INFO",
                                "All pending queue items are blocked by circuit isolation.",
                                "ORCHESTRATOR_STALL_WAIT",
                            )

        except Exception as loop_crash:
            send_telegram_broadcast(f"Loop exception: `{loop_crash}`", "critical")

        await asyncio.sleep(60)
