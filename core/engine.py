import asyncio
import datetime
import os
import requests
from zoneinfo import ZoneInfo
from psycopg.rows import dict_row

from services.telemetry import (
    send_telegram_broadcast,
    write_helix_command_center_log,
    ExecutiveNotificationEvent,
    emit_executive_notification,
)
from services.mesh_client import trigger_downstream_mesh_with_payload
from helix_optimizer import run_daily_prompt_optimization

NEW_YORK_TZ = ZoneInfo("America/New_York")
VENDOR_MYEMAILVERIFIER = "MyEmailVerifier"
VENDOR_PHANTOMBUSTERS = "PhantomBusters"
VENDOR_MOLTSET = "MoltSet"
VENDOR_ANTHROPIC = "Anthropic"


async def check_and_heal_moltset_circuit(db_pool):
    """
    Actively checks the MoltSet usage endpoint to verify if the account's
    weekly search quota has refilled, healing the circuit breaker automatically.
    """
    moltset_key = os.getenv("MOLTSET_API_KEY")
    api_endpoint = os.getenv(
        "MOLTSET_API_ENDPOINT", "https://api.moltsets.com/api/v1/tools/"
    ).strip()

    if not moltset_key:
        return

    url = f"{api_endpoint.rstrip('/')}/get_account"
    headers = {
        "Authorization": f"Bearer {moltset_key}",
        "Content-Type": "application/json",
    }

    try:
        # Perform out-of-band balance check synchronously or with a standard request block
        # wrapped in an executor to avoid blocking the asyncio event loop
        loop = asyncio.get_event_loop()
        res = await loop.run_in_executor(
            None, lambda: requests.post(url, headers=headers, timeout=12)
        )

        if res.status_code == 200:
            data = res.json()

            # Navigate the payload schema defensively
            results = data.get("results", {}) or {}
            fair_use = results.get("fair_use", {}) or {}
            search_metrics = fair_use.get("search", {}) or {}
            records_1w = search_metrics.get("records", {}).get("1w", {}) or {}

            remaining = records_1w.get("remaining", 0)

            # If the quota has naturally cleared and refilled, heal the ledger!
            if remaining > 0:
                async with db_pool.connection() as conn:
                    # 🚀 Wrap this in a transaction context to auto-commit to the database
                    async with conn.transaction():
                        async with conn.cursor() as cur:
                            await cur.execute("""
                                UPDATE helix_infrastructure_oracle_ledger 
                                SET is_circuit_broken = FALSE, 
                                    last_updated_at = NOW() 
                                WHERE LOWER(vendor_name) = 'moltset' AND is_circuit_broken = TRUE;
                            """)
                            rowcount = cur.rowcount

                    # Move telemetry notifications outside the cursor block
                    if rowcount > 0:
                        print(
                            "🌤️ Central Orchestrator: MoltSet weekly search quota has been verified. Circuit healed."
                        )
                        emit_executive_notification(
                            ExecutiveNotificationEvent(
                                source_agent="helix",
                                event_type="pipeline_milestone",
                                severity="success",
                                headline="Circuit Breaker Healed Automatically",
                                outcome="MoltSet usage metrics verified positive credit balances. Circuit state cleared.",
                                next_action="Nova processing queue cleared for operational sweeps.",
                            )
                        )
    except Exception as e:
        print(f"⚠️ Circuit tracking verification failed against MoltSet portal: {e}")


async def load_vendor_isolation_matrix(db_pool) -> dict:
    """
    Loads the infrastructure ledger and evaluates circuit status.
    Forces MoltSet isolation to apply to Nova if the break occurred today.
    """
    vendor_isolation_matrix = {}
    today_date = datetime.datetime.now(NEW_YORK_TZ).date()

    try:
        async with db_pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                # 🌟 Added last_updated_at to selection criteria
                await cur.execute(
                    "SELECT vendor_name, is_circuit_broken, last_updated_at FROM helix_infrastructure_oracle_ledger;"
                )
                rows = await cur.fetchall()

                for row in rows:
                    v_name = row["vendor_name"]
                    is_broken = bool(row["is_circuit_broken"])

                    # Treat MoltSet like all other vendors: enforce isolation whenever is_circuit_broken is True
                    vendor_isolation_matrix[v_name] = is_broken

                    # Core Requirement: Check if the timestamp matches today's date
                    last_updated_date = (
                        row["last_updated_at"].date()
                        if row["last_updated_at"]
                        else None
                    )
                    is_updated_today = last_updated_date == today_date

                    # Bind rule: If it's MoltSet, evaluate both the broken status AND the date rule
                    if v_name.lower() == "moltset":
                        if is_broken and is_updated_today:
                            vendor_isolation_matrix[v_name] = True
                        else:
                            vendor_isolation_matrix[v_name] = False
                    else:
                        # Standard handling for other legacy systems
                        vendor_isolation_matrix[v_name] = is_broken

    except Exception as exc:
        print(f"⚠️ Vendor isolation matrix load failed inside engine loop: {exc}")

    return vendor_isolation_matrix


async def perpetual_awareness_loop(app_instance):
    print("🚀 [PERPETUAL BRAIN ENGINE] Staggered drip pipeline loop active...")
    db_pool = app_instance.state.db_pool
    last_standard_drip_at = None

    while True:
        try:
            # 🚀 INSERT THIS LINE RIGHT HERE:
            await check_and_heal_moltset_circuit(db_pool)

            est_now = datetime.datetime.now(NEW_YORK_TZ)
            current_weekday = est_now.weekday()
            current_time = est_now.time()
            current_date_str = est_now.strftime("%Y-%m-%d")

            # OPEN ONE CONNECTION CONTEXT FOR THE ENTIRE TICK'S READ OPERATIONS
            async with db_pool.connection() as conn:
                async with conn.cursor(row_factory=dict_row) as cur:

                    # =====================================================================
                    # 🌟 1. TRIGGER BEACON & DEPENDENT NEXUS OUT-OF-BAND
                    # =====================================================================
                    await cur.execute(
                        """
                        SELECT agent_name, scheduled_time 
                        FROM helix_agent_schedule 
                        WHERE weekday_int = %s AND is_enabled = TRUE 
                        AND scheduled_time IS NOT NULL AND LOWER(agent_name) != 'helix';
                        """,
                        [current_weekday],
                    )
                    time_locked_agents = await cur.fetchall()

            should_trigger_beacon = False
            for row in time_locked_agents:
                # We target beacon specifically for your out-of-band sequential gating rule
                if row["agent_name"].lower() == "beacon":
                    target_time = row["scheduled_time"]
                    target_dt = datetime.datetime.combine(
                        est_now.date(), target_time, tzinfo=NEW_YORK_TZ
                    )
                    # Check if current time falls within the 15-minute operational window
                    if (
                        target_dt
                        <= est_now
                        <= (target_dt + datetime.timedelta(minutes=15))
                    ):
                        should_trigger_beacon = True
                        break

            if should_trigger_beacon:
                # 🛡️ 1a. Pull recent safety locks to prevent tight concurrent firing loops
                async with db_pool.connection() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute("""
                            SELECT COUNT(*) 
                            FROM agent_system_logs 
                            WHERE agent_name = 'beacon' 
                              AND timestamp >= NOW() - INTERVAL '45 minutes';
                        """)
                        recent_safety_lock = (await cur.fetchone())[0]

                if recent_safety_lock == 0:
                    # 🔍 1b. Inspect the absolute latest record in beacon_history to check the directive state
                    nexus_directive_needs_charging = True

                    try:
                        async with db_pool.connection() as conn:
                            async with conn.cursor(row_factory=dict_row) as cur:
                                await cur.execute("""
                                    SELECT nexus_directive 
                                    FROM beacon_history 
                                    ORDER BY id DESC 
                                    LIMIT 1;
                                """)
                                last_beacon_record = await cur.fetchone()

                                # If a record exists and it has a valid directive, we are clear to run Beacon!
                                if (
                                    last_beacon_record
                                    and last_beacon_record.get("nexus_directive")
                                    is not None
                                ):
                                    nexus_directive_needs_charging = False
                    except Exception as db_err:
                        print(f"⚠️ Failed to inspect beacon_history ledger: {db_err}")
                        # Defensive: If table check fails, default to running Nexus for safety

                    # 🧠 1c. Fetch the active strategy vector
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
                        vendor_isolation_matrix = await load_vendor_isolation_matrix(
                            db_pool
                        )

                        # 🚨 THE RECHARGE INTERCEPTOR: If directive is missing/NULL, force Nexus first!
                        if nexus_directive_needs_charging:
                            emit_executive_notification(
                                ExecutiveNotificationEvent(
                                    source_agent="helix",
                                    event_type="pipeline_milestone",
                                    severity="info",
                                    headline="Nexus Interceptor Engaged",
                                    outcome=f"Latest beacon_history row has a NULL directive. Forcing Nexus to charge target strategy.",
                                    next_action="Deploying Nexus out-of-band ahead of Beacon.",
                                )
                            )

                            await trigger_downstream_mesh_with_payload(
                                db_pool,
                                app_instance.state,
                                active_strategy,
                                ["nexus"],
                                vendor_isolation_matrix,
                            )

                            await asyncio.sleep(60)
                            continue

                        # ✅ CLEAR ROADWAY: Directive is active, fire Beacon directly
                        else:
                            emit_executive_notification(
                                ExecutiveNotificationEvent(
                                    source_agent="helix",
                                    event_type="pipeline_milestone",
                                    severity="success",
                                    headline="Beacon Deployment Cleared",
                                    outcome=f"Valid nexus_directive verified in history. Dispatched Beacon for {active_strategy['industry_name']}.",
                                    next_action="Monitor Beacon execution payload structures.",
                                )
                            )

                            await trigger_downstream_mesh_with_payload(
                                db_pool,
                                app_instance.state,
                                active_strategy,
                                ["beacon"],
                                vendor_isolation_matrix,
                            )

            # =====================================================================
            # 🧠 2. DAILY PROMPT OPTIMIZATION TRIGGER (Helix is exempt from DB, runs continuous)
            # =====================================================================
            if est_now.hour == 23:
                if (
                    getattr(app_instance.state, "last_optimization_date", None)
                    != current_date_str
                ):
                    app_instance.state.last_optimization_date = current_date_str
                    emit_executive_notification(
                        ExecutiveNotificationEvent(
                            source_agent="helix",
                            event_type="pipeline_milestone",
                            severity="info",
                            headline="Daily Optimization Started",
                            outcome="Helix prompt optimization cycle is running against latest logs.",
                            next_action="Review optimization completion summary after execution.",
                        )
                    )
                    await run_daily_prompt_optimization(db_pool)

            # =====================================================================
            # 🔄 3. STANDARD SEQUENTIAL DRIP (DYNAMIC DATABASE LOOKUP)
            # =====================================================================
            now_utc = datetime.datetime.now(datetime.timezone.utc)
            if last_standard_drip_at is None or (
                now_utc - last_standard_drip_at
            ) >= datetime.timedelta(minutes=45):

                async with db_pool.connection() as conn:
                    async with conn.cursor(row_factory=dict_row) as cur:
                        # 3a. Pull current enabled continuous agents (where scheduled_time IS NULL)
                        await cur.execute(
                            """
                            SELECT agent_name, max_daily_runs 
                            FROM helix_agent_schedule 
                            WHERE weekday_int = %s AND is_enabled = TRUE 
                            AND scheduled_time IS NULL AND LOWER(agent_name) != 'helix';
                        """,
                            [current_weekday],
                        )
                        db_scheduled_agents = await cur.fetchall()

                        # 3b. Look up total run histories for today to see who has remaining quota
                        await cur.execute("""
                            SELECT DISTINCT LOWER(agent_name) as executed_agent, COUNT(*) as actual_runs
                            FROM agent_system_logs 
                            WHERE timestamp >= (NOW() AT TIME ZONE 'America/New_York')::date
                            GROUP BY LOWER(agent_name);
                        """)
                        log_rows = await cur.fetchall()
                        run_counts_today = {
                            r["executed_agent"]: r["actual_runs"] for r in log_rows
                        }

                # Construct list of eligible continuous loop agents based on current table configuration
                scheduled_agents_list = [
                    row["agent_name"].lower() for row in db_scheduled_agents
                ]

                if not scheduled_agents_list:
                    # Helix falls back safely if table is empty, printing info without stalling core execution loop
                    await write_helix_command_center_log(
                        db_pool,
                        "INFO",
                        f"No dynamic loop agents configured for weekday {current_weekday}.",
                        "ORCHESTRATOR_STANDDOWN",
                    )
                    last_standard_drip_at = now_utc
                    await asyncio.sleep(60)
                    continue

                # Filter out any agents that have met or exceeded their max_daily_runs restriction
                pending_agents_for_today = []
                for row in db_scheduled_agents:
                    agent = row["agent_name"].lower()
                    max_allowed = row["max_daily_runs"]
                    completed_count = run_counts_today.get(agent, 0)
                    if completed_count < max_allowed:
                        pending_agents_for_today.append(agent)

                # Maintain your explicit nova backoff bypass logic rules
                if (
                    "nova" in scheduled_agents_list
                    and "nova" not in pending_agents_for_today
                ):
                    async with db_pool.connection() as conn:
                        async with conn.cursor() as retry_cur:
                            await retry_cur.execute("""
                                SELECT COUNT(*) FROM helix_brain_scheduler 
                                WHERE is_active = TRUE 
                                AND (last_targeted_at IS NULL OR last_targeted_at <= NOW() - (minimum_cooldown_hours || ' hours')::INTERVAL);
                            """)
                            if (await retry_cur.fetchone())[0] > 0:
                                pending_agents_for_today.append("nova")

                if not pending_agents_for_today:
                    await write_helix_command_center_log(
                        db_pool,
                        "INFO",
                        f"All dynamic queue items for weekday {current_weekday} maximized quota allocations. Standing down.",
                        "ORCHESTRATOR_COMPLETE",
                    )
                    last_standard_drip_at = now_utc
                    await asyncio.sleep(60)
                    continue

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
                        elif candidate_agent == "radar" and vendor_isolation_matrix.get(
                            VENDOR_PHANTOMBUSTERS, False
                        ):
                            is_isolated = True
                        elif candidate_agent == "nova" and vendor_isolation_matrix.get(
                            VENDOR_MOLTSET, False
                        ):
                            is_isolated = True
                        elif candidate_agent == "nexus" and vendor_isolation_matrix.get(
                            VENDOR_ANTHROPIC, False
                        ):
                            is_isolated = True

                        if is_isolated:
                            # 1. Check if we have already reported and logged a run for this isolated agent today
                            async with db_pool.connection() as conn:
                                async with conn.cursor() as check_cur:
                                    await check_cur.execute(
                                        """
                                        SELECT COUNT(*) FROM agent_system_logs 
                                        WHERE LOWER(agent_name) = %s 
                                        AND timestamp >= (NOW() AT TIME ZONE 'America/New_York')::date;
                                        """,
                                        (candidate_agent,),
                                    )
                                    already_ran_today = (await check_cur.fetchone())[
                                        0
                                    ] > 0

                            if not already_ran_today:
                                # 2. First time hitting broken circuit today: Report it and record the run
                                vendor_name = (
                                    VENDOR_MOLTSET
                                    if candidate_agent == "nova"
                                    else "Vendor"
                                )

                                emit_executive_notification(
                                    ExecutiveNotificationEvent(
                                        source_agent="helix",
                                        event_type="pipeline_milestone",
                                        severity="warning",
                                        headline=f"{candidate_agent.capitalize()} Circuit Isolation Engaged",
                                        outcome=f"Circuit for {vendor_name} is broken. {candidate_agent.capitalize()} executed once to register state.",
                                        next_action=f"Halting {candidate_agent} schedule until circuit is healed.",
                                    )
                                )

                                await write_helix_command_center_log(
                                    db_pool,
                                    "WARNING",
                                    f"{candidate_agent.capitalize()} circuit is broken ({vendor_name}). Executed single standdown check. Halting agent for today.",
                                    "CIRCUIT_STANDDOWN_LOGGED",
                                )

                                # 3. Resume schedule: Advance the standard drip timer so the loop moves on
                                last_standard_drip_at = now_utc
                                break
                            else:
                                # Already reported and executed once today; bypass until circuit clears
                                continue

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
                            emit_executive_notification(
                                ExecutiveNotificationEvent(
                                    source_agent="helix",
                                    event_type="pipeline_milestone",
                                    severity="info",
                                    headline="Staggered Dispatch",
                                    outcome=f"{next_agent_up} dispatched for {active_strategy['industry_name']}.",
                                    impact=f"Region: {active_strategy.get('target_region') or 'Global'}",
                                    next_action="Track downstream execution milestone in logs.",
                                )
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
            emit_executive_notification(
                ExecutiveNotificationEvent(
                    source_agent="helix",
                    event_type="critical_failure",
                    severity="critical",
                    headline="Core Orchestrator Loop Exception",
                    outcome="Perpetual awareness loop hit an unhandled exception.",
                    impact=str(loop_crash),
                    next_action="Inspect engine logs and dependent service health.",
                )
            )

        await asyncio.sleep(60)
