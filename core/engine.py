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


# In core/engine.py


async def check_and_heal_moltset_circuit(app_instance):
    """
    Checks MoltSet circuit status every 30 minutes.
    Queries /get_account, updates credits_remaining in the DB,
    and automatically heals the circuit if MoltSet returns HTTP 200.
    """
    db_pool = app_instance.state.db_pool

    if not hasattr(app_instance.state, "disabled_circuits"):
        app_instance.state.disabled_circuits = set()

    # 1. Enforce 30-Minute Probe Cooldown
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    last_probe = getattr(app_instance.state, "last_moltset_probe_time", None)

    if last_probe and (now_utc - last_probe) < datetime.timedelta(minutes=30):
        return  # Stand down until the 30-minute window elapses

    # 2. Check if circuit is currently broken in DB
    try:
        async with db_pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute("""
                    SELECT is_circuit_broken, credits_remaining 
                    FROM helix_infrastructure_oracle_ledger 
                    WHERE LOWER(vendor_name) = 'moltset';
                """)
                ledger_record = await cur.fetchone()

        is_broken = (
            ledger_record.get("is_circuit_broken", True) if ledger_record else False
        )
    except Exception as db_err:
        print(f"⚠️ Failed to read MoltSet ledger state: {db_err}")
        return

    # If the circuit is healthy, clean state and exit
    if not is_broken:
        app_instance.state.disabled_circuits.discard("moltset")
        return

    # 3. Circuit IS broken — Ping MoltSet /get_account to probe balance & rate limits
    moltset_key = os.getenv("MOLTSET_API_KEY")
    api_endpoint = os.getenv(
        "MOLTSET_API_ENDPOINT", "https://api.moltsets.com/api/v1/tools/"
    ).strip()

    if not moltset_key:
        return

    # Update last probe attempt timestamp
    app_instance.state.last_moltset_probe_time = now_utc

    url = f"{api_endpoint.rstrip('/')}/get_account"
    headers = {
        "Authorization": f"Bearer {moltset_key}",
        "Content-Type": "application/json",
    }

    try:
        loop = asyncio.get_event_loop()
        res = await loop.run_in_executor(
            None, lambda: requests.post(url, headers=headers, timeout=12)
        )

        # 4. Process HTTP Response & Parse Available Balance/Credits
        if res.status_code == 200:
            data = res.json() or {}

            # Extract credits/tokens from MoltSet response payload safely
            # (Fallback to 100.00 if balance keys aren't in payload)
            tokens_remaining = (
                data.get("tokens_remaining")
                or data.get("credits")
                or data.get("balance")
                or 100.00
            )

            async with db_pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        UPDATE helix_infrastructure_oracle_ledger 
                        SET is_circuit_broken = FALSE,
                            credits_remaining = %s,
                            last_updated_at = NOW() 
                        WHERE LOWER(vendor_name) = 'moltset';
                    """,
                        (float(tokens_remaining),),
                    )
                await conn.commit()

            # Restore in-memory state
            app_instance.state.disabled_circuits.discard("moltset")

            print(
                f"🟢 [CIRCUIT HEALED] MoltSet API returned 200 OK. Balance: {tokens_remaining}. Circuit restored."
            )

            emit_executive_notification(
                ExecutiveNotificationEvent(
                    source_agent="helix",
                    event_type="pipeline_milestone",
                    severity="success",
                    headline="MoltSet Circuit Healed",
                    outcome=f"MoltSet /get_account returned 200 OK (Credits: {tokens_remaining}). Restored automated dispatches.",
                    next_action="Nova dispatches resumed.",
                )
            )
        else:
            print(
                f"⏳ [MOLTSET PROBE] Probe returned HTTP {res.status_code}. Circuit remains broken. Next check in 30m."
            )

    except Exception as e:
        print(f"⚠️ Circuit healing probe failed against MoltSet portal: {e}")


async def load_vendor_isolation_matrix(db_pool) -> dict:
    """
    Loads the infrastructure ledger and evaluates circuit status.
    Enforces vendor isolation whenever is_circuit_broken is True.
    """
    vendor_isolation_matrix = {}

    try:
        async with db_pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    "SELECT vendor_name, is_circuit_broken FROM helix_infrastructure_oracle_ledger;"
                )
                rows = await cur.fetchall()

                for row in rows:
                    v_name = row["vendor_name"]
                    is_broken = bool(row["is_circuit_broken"])

                    # Strictly enforce circuit breaker state across all vendors
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
            await check_and_heal_moltset_circuit(app_instance)

            est_now = datetime.datetime.now(NEW_YORK_TZ)
            current_weekday = int(est_now.strftime("%w"))
            current_date_str = est_now.strftime("%Y-%m-%d")

            # Define morning execution window (e.g., 4:30 AM EST pre-flight)
            if est_now.hour == 4 and est_now.minute == 30:
                preflight_key = f"preflight_{current_date_str}"
                if (
                    getattr(app_instance.state, "last_preflight_date", None)
                    != preflight_key
                ):
                    app_instance.state.last_preflight_date = preflight_key

                    # Run proactive audit before the main morning drip fires!
                    from core.auditor import run_preflight_audit

                    await run_preflight_audit(db_pool)

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
                if row["agent_name"].lower() == "beacon":
                    target_time = row["scheduled_time"]
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
                            SELECT COUNT(*) 
                            FROM agent_system_logs 
                            WHERE agent_name = 'beacon' 
                              AND timestamp >= NOW() - INTERVAL '45 minutes';
                        """)
                        recent_safety_lock = (await cur.fetchone())[0]

                if recent_safety_lock == 0:
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

                                if (
                                    last_beacon_record
                                    and last_beacon_record.get("nexus_directive")
                                    is not None
                                ):
                                    nexus_directive_needs_charging = False
                    except Exception as db_err:
                        print(f"⚠️ Failed to inspect beacon_history ledger: {db_err}")

                    async with db_pool.connection() as conn:
                        async with conn.cursor(row_factory=dict_row) as cur:
                            await cur.execute("""
                                SELECT industry_id, industry_name, moltset_category, bottleneck_profile, pain_tokens, target_region, campaign_strategy
                                FROM helix_brain_scheduler 
                                WHERE is_active = TRUE 
                                ORDER BY last_targeted_at ASC NULLS FIRST, priority_score DESC LIMIT 1;
                            """)
                            active_strategy = await cur.fetchone()

                            if active_strategy:
                                await cur.execute(
                                    "UPDATE helix_brain_scheduler SET last_targeted_at = NOW() WHERE industry_id = %s;",
                                    (active_strategy["industry_id"],),
                                )
                        await conn.commit()

                    if active_strategy:
                        vendor_isolation_matrix = await load_vendor_isolation_matrix(
                            db_pool
                        )

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
            # 🧠 2. DAILY PROMPT OPTIMIZATION TRIGGER
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

                scheduled_agents_list = [
                    row["agent_name"].lower() for row in db_scheduled_agents
                ]

                if not scheduled_agents_list:
                    await write_helix_command_center_log(
                        db_pool,
                        "INFO",
                        f"No dynamic loop agents configured for weekday {current_weekday}.",
                        "ORCHESTRATOR_STANDDOWN",
                    )
                    last_standard_drip_at = now_utc
                    await asyncio.sleep(60)
                    continue

                pending_agents_for_today = []
                for row in db_scheduled_agents:
                    agent = row["agent_name"].lower()
                    max_allowed = row["max_daily_runs"]
                    completed_count = run_counts_today.get(agent, 0)
                    if completed_count < max_allowed:
                        pending_agents_for_today.append(agent)

                # Maintain Nova backoff bypass logic with auto-recycle fallback
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
                            runnable_count = (await retry_cur.fetchone())[0]

                            # 🌟 OPTION B FALLBACK: If 0 targets are eligible due to 24h cooldown, auto-recycle!
                            if runnable_count == 0:
                                await retry_cur.execute("""
                                    UPDATE helix_brain_scheduler 
                                    SET last_targeted_at = NULL 
                                    WHERE is_active = TRUE;
                                """)
                                await conn.commit()
                                print(
                                    "🔄 [SCHEDULER RECYCLE] Auto-cleared cooldown timestamps for Nova bypass."
                                )
                                pending_agents_for_today.append("nova")
                            elif runnable_count > 0:
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
                                vendor_name = (
                                    VENDOR_MOLTSET
                                    if candidate_agent == "nova"
                                    else "Vendor Dependency"
                                )

                                emit_executive_notification(
                                    ExecutiveNotificationEvent(
                                        source_agent="helix",
                                        event_type="pipeline_milestone",
                                        severity="warning",
                                        headline=f"{candidate_agent.capitalize()} Circuit Isolation Engaged",
                                        outcome=f"Circuit for {vendor_name} is broken. Registered standdown state for {candidate_agent.capitalize()}.",
                                        next_action=f"Halting {candidate_agent} dispatches until circuit heals.",
                                    )
                                )

                                async with db_pool.connection() as conn:
                                    async with conn.cursor() as log_cur:
                                        await log_cur.execute(
                                            """
                                            INSERT INTO agent_system_logs (agent_name, log_level, message, metadata) 
                                            VALUES (%s, 'WARNING', %s, %s::jsonb);
                                            """,
                                            (
                                                candidate_agent,
                                                f"Circuit broken ({vendor_name}). Registered single daily standdown check. Halting dispatches today.",
                                                '{"action": "CIRCUIT_STANDDOWN_LOGGED"}',
                                            ),
                                        )
                                    await conn.commit()

                            continue

                        next_agent_up = candidate_agent
                        break

                    if next_agent_up:
                        async with db_pool.connection() as conn:
                            async with conn.cursor(row_factory=dict_row) as cur:
                                if next_agent_up in ["radar", "pulsar"]:
                                    await cur.execute("""
                                        SELECT industry_id, industry_name, moltset_category, bottleneck_profile, pain_tokens, campaign_strategy, target_region
                                        FROM helix_brain_scheduler WHERE is_active = TRUE 
                                        ORDER BY last_targeted_at DESC NULLS LAST LIMIT 1;
                                    """)
                                    active_strategy = await cur.fetchone()
                                else:
                                    # 🌟 OPTION B SELECTION LOGIC: Check if runnable strategies exist first
                                    await cur.execute("""
                                        SELECT COUNT(*) as valid_targets
                                        FROM helix_brain_scheduler WHERE is_active = TRUE 
                                        AND (last_targeted_at IS NULL OR last_targeted_at <= NOW() - (minimum_cooldown_hours || ' hours')::INTERVAL);
                                    """)
                                    target_check = await cur.fetchone()

                                    # If ALL active targets are locked in 24h cooldown, recycle them automatically!
                                    if (
                                        target_check
                                        and target_check["valid_targets"] == 0
                                    ):
                                        await cur.execute("""
                                            UPDATE helix_brain_scheduler 
                                            SET last_targeted_at = NULL 
                                            WHERE is_active = TRUE;
                                        """)
                                        await conn.commit()
                                        print(
                                            "🔄 [SCHEDULER RECYCLE] Auto-cleared cooldown timestamps for standard dispatch."
                                        )

                                    await cur.execute("""
                                        SELECT industry_id, industry_name, moltset_category, bottleneck_profile, pain_tokens, campaign_strategy, target_region
                                        FROM helix_brain_scheduler WHERE is_active = TRUE 
                                        AND (last_targeted_at IS NULL OR last_targeted_at <= NOW() - (minimum_cooldown_hours || ' hours')::INTERVAL)
                                        ORDER BY last_targeted_at ASC NULLS FIRST, priority_score DESC LIMIT 1;
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
                        print(
                            "⏳ [ENGINE] All pending queue items are blocked by circuit isolation. Waiting..."
                        )
                        last_standard_drip_at = now_utc

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
