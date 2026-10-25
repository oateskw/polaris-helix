import asyncio
import datetime
import math
import os
import requests
import traceback
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
NEXUS_MORNING_HOUR = int(os.getenv("NEXUS_MORNING_HOUR", "6"))
# When true, scheduler selection puts priority_score ahead of rotation order, so
# approved strategy proposals take effect. Off by default: it changes Nova rotation.
PRIORITY_FIRST_ORDERING = (
    os.getenv("HELIX_PRIORITY_FIRST_ORDERING", "false").strip().lower() == "true"
)
SCHEDULER_ORDER_BY = (
    "priority_score DESC, last_targeted_at ASC NULLS FIRST"
    if PRIORITY_FIRST_ORDERING
    else "last_targeted_at ASC NULLS FIRST, priority_score DESC"
)

MOLTSET_RECOVERY_COOLDOWN = datetime.timedelta(minutes=30)
NOVA_DAILY_RESERVE_RATIO = float(os.getenv("NOVA_DAILY_RESERVE_RATIO", "0.10"))
NOVA_EXPECTED_RECORDS_PER_DOMAIN = int(
    os.getenv("NOVA_EXPECTED_RECORDS_PER_DOMAIN", "25")
)
NOVA_THRESHOLD_ALERT_DEBOUNCE_HOURS = int(
    os.getenv("NOVA_THRESHOLD_ALERT_DEBOUNCE_HOURS", "6")
)
NOVA_DEFAULT_RECORDS_PER_PEOPLE_QUERY = int(
    os.getenv("NOVA_DEFAULT_RECORDS_PER_PEOPLE_QUERY", "150")
)
NOVA_BUDGET_DEFERRAL_ALERT_COOLDOWN_MINUTES = int(
    os.getenv("NOVA_BUDGET_DEFERRAL_ALERT_COOLDOWN_MINUTES", "720")
)


def parse_iso_datetime(value: str | None) -> datetime.datetime | None:
    if not value:
        return None
    try:
        return datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def parse_moltset_records_1w_window(data: dict) -> dict[str, float | str] | None:
    account = data.get("results") or {}
    if data.get("status") != "ok" or account.get("status") != "active":
        return None

    records_1w = (
        account.get("fair_use", {}).get("search", {}).get("records", {}).get("1w", {})
    )
    limit = records_1w.get("limit")
    used = records_1w.get("used")
    remaining = records_1w.get("remaining")
    values = [limit, used, remaining]
    if any(isinstance(v, bool) for v in values) or not all(
        isinstance(v, (int, float)) for v in values
    ):
        return None

    return {
        "limit": float(limit),
        "used": float(used),
        "remaining": float(remaining),
        "resets_at": str(records_1w.get("resets_at") or ""),
    }


def compute_daily_record_budget(
    records_remaining_1w: float,
    resets_at_iso: str,
    now_utc: datetime.datetime,
    expected_records_per_domain: int,
    reserve_ratio: float,
) -> tuple[int, int, int]:
    if records_remaining_1w <= 0:
        return 0, 0, 1

    reset_dt = parse_iso_datetime(resets_at_iso)
    if reset_dt is None:
        remaining_days = 1
    else:
        delta_seconds = max(0.0, (reset_dt - now_utc).total_seconds())
        remaining_days = max(1, int(math.ceil(delta_seconds / 86400.0)))

    expected_floor = max(1, int(expected_records_per_domain))
    reserve = max(expected_floor, int(records_remaining_1w * max(0.0, reserve_ratio)))
    usable = max(0, int(records_remaining_1w) - reserve)
    if usable <= 0:
        return min(int(records_remaining_1w), expected_floor), reserve, remaining_days

    daily_budget = max(expected_floor, usable // remaining_days)
    daily_budget = min(daily_budget, int(records_remaining_1w))
    return daily_budget, reserve, remaining_days


async def fetch_moltset_records_1w_window() -> dict[str, float | str] | None:
    moltset_key = os.getenv("MOLTSET_API_KEY")
    api_endpoint = os.getenv(
        "MOLTSET_API_ENDPOINT", "https://api.moltsets.com/api/v1/tools/"
    ).strip()
    if not moltset_key:
        return None

    url = f"{api_endpoint.rstrip('/')}/get_account"
    headers = {
        "Authorization": f"Bearer {moltset_key}",
        "Content-Type": "application/json",
    }
    loop = asyncio.get_event_loop()
    try:
        res = await loop.run_in_executor(
            None, lambda: requests.post(url, headers=headers, json={}, timeout=12)
        )
        if res.status_code != 200:
            return None
        return parse_moltset_records_1w_window(res.json() or {})
    except Exception:
        return None


async def estimate_nova_records_per_people_query(db_pool) -> int:
    async with db_pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("""
                SELECT
                    COALESCE(NULLIF(metadata->'extra_context'->>'records_consumed_1w', '')::numeric, 0) AS records_consumed,
                    COALESCE(NULLIF(metadata->'extra_context'->>'api_calls_charged', '')::numeric, 0) AS api_calls_charged
                FROM agent_system_logs
                WHERE LOWER(agent_name) = 'nova'
                  AND timestamp >= (NOW() AT TIME ZONE 'America/New_York')::date
                  AND metadata->>'lifecycle_event' IN ('RUN_COMPLETED', 'RUN_RATE_LIMITED')
                ORDER BY timestamp DESC
                LIMIT 5;
                """)
            rows = await cur.fetchall()

    ratios: list[float] = []
    for row in rows:
        consumed = row.get("records_consumed") if isinstance(row, dict) else None
        calls = row.get("api_calls_charged") if isinstance(row, dict) else None
        try:
            consumed_value = float(consumed or 0)
            calls_value = float(calls or 0)
        except (TypeError, ValueError):
            continue
        if consumed_value > 0 and calls_value > 0:
            ratios.append(consumed_value / calls_value)

    if not ratios:
        return NOVA_DEFAULT_RECORDS_PER_PEOPLE_QUERY

    return max(1, int(math.ceil(max(ratios))))


async def get_nova_consumed_records_today(db_pool, estimated_per_request: int) -> int:
    async with db_pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("""
                SELECT metadata
                FROM agent_system_logs
                WHERE LOWER(agent_name) = 'nova'
                  AND timestamp >= (NOW() AT TIME ZONE 'America/New_York')::date
                  AND metadata->>'lifecycle_event' IN ('RUN_COMPLETED', 'RUN_RATE_LIMITED');
                """)
            rows = await cur.fetchall()

    total = 0
    for row in rows:
        meta = row.get("metadata") if isinstance(row, dict) else None
        if not isinstance(meta, dict):
            continue
        extra = meta.get("extra_context")
        if not isinstance(extra, dict):
            extra = {}
        consumed = extra.get("records_consumed_1w")
        try:
            if consumed is None:
                calls = extra.get("api_calls_charged") or 0
                total += max(0, int(calls)) * max(1, int(estimated_per_request))
            else:
                total += max(0, int(consumed or 0))
        except (TypeError, ValueError):
            continue
    return total


def should_emit_threshold_change_alert(
    app_state, old_limit: float, new_limit: float
) -> bool:
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    state = getattr(app_state, "nova_threshold_alert_state", None)
    if not isinstance(state, dict):
        state = {}
    last_transition = state.get("last_transition")
    last_sent_at = state.get("last_sent_at")
    if isinstance(last_sent_at, str):
        last_sent_at = parse_iso_datetime(last_sent_at)

    transition = (int(old_limit), int(new_limit))
    if (
        last_transition == transition
        and isinstance(last_sent_at, datetime.datetime)
        and (now_utc - last_sent_at)
        < datetime.timedelta(hours=NOVA_THRESHOLD_ALERT_DEBOUNCE_HOURS)
    ):
        return False

    app_state.nova_threshold_alert_state = {
        "last_transition": transition,
        "last_sent_at": now_utc.isoformat(),
    }
    return True


def notify_nova_budget_deferral(
    app_state,
    *,
    today_et_key: str,
    daily_budget_records: int,
    consumed_today: int,
    estimated_records_per_people_query: int,
    weekly_remaining: int,
    weekly_limit: int,
    weekly_resets_at: str,
    reason: str,
) -> None:
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    state = getattr(app_state, "nova_budget_deferral_alert_state", None)
    if not isinstance(state, dict):
        state = {}
    last_sent_at = parse_iso_datetime(state.get("last_sent_at"))
    last_reason = state.get("last_reason")
    last_day = state.get("last_day")

    if (
        last_reason == reason
        and last_day == today_et_key
        and isinstance(last_sent_at, datetime.datetime)
        and (now_utc - last_sent_at)
        < datetime.timedelta(minutes=NOVA_BUDGET_DEFERRAL_ALERT_COOLDOWN_MINUTES)
    ):
        return

    if reason == "DAILY_BUDGET_REACHED":
        outcome = (
            "Nova was held back to level out the weekly allotment. "
            f"Today's Nova budget is exhausted ({consumed_today}/{daily_budget_records} records)."
        )
    else:
        outcome = (
            "Nova was held back to level out the weekly allotment. "
            f"The remaining daily budget ({daily_budget_records - consumed_today} records) "
            "is below the safe floor for one projected Nova request "
            f"(~{estimated_records_per_people_query} records/request)."
        )

    emit_executive_notification(
        ExecutiveNotificationEvent(
            source_agent="nova",
            event_type="budget_deferral",
            severity="warning",
            headline="Nova Budget Deferral",
            outcome=outcome,
            impact=(
                f"Weekly remaining: {weekly_remaining} of {weekly_limit} until {weekly_resets_at}."
            ),
            next_action="No action required unless Nova budget constraints persist unexpectedly.",
        )
    )
    app_state.nova_budget_deferral_alert_state = {
        "last_reason": reason,
        "last_day": today_et_key,
        "last_sent_at": now_utc.isoformat(),
    }


def parse_moltset_search_capacity(data: dict) -> float | None:
    account = data.get("results") or {}
    requests_remaining = (
        account.get("fair_use", {})
        .get("search", {})
        .get("requests", {})
        .get("5h", {})
        .get("remaining")
    )
    records_remaining_5h = (
        account.get("fair_use", {})
        .get("search", {})
        .get("records", {})
        .get("5h", {})
        .get("remaining")
    )
    records_remaining_1w = (
        account.get("fair_use", {})
        .get("search", {})
        .get("records", {})
        .get("1w", {})
        .get("remaining")
    )

    remaining_values = [
        requests_remaining,
        records_remaining_5h,
        records_remaining_1w,
    ]

    if (
        data.get("status") != "ok"
        or account.get("status") != "active"
        or any(isinstance(v, bool) for v in remaining_values)
        or not all(isinstance(v, (int, float)) for v in remaining_values)
    ):
        return None

    # Return the tightest search fair-use ceiling so callers can fail closed.
    return float(min(remaining_values))


def is_moltset_probe_due(
    last_updated_at: datetime.datetime | None,
    now_utc: datetime.datetime | None = None,
) -> bool:
    if last_updated_at is None:
        return False
    if last_updated_at.tzinfo is None:
        last_updated_at = last_updated_at.replace(tzinfo=datetime.timezone.utc)
    check_time = now_utc or datetime.datetime.now(datetime.timezone.utc)
    return check_time - last_updated_at >= MOLTSET_RECOVERY_COOLDOWN


async def check_and_heal_moltset_circuit(app_instance) -> bool:
    """
    Checks MoltSet circuit status every 30 minutes.
    Queries the quota-free /get_account endpoint before healing the circuit.
    """
    db_pool = app_instance.state.db_pool

    if not hasattr(app_instance.state, "disabled_circuits"):
        app_instance.state.disabled_circuits = set()

    now_utc = datetime.datetime.now(datetime.timezone.utc)

    # 2. Check if circuit is currently broken in DB
    try:
        async with db_pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute("""
                    SELECT is_circuit_broken, credits_remaining, last_updated_at
                    FROM helix_infrastructure_oracle_ledger 
                    WHERE LOWER(vendor_name) = 'moltset';
                """)
                ledger_record = await cur.fetchone()

        is_broken = (
            ledger_record.get("is_circuit_broken", False) if ledger_record else False
        )
    except Exception as db_err:
        print(f"⚠️ Failed to read MoltSet ledger state: {db_err}")
        return False

    # If the database already shows the circuit is healthy, clear memory state & exit cleanly
    if not is_broken:
        app_instance.state.disabled_circuits.discard("moltset")
        return False

    app_instance.state.disabled_circuits.add("moltset")
    if not is_moltset_probe_due(ledger_record.get("last_updated_at"), now_utc):
        return False

    # 3. Circuit IS broken — Probe MoltSet get_account endpoint
    moltset_key = os.getenv("MOLTSET_API_KEY")
    api_endpoint = os.getenv(
        "MOLTSET_API_ENDPOINT", "https://api.moltsets.com/api/v1/tools/"
    ).strip()

    if not moltset_key:
        return False

    # Atomically claim this recovery probe. This persists the next cooldown and
    # prevents multiple Render instances from probing or healing concurrently.
    async with db_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                UPDATE helix_infrastructure_oracle_ledger
                SET last_updated_at = NOW()
                WHERE LOWER(vendor_name) = 'moltset'
                  AND is_circuit_broken = TRUE
                  AND last_updated_at <= NOW() - INTERVAL '30 minutes'
                RETURNING vendor_name;
            """)
            claimed_probe = await cur.fetchone()
        await conn.commit()

    if not claimed_probe:
        return False

    await write_helix_command_center_log(
        db_pool,
        "INFO",
        "Probing MoltSet account capacity after the 30-minute circuit cooldown.",
        "MOLTSET_RECOVERY_PROBE",
        "MoltSet",
    )

    url = f"{api_endpoint.rstrip('/')}/get_account"
    headers = {
        "Authorization": f"Bearer {moltset_key}",
        "Content-Type": "application/json",
    }

    try:
        loop = asyncio.get_event_loop()
        res = await loop.run_in_executor(
            None, lambda: requests.post(url, headers=headers, json={}, timeout=12)
        )

        # 4. Process HTTP response and parse strict search fair-use capacity.
        if res.status_code == 200:
            data = res.json() or {}
            requests_remaining = parse_moltset_search_capacity(data)

            if requests_remaining is not None and requests_remaining > 0:
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
                            (requests_remaining,),
                        )
                    await conn.commit()

                # Restore in-memory state
                app_instance.state.disabled_circuits.discard("moltset")

                print(
                    f"🟢 [CIRCUIT HEALED] MoltSet get_account reported positive search fair-use capacity ({requests_remaining} minimum remaining). Circuit restored."
                )

                emit_executive_notification(
                    ExecutiveNotificationEvent(
                        source_agent="helix",
                        event_type="pipeline_milestone",
                        severity="success",
                        headline="MoltSet Circuit Healed",
                        outcome=f"MoltSet get_account reported positive search fair-use capacity ({requests_remaining} minimum remaining across requests/records windows). Restored automated dispatches.",
                        next_action="Nova dispatches resumed.",
                    )
                )
                await write_helix_command_center_log(
                    db_pool,
                    "INFO",
                    f"MoltSet circuit healed from quota-free account capacity verification ({requests_remaining} minimum remaining across required search fair-use windows).",
                    "MOLTSET_CIRCUIT_HEALED",
                    "MoltSet",
                    {"requests_remaining": requests_remaining},
                )
                return True
            else:
                print(
                    "⏳ [MOLTSET PROBE] get_account did not report positive capacity across required search fair-use windows. Circuit remains broken."
                )
                await write_helix_command_center_log(
                    db_pool,
                    "WARNING",
                    "MoltSet recovery probe found no positive capacity across required search fair-use windows.",
                    "MOLTSET_RECOVERY_DEFERRED",
                    "MoltSet",
                )
        else:
            print(
                f"⏳ [MOLTSET PROBE] get_account probe failed with status {res.status_code}. Circuit remains broken."
            )
            await write_helix_command_center_log(
                db_pool,
                "WARNING",
                f"MoltSet recovery probe returned HTTP {res.status_code}; circuit remains broken.",
                "MOLTSET_RECOVERY_FAILED",
                "MoltSet",
                {"http_status": res.status_code},
            )

    except Exception as e:
        print(f"⚠️ Circuit healing probe failed against MoltSet portal: {e}")
        await write_helix_command_center_log(
            db_pool,
            "ERROR",
            f"MoltSet recovery probe raised {type(e).__name__}; circuit remains broken.",
            "MOLTSET_RECOVERY_FAILED",
            "MoltSet",
            {"error_type": type(e).__name__},
        )

    return False


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


async def dispatch_nova_if_due(app_instance) -> None:
    """Dispatch Nova every 30 minutes with daily record budget leveling."""
    db_pool = app_instance.state.db_pool
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    now_et = datetime.datetime.now(NEW_YORK_TZ)
    today_et_key = now_et.strftime("%Y-%m-%d")
    async with db_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                SELECT COUNT(*)
                FROM agent_system_logs
                WHERE agent_name = 'helix'
                  AND timestamp >= NOW() - INTERVAL '30 minutes'
                  AND metadata->'command_center_view'->>'action_taken' IN (
                      'DISPATCH_NOVA_30_MINUTE',
                      'NOVA_DAILY_BUDGET_DISPATCHED',
                      'NOVA_DAILY_BUDGET_DEFERRED',
                      'DISPATCH_NOVA_REJECTED',
                      'SKIP_NOVA_ISOLATION'
                  );
            """)
            recent_attempt = (await cur.fetchone())[0]

    if recent_attempt:
        return

    vendor_isolation_matrix = await load_vendor_isolation_matrix(db_pool)
    if vendor_isolation_matrix.get(VENDOR_MOLTSET, False):
        return

    threshold_window = await fetch_moltset_records_1w_window()
    if not threshold_window:
        threshold_failures = (
            int(getattr(app_instance.state, "nova_threshold_unavailable_count", 0)) + 1
        )
        app_instance.state.nova_threshold_unavailable_count = threshold_failures
        await write_helix_command_center_log(
            db_pool,
            "WARNING",
            "MoltSet threshold metadata unavailable; Nova dispatch deferred until threshold telemetry recovers.",
            "NOVA_DAILY_BUDGET_DEFERRED",
            "nova",
            {"reason": "THRESHOLD_UNAVAILABLE", "failure_count": threshold_failures},
        )
        if threshold_failures >= 2:
            send_telegram_broadcast(
                "⚠️ MoltSet Threshold Metadata Unavailable: Nova is running in conservative pacing mode until threshold telemetry recovers.",
                "critical",
            )
        return

    app_instance.state.nova_threshold_unavailable_count = 0
    weekly_limit = float(threshold_window["limit"])
    weekly_used = float(threshold_window["used"])
    weekly_remaining = float(threshold_window["remaining"])
    weekly_resets_at = str(threshold_window.get("resets_at") or "")

    last_weekly_resets_at = getattr(
        app_instance.state, "nova_last_weekly_resets_at", None
    )
    if (
        last_weekly_resets_at
        and weekly_resets_at
        and weekly_resets_at != last_weekly_resets_at
    ):
        await write_helix_command_center_log(
            db_pool,
            "INFO",
            "Detected new MoltSet weekly window. Resetting Nova pacing state.",
            "NOVA_WEEKLY_WINDOW_RESET_DETECTED",
            "nova",
            {
                "previous_resets_at": last_weekly_resets_at,
                "current_resets_at": weekly_resets_at,
            },
        )
    app_instance.state.nova_last_weekly_resets_at = weekly_resets_at

    last_weekly_limit = getattr(app_instance.state, "nova_last_weekly_limit", None)
    if (
        isinstance(last_weekly_limit, (int, float))
        and float(last_weekly_limit) != weekly_limit
        and should_emit_threshold_change_alert(
            app_instance.state, float(last_weekly_limit), weekly_limit
        )
    ):
        change_delta = (
            "increased" if weekly_limit > float(last_weekly_limit) else "decreased"
        )
        severity = "INFO" if weekly_limit > float(last_weekly_limit) else "WARNING"
        daily_budget_preview, reserve_preview, days_preview = (
            compute_daily_record_budget(
                weekly_remaining,
                weekly_resets_at,
                now_utc,
                NOVA_EXPECTED_RECORDS_PER_DOMAIN,
                NOVA_DAILY_RESERVE_RATIO,
            )
        )
        await write_helix_command_center_log(
            db_pool,
            severity,
            f"MoltSet weekly records limit {change_delta} from {int(last_weekly_limit)} to {int(weekly_limit)}.",
            "NOVA_THRESHOLD_LIMIT_CHANGED",
            "nova",
            {
                "old_limit": int(last_weekly_limit),
                "new_limit": int(weekly_limit),
                "daily_budget_preview": daily_budget_preview,
                "reserve_preview": reserve_preview,
                "remaining_days_preview": days_preview,
                "resets_at": weekly_resets_at,
            },
        )
        if weekly_limit > float(last_weekly_limit):
            send_telegram_broadcast(
                (
                    f"📈 MoltSet Plan Change Detected: Weekly records limit increased from `{int(last_weekly_limit)}` "
                    f"to `{int(weekly_limit)}`. Nova daily pacing recalibrated to `{daily_budget_preview}` "
                    f"records/day until `{weekly_resets_at}`."
                ),
                "verbose",
            )
        else:
            send_telegram_broadcast(
                (
                    f"📉 MoltSet Plan Change Detected: Weekly records limit decreased from `{int(last_weekly_limit)}` "
                    f"to `{int(weekly_limit)}`. Nova daily pacing tightened to `{daily_budget_preview}` "
                    f"records/day until `{weekly_resets_at}` to avoid weekly exhaustion."
                ),
                "critical",
            )

    app_instance.state.nova_last_weekly_limit = weekly_limit
    app_instance.state.nova_last_weekly_used = weekly_used

    estimated_records_per_people_query = await estimate_nova_records_per_people_query(
        db_pool
    )
    daily_budget_records, dynamic_reserve, remaining_run_days = (
        compute_daily_record_budget(
            weekly_remaining,
            weekly_resets_at,
            now_utc,
            NOVA_EXPECTED_RECORDS_PER_DOMAIN,
            NOVA_DAILY_RESERVE_RATIO,
        )
    )
    consumed_today = await get_nova_consumed_records_today(
        db_pool, estimated_records_per_people_query
    )

    await write_helix_command_center_log(
        db_pool,
        "INFO",
        "Computed Nova daily budget allocation from live MoltSet weekly thresholds.",
        "NOVA_DAILY_BUDGET_COMPUTED",
        "nova",
        {
            "date_et": today_et_key,
            "records_1w_limit": int(weekly_limit),
            "records_1w_used": int(weekly_used),
            "records_1w_remaining": int(weekly_remaining),
            "records_1w_resets_at": weekly_resets_at,
            "remaining_run_days": remaining_run_days,
            "dynamic_reserve_records": dynamic_reserve,
            "daily_budget_records": daily_budget_records,
            "estimated_records_per_people_query": estimated_records_per_people_query,
            "consumed_today_records": consumed_today,
        },
    )

    if consumed_today >= daily_budget_records:
        await write_helix_command_center_log(
            db_pool,
            "INFO",
            "Nova daily budget reached. Deferring additional dispatches until next ET day.",
            "NOVA_DAILY_BUDGET_DEFERRED",
            "nova",
            {
                "date_et": today_et_key,
                "daily_budget_records": daily_budget_records,
                "consumed_today_records": consumed_today,
                "reason": "DAILY_BUDGET_REACHED",
            },
        )
        notify_nova_budget_deferral(
            app_state=app_instance.state,
            today_et_key=today_et_key,
            daily_budget_records=daily_budget_records,
            consumed_today=consumed_today,
            estimated_records_per_people_query=estimated_records_per_people_query,
            weekly_remaining=int(weekly_remaining),
            weekly_limit=int(weekly_limit),
            weekly_resets_at=weekly_resets_at,
            reason="DAILY_BUDGET_REACHED",
        )
        return

    remaining_budget_today = max(0, daily_budget_records - consumed_today)
    if remaining_budget_today < estimated_records_per_people_query:
        await write_helix_command_center_log(
            db_pool,
            "INFO",
            "Nova deferred because remaining daily budget cannot safely fund one projected people query.",
            "NOVA_DAILY_BUDGET_DEFERRED",
            "nova",
            {
                "date_et": today_et_key,
                "daily_budget_records": daily_budget_records,
                "consumed_today_records": consumed_today,
                "estimated_records_per_people_query": estimated_records_per_people_query,
                "reason": "INSUFFICIENT_BUDGET_FOR_SINGLE_QUERY",
            },
        )
        notify_nova_budget_deferral(
            app_state=app_instance.state,
            today_et_key=today_et_key,
            daily_budget_records=daily_budget_records,
            consumed_today=consumed_today,
            estimated_records_per_people_query=estimated_records_per_people_query,
            weekly_remaining=int(weekly_remaining),
            weekly_limit=int(weekly_limit),
            weekly_resets_at=weekly_resets_at,
            reason="INSUFFICIENT_BUDGET_FOR_SINGLE_QUERY",
        )
        return

    max_people_queries_for_run = max(
        1, int(remaining_budget_today // estimated_records_per_people_query)
    )

    async with db_pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(f"""
                SELECT industry_id, industry_name, moltset_category,
                       bottleneck_profile, pain_tokens, target_region,
                       campaign_strategy
                FROM helix_brain_scheduler
                WHERE is_active = TRUE
                ORDER BY {SCHEDULER_ORDER_BY}
                LIMIT 1;
            """)
            strategy = await cur.fetchone()
            if strategy:
                strategy["daily_record_budget"] = int(remaining_budget_today)
                strategy["max_people_queries_for_run"] = int(max_people_queries_for_run)
                strategy["estimated_records_per_people_query"] = int(
                    estimated_records_per_people_query
                )
                strategy["budget_day_key"] = today_et_key
                strategy["records_1w_limit_current"] = int(weekly_limit)
                strategy["records_1w_resets_at"] = weekly_resets_at
                await cur.execute(
                    "UPDATE helix_brain_scheduler SET last_targeted_at = NOW() WHERE industry_id = %s;",
                    (strategy["industry_id"],),
                )
        await conn.commit()

    if not strategy:
        return

    dispatch_results = await trigger_downstream_mesh_with_payload(
        db_pool,
        app_instance.state,
        strategy,
        ["nova"],
        vendor_isolation_matrix,
    )
    if not dispatch_results.get("nova", False):
        return

    await write_helix_command_center_log(
        db_pool,
        "INFO",
        f"Completed quota-aware Nova dispatch for {strategy['industry_name']}.",
        "NOVA_DAILY_BUDGET_DISPATCHED",
        strategy["industry_name"],
        {
            "strategy_id": strategy["industry_id"],
            "run_id": strategy.get("_nova_run_id"),
            "lifecycle_event": "DISPATCH_ACCEPTED",
            "budget_day_key": today_et_key,
            "daily_budget_records": daily_budget_records,
            "consumed_today_records": consumed_today,
            "remaining_budget_today": remaining_budget_today,
            "max_people_queries_for_run": max_people_queries_for_run,
            "estimated_records_per_people_query": estimated_records_per_people_query,
            "records_1w_limit": int(weekly_limit),
            "records_1w_remaining": int(weekly_remaining),
            "records_1w_resets_at": weekly_resets_at,
        },
    )


async def perpetual_awareness_loop(app_instance):
    print("🚀 [PERPETUAL BRAIN ENGINE] Staggered drip pipeline loop active...")
    db_pool = app_instance.state.db_pool
    last_standard_drip_at = None

    while True:
        try:
            moltset_healed = await check_and_heal_moltset_circuit(app_instance)

            est_now = datetime.datetime.now(NEW_YORK_TZ)
            current_weekday = int(est_now.strftime("%w"))
            current_date_str = est_now.strftime("%Y-%m-%d")

            if not moltset_healed:
                await dispatch_nova_if_due(app_instance)

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

            if est_now.hour >= NEXUS_MORNING_HOUR:
                async with db_pool.connection() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute("""
                            SELECT COUNT(*)
                            FROM agent_system_logs
                            WHERE agent_name = 'helix'
                              AND timestamp >= (NOW() AT TIME ZONE 'America/New_York')::date
                              AND metadata->'command_center_view'->>'action_taken' = 'DISPATCH_NEXUS_MORNING';
                            """)
                        nexus_morning_dispatches = (await cur.fetchone())[0]

                if nexus_morning_dispatches == 0:
                    async with db_pool.connection() as conn:
                        async with conn.cursor(row_factory=dict_row) as cur:
                            await cur.execute("""
                                SELECT industry_id, industry_name, moltset_category, bottleneck_profile, pain_tokens, target_region, campaign_strategy
                                FROM helix_brain_scheduler
                                WHERE is_active = TRUE
                                ORDER BY priority_score DESC, last_targeted_at ASC NULLS FIRST
                                LIMIT 1;
                            """)
                            nexus_strategy = await cur.fetchone()

                    if nexus_strategy:
                        vendor_isolation_matrix = await load_vendor_isolation_matrix(
                            db_pool
                        )
                        await trigger_downstream_mesh_with_payload(
                            db_pool,
                            app_instance.state,
                            nexus_strategy,
                            ["nexus"],
                            vendor_isolation_matrix,
                        )
                        await write_helix_command_center_log(
                            db_pool,
                            "INFO",
                            f"Completed scheduled morning Nexus dispatch for {nexus_strategy['industry_name']}.",
                            "DISPATCH_NEXUS_MORNING",
                            nexus_strategy["industry_name"],
                            {"strategy_id": nexus_strategy["industry_id"]},
                        )

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
                            await cur.execute(f"""
                                SELECT industry_id, industry_name, moltset_category, bottleneck_profile, pain_tokens, target_region, campaign_strategy
                                FROM helix_brain_scheduler 
                                WHERE is_active = TRUE 
                                ORDER BY {SCHEDULER_ORDER_BY} LIMIT 1;
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
                            AND scheduled_time IS NULL
                            AND LOWER(agent_name) NOT IN ('helix', 'nova');
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

                if not pending_agents_for_today:
                    # Build a readable quota summary (e.g. "nexus: 3/3, vanguard: 5/5, nova: 2/2")
                    quota_summary_str = ", ".join(
                        f"{row['agent_name'].lower()}: {run_counts_today.get(row['agent_name'].lower(), 0)}/{row['max_daily_runs']}"
                        for row in db_scheduled_agents
                    )

                    quota_telemetry_meta = {
                        "weekday_int": current_weekday,
                        "date_est": current_date_str,
                        "agent_quota_breakdown": {
                            row["agent_name"].lower(): {
                                "completed": run_counts_today.get(
                                    row["agent_name"].lower(), 0
                                ),
                                "max_allowed": row["max_daily_runs"],
                            }
                            for row in db_scheduled_agents
                        },
                    }

                    await write_helix_command_center_log(
                        db_pool,
                        "INFO",
                        f"All dynamic queue items for weekday {current_weekday} ({est_now.strftime('%A')}) have reached maximum daily quota allocations [{quota_summary_str}]. Standing down standard loop.",
                        "ORCHESTRATOR_COMPLETE",
                        "System Core",
                        quota_telemetry_meta,
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

                                    await cur.execute(f"""
                                        SELECT industry_id, industry_name, moltset_category, bottleneck_profile, pain_tokens, campaign_strategy, target_region
                                        FROM helix_brain_scheduler WHERE is_active = TRUE 
                                        AND (last_targeted_at IS NULL OR last_targeted_at <= NOW() - (minimum_cooldown_hours || ' hours')::INTERVAL)
                                        ORDER BY {SCHEDULER_ORDER_BY} LIMIT 1;
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

        except Exception as loop_crash:
            loop_traceback = traceback.format_exc()
            print(loop_traceback)
            await write_helix_command_center_log(
                db_pool,
                "CRITICAL",
                f"Perpetual awareness loop failed: {loop_crash}",
                "HELIX_AWARENESS_LOOP_FAILED",
                "awareness-dispatcher",
                {"traceback": loop_traceback},
            )
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
