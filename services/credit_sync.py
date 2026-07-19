import os
import re
import random
import asyncio
import httpx
from typing import Any
from services.telemetry import send_telegram_broadcast

VENDOR_PHANTOMBUSTERS = "PhantomBusters"
VENDOR_MYEMAILVERIFIER = "MyEmailVerifier"

CREDIT_SYNC_INTERVAL_SECONDS = 8 * 60 * 60
CREDIT_SYNC_JITTER_SECONDS = 10 * 60


def parse_credit_value(raw_value: Any) -> float | None:
    """Safely extracts numeric values from string or raw API numeric formats."""
    if raw_value is None:
        return None
    if isinstance(raw_value, (int, float)):
        return float(raw_value)
    if isinstance(raw_value, str):
        cleaned = raw_value.strip().replace(",", "")
        match = re.search(r"-?\d+(?:\.\d+)?", cleaned)
        if match:
            return float(match.group(0))
    return None


async def upsert_vendor_credit_state(
    db_pool,
    vendor_name: str,
    credits_remaining: float | None,
    is_circuit_broken: bool,
    credit_unit: str = "USD",
) -> None:
    """Updates the shared global oracle balance ledger inside Postgres."""
    normalized_credits = 0.0 if credits_remaining is None else float(credits_remaining)
    normalized_unit = (credit_unit or "USD")[:20]
    async with db_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO helix_infrastructure_oracle_ledger
                    (vendor_name, credits_remaining, credit_unit, is_circuit_broken, last_updated_at)
                VALUES (%s, %s, %s, %s, NOW())
                ON CONFLICT (vendor_name)
                DO UPDATE SET
                    credits_remaining = EXCLUDED.credits_remaining,
                    is_circuit_broken = EXCLUDED.is_circuit_broken,
                    credit_unit = EXCLUDED.credit_unit,
                    last_updated_at = NOW();
                """,
                (vendor_name, normalized_credits, normalized_unit, is_circuit_broken),
            )
        await conn.commit()


async def mark_vendor_fail_closed(
    db_pool, vendor_name: str, failure_reason: str
) -> None:
    """Enforces fail-safe structural isolating blocks on depleted/broken vendor endpoints."""
    await upsert_vendor_credit_state(
        db_pool,
        vendor_name=vendor_name,
        credits_remaining=0.0,
        is_circuit_broken=True,
        credit_unit="MINUTES" if vendor_name == VENDOR_PHANTOMBUSTERS else "USD",
    )
    send_telegram_broadcast(
        f"🚨 [CIRCUIT BREAKER] {vendor_name} credit sync failed. Fail-closed isolation engaged. Reason: {failure_reason[:200]}",
        "critical",
    )


async def sync_phantombuster_credits(db_pool) -> None:
    """Queries PhantomBuster engines to track run times and isolate queue drops on depletion."""
    pb_key = os.getenv("PHANTOMBUSTER_API_KEY")
    if not pb_key:
        await mark_vendor_fail_closed(
            db_pool, VENDOR_PHANTOMBUSTERS, "Missing PHANTOMBUSTER_API_KEY"
        )
        return

    try:
        async with httpx.AsyncClient() as client:
            res = await client.get(
                "https://api.phantombuster.com/api/v2/orgs/fetch-resources",
                headers={"X-PhantomBuster-Key": pb_key},
                timeout=10,
            )

            if res.status_code != 200:
                await mark_vendor_fail_closed(
                    db_pool, VENDOR_PHANTOMBUSTERS, f"HTTP Status {res.status_code}"
                )
                return

            payload = res.json()
            monthly_used_ms = parse_credit_value(payload.get("monthlyExecutionTime"))
            daily_used_ms = parse_credit_value(payload.get("dailyExecutionTime"))
            plan = payload.get("plan") if isinstance(payload.get("plan"), dict) else {}
            monthly_limit_ms = parse_credit_value(plan.get("monthlyExecutionTime"))
            daily_limit_ms = parse_credit_value(plan.get("dailyExecutionTime"))

            remaining_ms: float | None = None
            if (
                monthly_used_ms is not None
                and monthly_limit_ms
                and monthly_limit_ms > 0
            ):
                remaining_ms = max(monthly_limit_ms - monthly_used_ms, 0.0)
            elif daily_used_ms is not None and daily_limit_ms and daily_limit_ms > 0:
                remaining_ms = max(daily_limit_ms - daily_used_ms, 0.0)

            if remaining_ms is None:
                await mark_vendor_fail_closed(
                    db_pool, VENDOR_PHANTOMBUSTERS, "Resource calculation missing."
                )
                return

            credits = round(remaining_ms / 60000.0, 2)

            was_circuit_broken = False
            async with db_pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "SELECT is_circuit_broken FROM helix_infrastructure_oracle_ledger WHERE vendor_name = %s;",
                        (VENDOR_PHANTOMBUSTERS,),
                    )
                    row = await cur.fetchone()
                    if row:
                        was_circuit_broken = row[0]

            is_circuit_broken = credits <= 0
            await upsert_vendor_credit_state(
                db_pool,
                vendor_name=VENDOR_PHANTOMBUSTERS,
                credits_remaining=credits,
                is_circuit_broken=is_circuit_broken,
                credit_unit="MINUTES",
            )

            if was_circuit_broken and not is_circuit_broken:
                send_telegram_broadcast(
                    f"🔄 *INFRASTRUCTURE CIRCUIT HEALED*: PhantomBusters has recovered. Balance healthy at `{credits} MINUTES`.",
                    "success",
                )
            elif is_circuit_broken and not was_circuit_broken:
                send_telegram_broadcast(
                    f"🚨 [CIRCUIT BREAKER] PhantomBusters credits depleted (`{credits} MINUTES`). Downstream scraping queues isolated.",
                    "critical",
                )
    except Exception as e:
        await mark_vendor_fail_closed(
            db_pool, VENDOR_PHANTOMBUSTERS, f"Sync exception: {e}"
        )


async def sync_myemailverifier_credits(db_pool) -> None:
    """Fetches remaining credits for email outbound validations."""
    mev_key = os.getenv("MYEMAILVERIFIER_API_KEY")
    if not mev_key:
        await mark_vendor_fail_closed(
            db_pool, VENDOR_MYEMAILVERIFIER, "Missing MYEMAILVERIFIER_API_KEY"
        )
        return

    endpoint = f"https://client.myemailverifier.com/verifier/getcredits/{mev_key}"
    try:
        async with httpx.AsyncClient() as client:
            res = await client.get(endpoint, timeout=10)

            if res.status_code != 200:
                await mark_vendor_fail_closed(
                    db_pool, VENDOR_MYEMAILVERIFIER, f"HTTP Status {res.status_code}"
                )
                return

            payload = res.json()
            if payload.get("status") is False:
                await mark_vendor_fail_closed(
                    db_pool, VENDOR_MYEMAILVERIFIER, str(payload.get("message"))
                )
                return

            credits = parse_credit_value(
                payload.get("Credits") or payload.get("credits")
            )
            if credits is None:
                await mark_vendor_fail_closed(
                    db_pool, VENDOR_MYEMAILVERIFIER, "Failed to parse API credits."
                )
                return

            was_circuit_broken = False
            async with db_pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "SELECT is_circuit_broken FROM helix_infrastructure_oracle_ledger WHERE vendor_name = %s;",
                        (VENDOR_MYEMAILVERIFIER,),
                    )
                    row = await cur.fetchone()
                    if row:
                        was_circuit_broken = row[0]

            is_circuit_broken = credits <= 0
            await upsert_vendor_credit_state(
                db_pool,
                vendor_name=VENDOR_MYEMAILVERIFIER,
                credits_remaining=credits,
                is_circuit_broken=is_circuit_broken,
                credit_unit="CREDITS",
            )

            if was_circuit_broken and not is_circuit_broken:
                send_telegram_broadcast(
                    f"🔄 *INFRASTRUCTURE CIRCUIT HEALED*: MyEmailVerifier has recovered. Balance healthy at `{credits} CREDITS`.",
                    "success",
                )
            elif is_circuit_broken and not was_circuit_broken:
                send_telegram_broadcast(
                    f"🚨 [CIRCUIT BREAKER] MyEmailVerifier credits depleted (`{credits} CREDITS`). Outbound verification isolated.",
                    "critical",
                )
    except Exception as e:
        await mark_vendor_fail_closed(
            db_pool, VENDOR_MYEMAILVERIFIER, f"Sync exception: {e}"
        )


async def sync_external_vendor_credits(db_pool):
    await sync_phantombuster_credits(db_pool)
    await sync_myemailverifier_credits(db_pool)


async def credit_sync_scheduler_loop(db_pool, execution_constraints: dict | None = None) -> None:
    """Orchestrates periodic infrastructure sweeps to feed vendor matrix breakers."""
    from config.settings import is_execution_allowed_today
    
    while True:
        try:
            # Check if execution is allowed before proceeding
            if execution_constraints and not is_execution_allowed_today(execution_constraints):
                await asyncio.sleep(3600)  # Sleep 1 hour on unassigned days
                continue
            
            jitter_seconds = random.randint(
                -CREDIT_SYNC_JITTER_SECONDS, CREDIT_SYNC_JITTER_SECONDS
            )
            sleep_seconds = max(300, CREDIT_SYNC_INTERVAL_SECONDS + jitter_seconds)
            await asyncio.sleep(sleep_seconds)
            await sync_external_vendor_credits(db_pool)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(f"⚠️ Credit scheduler loop anomaly: {exc}")
