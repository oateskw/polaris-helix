from curses import meta
import os
import re
import traceback
import asyncio
import random
import json
import hmac
import requests
import datetime
import psycopg
import httpx
from anthropic import Anthropic
from contextlib import asynccontextmanager, suppress
from typing import Any, Literal, Optional, List
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, Request, Header, BackgroundTasks
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, ConfigDict, model_validator
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool
import uvicorn

load_dotenv()

MOLTSET_ALLOWED_CATEGORIES = [
    "Information Technology",
    "Professional and Business Services",
    "Finance and Banking",
    "Education",
    "Health and Pharmaceuticals",
    "Manufacturing",
    "Government and Public Administration",
    "Retail",
    "Food and Beverage",
    "Creative Arts and Entertainment",
    "Non-Profit and Social Services",
    "Transportation and Logistics",
    "Construction",
    "Tourism and Hospitality",
    "Energy",
    "Marketing & Advertising",
    "Telecommunications",
    "Real Estate",
    "Automotive",
    "Media and Publishing",
    "Agriculture",
]

NEW_YORK_TZ = ZoneInfo("America/New_York")
PHANTOMBUSTER_KEYWORD_STAIRCASE = [25, 12, 8, 5]
PHANTOMBUSTER_ZERO_RESULT_THRESHOLD = 2
PHANTOMBUSTER_TUNING_COOLDOWN_MINUTES = 60
VENDOR_PHANTOMBUSTERS = "PhantomBusters"
VENDOR_MYEMAILVERIFIER = "MyEmailVerifier"
VENDOR_MOLTSET = "Moltset"
VENDOR_ANTHROPIC = "Anthropic"
LEDGER_QUERYABLE_VENDORS = (VENDOR_MYEMAILVERIFIER, VENDOR_PHANTOMBUSTERS)
CREDIT_SYNC_INTERVAL_SECONDS = 8 * 60 * 60
CREDIT_SYNC_JITTER_SECONDS = 10 * 60


def get_validated_database_url() -> str:
    url = os.getenv("DATABASE_URL", "")
    if not url or not url.startswith("postgresql://"):
        raise ValueError("Missing or invalid cloud DATABASE_URL.")
    parsed = urlparse(url)
    query_map = dict(parse_qsl(parsed.query, keep_blank_values=True))
    if query_map.get("sslmode") != "require":
        query_map["sslmode"] = "require"
        url = urlunparse(parsed._replace(query=urlencode(query_map)))
    return url


def get_new_york_now() -> datetime.datetime:
    return datetime.datetime.now(NEW_YORK_TZ)


def get_phantombuster_keyword_cap(industry_name: str | None = None) -> int:
    default_cap = PHANTOMBUSTER_KEYWORD_STAIRCASE[0]
    if not industry_name:
        return default_cap
    remediation_state = getattr(app.state, "phantombuster_remediation", {})
    industry_state = remediation_state.get(industry_name.lower(), {})
    keyword_cap = industry_state.get("keyword_cap", default_cap)
    return max(PHANTOMBUSTER_KEYWORD_STAIRCASE[-1], int(keyword_cap))


def apply_phantombuster_zero_result_policy(
    industry_name: str | None,
) -> dict[str, Any] | None:
    """Move an industry through Watch -> Constrained -> Isolated states using bounded keyword caps."""
    if not industry_name:
        return None

    key = industry_name.lower().strip()
    if not key:
        return None

    remediation_state = getattr(app.state, "phantombuster_remediation", None)
    if remediation_state is None:
        app.state.phantombuster_remediation = {}
        remediation_state = app.state.phantombuster_remediation

    now_utc = datetime.datetime.now(datetime.timezone.utc)
    entry = remediation_state.get(
        key,
        {
            "state": "healthy",
            "zero_streak": 0,
            "keyword_cap": PHANTOMBUSTER_KEYWORD_STAIRCASE[0],
            "cooldown_until": None,
        },
    )

    cooldown_until = entry.get("cooldown_until")
    if isinstance(cooldown_until, datetime.datetime) and now_utc < cooldown_until:
        return {
            "state": entry.get("state", "watch"),
            "industry": industry_name,
            "keyword_cap": entry.get("keyword_cap", PHANTOMBUSTER_KEYWORD_STAIRCASE[0]),
            "cooldown_active": True,
            "cooldown_until": cooldown_until.isoformat(),
        }

    entry["zero_streak"] = int(entry.get("zero_streak", 0)) + 1
    current_cap = int(entry.get("keyword_cap", PHANTOMBUSTER_KEYWORD_STAIRCASE[0]))

    if entry["zero_streak"] < PHANTOMBUSTER_ZERO_RESULT_THRESHOLD:
        entry["state"] = "watch"
        remediation_state[key] = entry
        return {
            "state": "watch",
            "industry": industry_name,
            "keyword_cap": current_cap,
            "zero_streak": entry["zero_streak"],
            "cooldown_active": False,
        }

    next_cap = current_cap
    for stage_cap in PHANTOMBUSTER_KEYWORD_STAIRCASE:
        if stage_cap < current_cap:
            next_cap = stage_cap
            break

    if next_cap < current_cap:
        entry["state"] = "constrained"
        entry["keyword_cap"] = next_cap
        entry["zero_streak"] = 0
        entry["cooldown_until"] = now_utc + datetime.timedelta(
            minutes=PHANTOMBUSTER_TUNING_COOLDOWN_MINUTES
        )
    else:
        entry["state"] = "isolated"
        entry["keyword_cap"] = PHANTOMBUSTER_KEYWORD_STAIRCASE[-1]
        entry["zero_streak"] = 0
        entry["cooldown_until"] = now_utc + datetime.timedelta(
            minutes=PHANTOMBUSTER_TUNING_COOLDOWN_MINUTES
        )

    remediation_state[key] = entry
    return {
        "state": entry["state"],
        "industry": industry_name,
        "keyword_cap": entry["keyword_cap"],
        "zero_streak": entry["zero_streak"],
        "cooldown_active": False,
        "cooldown_until": entry["cooldown_until"].isoformat(),
    }


def reset_phantombuster_policy_on_success(industry_name: str | None) -> None:
    if not industry_name:
        return
    remediation_state = getattr(app.state, "phantombuster_remediation", None)
    if not remediation_state:
        return
    key = industry_name.lower().strip()
    if not key or key not in remediation_state:
        return

    remediation_state[key] = {
        "state": "healthy",
        "zero_streak": 0,
        "keyword_cap": remediation_state[key].get(
            "keyword_cap", PHANTOMBUSTER_KEYWORD_STAIRCASE[0]
        ),
        "cooldown_until": None,
    }


def is_phantombuster_zero_result_signal(message: str, metadata: dict[str, Any]) -> bool:
    combined = f"{message} {json.dumps(metadata)}".lower()
    if "phantombuster" not in combined:
        return False

    if any(token in combined for token in ("0 results", "zero results", "no results")):
        return True

    results_count = metadata.get("results_count")
    if isinstance(results_count, int) and results_count == 0:
        return True
    if (
        isinstance(results_count, str)
        and results_count.isdigit()
        and int(results_count) == 0
    ):
        return True

    return False


def send_telegram_broadcast(message: str, notification_type: str = "verbose"):
    """Sends immediate scheduling sync alerts to Telegram with type tagging for future muting capability."""
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        return

    prefixes = {
        "critical": "🚨 [CRITICAL INFRASTRUCTURE]",
        "success": "✅ [PIPELINE SUCCESS]",
        "orchestration": "🧠 [HELIX ORCHESTRATOR]",
        "verbose": "📡 [TELEMETRY]",
    }
    prefix = prefixes.get(notification_type, "📡")
    formatted_message = f"{prefix}\n{message}"

    async def _send() -> None:
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                await client.post(
                    f"https://api.telegram.org/bot{token}/sendMessage",
                    json={
                        "chat_id": chat_id,
                        "text": formatted_message,
                        "parse_mode": "Markdown",
                    },
                )
        except Exception as e:
            print(f"⚠️ Telegram broadcast failed: {e}")

    try:
        loop = asyncio.get_running_loop()
        loop.create_task(_send())
    except RuntimeError:
        try:
            requests.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={
                    "chat_id": chat_id,
                    "text": formatted_message,
                    "parse_mode": "Markdown",
                },
                timeout=5,
            )
        except Exception as e:
            print(f"⚠️ Telegram broadcast failed: {e}")
    except Exception as e:
        print(f"⚠️ Telegram broadcast failed: {e}")


def run_deterministic_moltset_mapping(ai_client: Anthropic, industry_name: str) -> str:
    """Uses modern tool/JSON parameters to force Claude to return a valid category."""
    try:
        response = ai_client.messages.create(
            model=os.getenv("ANTHROPIC_MODEL", "claude-3-5-sonnet-latest"),
            max_tokens=100,
            temperature=0.0,
            system="You are a data categorization engine. You must map the user input exactly to one of the provided options.",
            messages=[
                {
                    "role": "user",
                    "content": f"Map this business niche: '{industry_name}'",
                }
            ],
            tools=[
                {
                    "name": "categorize_industry",
                    "description": "Output the single best matching industry category.",
                    "input_schema": {
                        "type": "object",
                        "properties": {
                            "category": {
                                "type": "string",
                                "enum": MOLTSET_ALLOWED_CATEGORIES,
                            }
                        },
                        "required": ["category"],
                    },
                }
            ],
            tool_choice={"type": "tool", "name": "categorize_industry"},
        )

        for block in response.content:
            if block.type == "tool_use" and block.name == "categorize_industry":
                chosen_category = block.input.get("category")
                if (
                    isinstance(chosen_category, str)
                    and chosen_category in MOLTSET_ALLOWED_CATEGORIES
                ):
                    return chosen_category

    except Exception as e:
        send_telegram_broadcast(
            f"Moltset AI mapping failed for `{industry_name}`: {e}", "critical"
        )

    return "Professional and Business Services"


class LogIngestSchema(BaseModel):
    agent_name: Literal[
        "vanguard",
        "beacon",
        "echo",
        "shadow",
        "aura",
        "prism",
        "vector",
        "nexus",
        "helix",
    ]
    log_level: str = "INFO"
    message: str
    metadata: dict[str, Any] | None = None

    @model_validator(mode="before")
    @classmethod
    def self_heal_legacy_payloads(cls, data: Any) -> Any:
        if isinstance(data, dict):
            if "agent" in data and "agent_name" not in data:
                raw_agent = str(data.pop("agent")).lower()
                data["agent_name"] = (
                    "vector"
                    if "vector" in raw_agent
                    else ("nexus" if "nexus" in raw_agent else raw_agent)
                )
            if "message" not in data or not data["message"]:
                data["message"] = (
                    f"Background pipeline activity: {data.get('event', 'generic_background_telemetry')}"
                )
            if "metadata" not in data or data["metadata"] is None:
                known_fields = {"agent_name", "log_level", "message"}
                data["metadata"] = {
                    k: v for k, v in data.items() if k not in known_fields
                }
        return data


class LeadIngestModel(BaseModel):
    email: str
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    company: Optional[str] = None
    title: Optional[str] = None
    website: Optional[str] = None
    industry: Optional[str] = None


class LeadStatusUpdateModel(BaseModel):
    status: str
    email_body: Optional[str] = None


class BlueprintInboundModel(BaseModel):
    headcount: int
    weekly_hours_wasted: int
    annual_wasted_hours: int
    annual_financial_bleed: float
    friction_points: List[str]
    name: str
    company_name: str
    email: str
    status: Optional[str] = "raw_calculation_captured"
    industry_name: Optional[str] = "Unassigned"


class SchedulerTargetUpdateModel(BaseModel):
    industry_name: str
    target_region: str


class SchedulerRecordIngestModel(BaseModel):
    industry_name: str
    bottleneck_profile: str
    pain_tokens: List[str]
    target_region: str = "Texas"
    priority_score: int = 1
    minimum_cooldown_hours: int = 24


HELIX_INBOUND_BLUEPRINTS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS helix_inbound_blueprints (
    id BIGSERIAL PRIMARY KEY, headcount INTEGER NOT NULL, weekly_hours_wasted INTEGER NOT NULL,
    annual_wasted_hours INTEGER NOT NULL, annual_financial_bleed DOUBLE PRECISION NOT NULL,
    friction_points JSONB NOT NULL, name TEXT NOT NULL, company_name TEXT NOT NULL, email TEXT NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'raw_calculation_captured', industry_name VARCHAR(100) DEFAULT 'Unassigned',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

# ─── ITEM 4: SCHEMA EXTENSION EMBEDDED ───
SCHEDULER_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS helix_brain_scheduler (
    industry_id SERIAL PRIMARY KEY, industry_name VARCHAR(100) UNIQUE NOT NULL,
    moltset_category VARCHAR(100) DEFAULT 'Professional and Business Services',
    bottleneck_profile TEXT NOT NULL, pain_tokens TEXT[] NOT NULL, target_region VARCHAR(100) DEFAULT 'Texas',
    is_active BOOLEAN DEFAULT TRUE, last_targeted_at TIMESTAMP DEFAULT NULL, priority_score INT DEFAULT 1,
    minimum_cooldown_hours INT DEFAULT 24
);
"""

HELIX_INFRASTRUCTURE_ORACLE_LEDGER_SQL = """
CREATE TABLE IF NOT EXISTS helix_infrastructure_oracle_ledger (
    vendor_name VARCHAR(50) PRIMARY KEY,
    credits_remaining NUMERIC(10, 2) NOT NULL DEFAULT 100.00,
    credit_unit VARCHAR(20) NOT NULL DEFAULT 'USD',
    is_circuit_broken BOOLEAN DEFAULT FALSE,
    last_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""


def parse_credit_value(raw_value: Any) -> float | None:
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


async def seed_oracle_ledger_rows(db_pool) -> None:
    async with db_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.executemany(
                """
                INSERT INTO helix_infrastructure_oracle_ledger
                    (vendor_name, credits_remaining, credit_unit, is_circuit_broken)
                VALUES (%s, 100.00, 'USD', FALSE)
                ON CONFLICT (vendor_name) DO NOTHING;
                """,
                [(vendor_name,) for vendor_name in LEDGER_QUERYABLE_VENDORS],
            )
        await conn.commit()


async def load_vendor_isolation_matrix(db_pool) -> dict[str, bool]:
    vendor_isolation_matrix: dict[str, bool] = {}
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

        if getattr(app.state, "vendor_matrix_degraded", False):
            app.state.vendor_matrix_degraded = False
            send_telegram_broadcast(
                "✅ Vendor isolation matrix recovered. Circuit guardrails are live again.",
                "success",
            )
    except Exception as exc:
        if not getattr(app.state, "vendor_matrix_degraded", False):
            app.state.vendor_matrix_degraded = True
            send_telegram_broadcast(
                "🚨 Vendor isolation matrix read failed. Running fail-safe with default open matrix until recovery.",
                "critical",
            )
        print(f"⚠️ Vendor isolation matrix load failed: {exc}")
    return vendor_isolation_matrix


async def upsert_vendor_credit_state(
    db_pool,
    vendor_name: str,
    credits_remaining: float | None,
    is_circuit_broken: bool,
    credit_unit: str = "USD",
) -> None:
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
                (
                    vendor_name,
                    normalized_credits,
                    normalized_unit,
                    is_circuit_broken,
                ),
            )
        await conn.commit()


async def mark_vendor_fail_closed(
    db_pool, vendor_name: str, failure_reason: str
) -> None:
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


async def log_helix_state(
    cur, strategic_mode: str, geo_lock: str, buying_signal: str
) -> None:
    metadata_payload = {
        "strategic_mode": strategic_mode,
        "geo_lock": geo_lock,
        "buying_signal": buying_signal,
    }
    await cur.execute(
        "INSERT INTO agent_system_logs (agent_name, log_level, message, metadata) VALUES ('helix', 'INFO', 'Global system parameters synchronized.', %s);",
        (Jsonb(metadata_payload),),
    )


DAILY_EXECUTION_ROUTERS = {
    0: ["nexus", "beacon", "harvester", "vanguard", "beacon_monitor", "listener"],
    1: ["nexus", "beacon", "harvester", "vanguard", "beacon_monitor", "listener"],
    2: ["nexus", "beacon", "harvester", "vanguard", "beacon_monitor", "listener"],
    3: ["nexus", "beacon", "harvester", "vanguard", "beacon_monitor", "listener"],
    4: ["nexus", "beacon", "harvester", "vanguard", "beacon_monitor", "listener"],
    5: ["harvester", "listener"],
    6: ["harvester", "listener"],
}


async def evaluate_lead_density_and_escalate(
    industry_name: str, influx_count: int, db_pool, background_tasks: BackgroundTasks
):
    """🧠 ADAPTIVE INTAKE TRIGGER"""
    if not industry_name or industry_name == "Unassigned":
        return

    try:
        async with db_pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                boost_weight = min(influx_count // 5 + 1, 10)
                await cur.execute(
                    """
                    UPDATE helix_brain_scheduler 
                    SET priority_score = priority_score + %s,
                        is_active = TRUE 
                    WHERE industry_name = %s
                    RETURNING industry_id, industry_name, moltset_category, bottleneck_profile, pain_tokens, target_region, priority_score;
                """,
                    (boost_weight, industry_name),
                )
                updated_strategy = await cur.fetchone()
                await conn.commit()

        if updated_strategy:
            send_telegram_broadcast(
                f"📈 *ADAPTIVE INGESTION DETECTED*\n"
                f"Niche: `{industry_name}` received `{influx_count}` new leads.\n"
                f"Dynamically boosted priority score to: `{updated_strategy['priority_score']}`.",
                "orchestration",
            )

            if influx_count >= 10:
                send_telegram_broadcast(
                    f"⚡ *HIGH DENSITY THRESHOLD BROKEN*\n"
                    f"Industry `{industry_name}` crossed emergency ingestion limits.\n"
                    f"Bypassing midnight EST clock cycle. Deploying real-time proxy tasks now!",
                    "orchestration",
                )

                # 🩹 Query the database to construct the live vendor isolation matrix
                vendor_isolation_matrix = await load_vendor_isolation_matrix(
                    app.state.db_pool
                )

                send_telegram_broadcast(
                    f"🚀 *REAL-TIME MESH ESCALATION INITIATED*\n"
                    f"Deploying production pipeline workers automatically for niche: `{updated_strategy['industry_name']}`",
                    "orchestration",
                )
                background_tasks.add_task(
                    trigger_downstream_mesh_with_payload,
                    updated_strategy,
                    ["nexus", "beacon", "harvester", "vanguard", "listener"],
                    vendor_isolation_matrix,  # 🩹 Pass real matrix data instead of {}
                )
    except Exception as e:
        print(f"⚠️ Adaptive intake trigger anomaly: {e}")


async def sync_external_vendor_credits(db_pool):
    """Sync balances for queryable vendors and enforce circuit-breaker state."""
    await sync_phantombuster_credits(db_pool)
    await sync_myemailverifier_credits(db_pool)


async def sync_phantombuster_credits(db_pool) -> None:
    pb_key = os.getenv("PHANTOMBUSTER_API_KEY")
    if not pb_key:
        await mark_vendor_fail_closed(
            db_pool,
            VENDOR_PHANTOMBUSTERS,
            "Missing PHANTOMBUSTER_API_KEY",
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
                    db_pool,
                    VENDOR_PHANTOMBUSTERS,
                    f"PhantomBuster returned HTTP {res.status_code}",
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
                    db_pool,
                    VENDOR_PHANTOMBUSTERS,
                    "Unable to compute remaining execution time from PhantomBuster resources payload",
                )
                return

            credits = round(remaining_ms / 60000.0, 2)

            is_circuit_broken = credits <= 0
            await upsert_vendor_credit_state(
                db_pool,
                vendor_name=VENDOR_PHANTOMBUSTERS,
                credits_remaining=credits,
                is_circuit_broken=is_circuit_broken,
                credit_unit="MINUTES",
            )

            if not is_circuit_broken:
                send_telegram_broadcast(
                    "🔄 *INFRASTRUCTURE CIRCUIT HEALED*: PhantomBusters has been automatically re-activated.",
                    "success",
                )
            else:
                send_telegram_broadcast(
                    "🚨 [CIRCUIT BREAKER] PhantomBusters credits are depleted. Isolation engaged.",
                    "critical",
                )
    except Exception as e:
        await mark_vendor_fail_closed(
            db_pool,
            VENDOR_PHANTOMBUSTERS,
            f"PhantomBuster sync exception: {e}",
        )


async def sync_myemailverifier_credits(db_pool) -> None:
    mev_key = os.getenv("MYEMAILVERIFIER_API_KEY")
    if not mev_key:
        await mark_vendor_fail_closed(
            db_pool,
            VENDOR_MYEMAILVERIFIER,
            "Missing MYEMAILVERIFIER_API_KEY",
        )
        return

    endpoint = f"https://client.myemailverifier.com/verifier/getcredits/{mev_key}"
    try:
        async with httpx.AsyncClient() as client:
            res = await client.get(endpoint, timeout=10)

            if res.status_code != 200:
                await mark_vendor_fail_closed(
                    db_pool,
                    VENDOR_MYEMAILVERIFIER,
                    f"MyEmailVerifier returned HTTP {res.status_code}",
                )
                return

            payload = res.json()
            if payload.get("status") is False:
                failure_message = payload.get(
                    "message", "MyEmailVerifier rejected request"
                )
                await mark_vendor_fail_closed(
                    db_pool,
                    VENDOR_MYEMAILVERIFIER,
                    str(failure_message),
                )
                return

            credits = parse_credit_value(
                payload.get("Credits") or payload.get("credits")
            )
            if credits is None:
                await mark_vendor_fail_closed(
                    db_pool,
                    VENDOR_MYEMAILVERIFIER,
                    "Unable to parse Credits from MyEmailVerifier response",
                )
                return

            is_circuit_broken = credits <= 0
            await upsert_vendor_credit_state(
                db_pool,
                vendor_name=VENDOR_MYEMAILVERIFIER,
                credits_remaining=credits,
                is_circuit_broken=is_circuit_broken,
                credit_unit="CREDITS",
            )

            if not is_circuit_broken:
                send_telegram_broadcast(
                    "🔄 *INFRASTRUCTURE CIRCUIT HEALED*: MyEmailVerifier has been automatically re-activated.",
                    "success",
                )
            else:
                send_telegram_broadcast(
                    "🚨 [CIRCUIT BREAKER] MyEmailVerifier credits are depleted. Isolation engaged.",
                    "critical",
                )
    except Exception as e:
        await mark_vendor_fail_closed(
            db_pool,
            VENDOR_MYEMAILVERIFIER,
            f"MyEmailVerifier sync exception: {e}",
        )


async def credit_sync_scheduler_loop(db_pool) -> None:
    while True:
        try:
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


async def perpetual_awareness_loop():
    print("🚀 [PERPETUAL BRAIN ENGINE] Staggered drip pipeline loop active...")

    while True:
        try:
            est_now = get_new_york_now()
            current_weekday = est_now.weekday()

            scheduled_agents_list = DAILY_EXECUTION_ROUTERS.get(current_weekday, [])
            if not scheduled_agents_list:
                await asyncio.sleep(60)
                continue

            # 1. Find what has completed successfully today
            async with app.state.db_pool.connection() as conn:
                async with conn.cursor(row_factory=dict_row) as cur:
                    await cur.execute("""
                        SELECT DISTINCT LOWER(
                            CASE 
                              WHEN LOWER(agent_name) = 'beacon' AND (message ILIKE '%Monitor%' OR message ILIKE '%background sweep%') THEN 'beacon_monitor'
                              WHEN LOWER(agent_name) = 'beacon' AND (message ILIKE '%Listener%' OR message ILIKE '%sweep%completed%' OR message ILIKE '%beacon_listener%') THEN 'listener'
                              WHEN LOWER(agent_name) = 'beacon' AND (message ILIKE '%Publer%' OR message ILIKE '%copy array%' OR message ILIKE '%Flux%') THEN 'beacon'
                              WHEN (LOWER(agent_name) = 'vanguard' OR LOWER(agent_name) = 'harvester') AND (message ILIKE '%Harvester%' OR message ILIKE '%mined%' OR message ILIKE '%MoltSets%') THEN 'harvester'
                              WHEN LOWER(agent_name) = 'vanguard' AND (message ILIKE '%dispatch%' OR message ILIKE '%Instantly%' OR message ILIKE '%campaign%') THEN 'vanguard'
                              ELSE agent_name
                            END
                        ) as executed_agent
                        FROM agent_system_logs 
                        WHERE timestamp >= (NOW() AT TIME ZONE 'America/New_York')::date;
                    """)
                    completed_rows = await cur.fetchall()
                    completed_today_set = {r["executed_agent"] for r in completed_rows}

            # Filter out already completed tasks
            pending_agents_for_today = [
                agent
                for agent in scheduled_agents_list
                if agent not in completed_today_set
            ]

            if pending_agents_for_today:
                # Load the live infrastructure circuit breaker status matrix
                vendor_isolation_matrix = await load_vendor_isolation_matrix(
                    app.state.db_pool
                )

                # 🧠 SMART EVALUATION: Scan the pending queue and find the first UNBLOCKED agent
                next_agent_up = None
                for candidate_agent in pending_agents_for_today:
                    is_isolated = False

                    if candidate_agent == "vanguard" and vendor_isolation_matrix.get(
                        VENDOR_MYEMAILVERIFIER, False
                    ):
                        is_isolated = True
                    elif (
                        candidate_agent == "beacon_monitor"
                        and vendor_isolation_matrix.get(VENDOR_PHANTOMBUSTERS, False)
                    ):
                        is_isolated = True
                    elif candidate_agent == "harvester" and vendor_isolation_matrix.get(
                        VENDOR_MOLTSET, False
                    ):
                        is_isolated = True
                    elif candidate_agent == "nexus" and vendor_isolation_matrix.get(
                        VENDOR_ANTHROPIC, False
                    ):
                        is_isolated = True

                    if is_isolated:
                        # 🩹 BYPASS THE ISOLATION BLOCK: Don't stall the thread, just jump to the next agent!
                        print(
                            f"⚠️ [QUEUE BYPASS] Skipping isolated agent '{candidate_agent}' to prevent queue blocking."
                        )
                        continue
                    else:
                        next_agent_up = candidate_agent
                        break

                if next_agent_up:
                    async with app.state.db_pool.connection() as conn:
                        async with conn.cursor(row_factory=dict_row) as cur:
                            if next_agent_up in ["beacon_monitor", "listener"]:
                                await cur.execute("""
                                    SELECT industry_id, industry_name, moltset_category, bottleneck_profile, pain_tokens, target_region
                                    FROM helix_brain_scheduler 
                                    WHERE is_active = TRUE 
                                    ORDER BY last_targeted_at DESC NULLS LAST LIMIT 1;
                                """)
                            else:
                                await cur.execute("""
                                    SELECT industry_id, industry_name, moltset_category, bottleneck_profile, pain_tokens, target_region
                                    FROM helix_brain_scheduler 
                                    WHERE is_active = TRUE 
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
                        await trigger_downstream_mesh_with_payload(
                            active_strategy,
                            [next_agent_up],
                            vendor_isolation_matrix,
                        )

                    # Only cool down if a task actually ran
                    await asyncio.sleep(45 * 60)
                    continue
                else:
                    logger.info(
                        "🏁 All pending queue items are currently blocked by circuit isolation. Retrying in 5 minutes..."
                    )
                    await asyncio.sleep(5 * 60)
                    continue

        except Exception as loop_crash:
            send_telegram_broadcast(f"Loop exception: `{loop_crash}`", "critical")

        await asyncio.sleep(60)


async def process_roi_attribution_boost(email: str, status: str, db_pool):
    """📈 CLOSED-LOOP ATTRIBUTION ENGINE"""
    normalized_status = status.lower().strip()

    reward_weight = 0
    if "reply" in normalized_status:
        reward_weight = 15
    elif "meeting" in normalized_status or "booked" in normalized_status:
        reward_weight = 40
    elif "won" in normalized_status or "converted" in normalized_status:
        reward_weight = 100

    if reward_weight == 0:
        return

    try:
        async with db_pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    "SELECT industry FROM cold_leads WHERE email = %s LIMIT 1;",
                    (email,),
                )
                lead_row = await cur.fetchone()

                if lead_row and lead_row["industry"]:
                    target_industry = lead_row["industry"]

                    await cur.execute(
                        """
                        UPDATE helix_brain_scheduler
                        SET priority_score = priority_score + %s,
                            is_active = TRUE
                        WHERE industry_name = %s
                        RETURNING industry_name, priority_score;
                    """,
                        (reward_weight, target_industry),
                    )
                    updated_row = await cur.fetchone()
                    await conn.commit()

                    if updated_row:
                        send_telegram_broadcast(
                            f"🏆 *CLOSED-LOOP ROI REWARD CONTRIBUTED*\n"
                            f"Lead: `{email}` hit status milestone: *{status.upper()}*\n"
                            f"Mapped Niche: `{target_industry}`\n"
                            f"Priority score organically scaled up by `+{reward_weight}` points!\n"
                            f"New Scheduling Matrix Rank: `{updated_row['priority_score']}`",
                            "success",
                        )
    except Exception as e:
        print(f"⚠️ Attribution processing anomaly: {e}")


async def trigger_downstream_mesh_with_payload(
    strategy: dict, agent_list: list, isolation_matrix: dict
):
    master_token = os.getenv("INBOUND_API_KEY", "")

    # 1. NEXUS OMNI-STRATEGIST (MUST SUCCEED FIRST)
    if "nexus" in agent_list:
        if isolation_matrix.get(VENDOR_ANTHROPIC, False):
            send_telegram_broadcast(
                "Skipping Nexus Omni-Strategist sync sequence. Anthropic wallet ledger is isolated.",
                "critical",
            )
        else:
            url = os.getenv(
                "NEXUS_NERVOUS_IMPULSE_URL", "https://polaris-nexus.onrender.com"
            ).rstrip("/")

            # ✅ FIXED: Direct await (no asyncio.create_task wrapper!)
            # ✅ FIXED: Passes BOTH industry_name and target_niche simultaneously to satisfy Nexus's schema!
            nexus_success = await async_http_post_proxy(
                f"{url}/api/v1/nexus/awaken",
                {
                    "X-Nexus-Auth-Token": master_token,
                    "Content-Type": "application/json",
                },
                {
                    "trigger_reason": "perpetual_awareness_heartbeat",
                    "batch_size": 25,
                    "rescue_mode": False,
                    "industry_id": strategy["industry_id"],
                    "industry_name": strategy["industry_name"],
                    "target_niche": strategy["industry_name"],
                    "target_region": strategy["target_region"],
                    "bottleneck_profile": strategy["bottleneck_profile"],
                    "pain_tokens": strategy["pain_tokens"],
                },
            )

            if not nexus_success:
                send_telegram_broadcast(
                    f"🛑 *CRITICAL ORCHESTRATION HALT*\n"
                    f"Nexus failed to initialize for strategy: `{strategy['industry_name']}`.\n"
                    f"Suppressing all downstream mesh tasks (Harvester, Beacon, Vanguard) to prevent corruption.",
                    "critical",
                )
                return  # 🧠 EXIT FUNCTION ENTIRELY. DO NOT RUN ANYTHING ELSE.

            send_telegram_broadcast(
                "Nexus execution impulse successfully deployed down-mesh.", "verbose"
            )

    # 2. LEAD HARVESTER
    if "harvester" in agent_list:
        if isolation_matrix.get(VENDOR_MOLTSET, False):
            send_telegram_broadcast(
                "Skipping Lead Harvester execution loop. Moltset circuit breaker is TRIPPED.",
                "critical",
            )
        else:
            url = os.getenv(
                "HARVESTER_API_URL",
                "https://polaris-vanguard-harvester-udf7.onrender.com",
            ).rstrip("/")
            asyncio.create_task(
                async_http_post_proxy(
                    f"{url}/api/v1/harvester/execute",
                    {
                        "X-Harvester-Auth-Token": master_token,
                        "Content-Type": "application/json",
                    },
                    {
                        "industry_id": strategy["industry_id"],
                        "industry_name": strategy["industry_name"],
                        "moltset_category": strategy["moltset_category"],
                        "target_region": strategy["target_region"],
                        "bottleneck_profile": strategy["bottleneck_profile"],
                    },
                )
            )
            send_telegram_broadcast(
                "Harvester proxy sync call successfully routed to mesh edge node.",
                "verbose",
            )

    # 3. BEACON TASK DISPATCH
    if "beacon" in agent_list:
        url = os.getenv(
            "BEACON_API_URL", "https://polaris-beacon-agent.onrender.com"
        ).rstrip("/")
        asyncio.create_task(
            async_http_post_proxy(
                f"{url}/api/v1/beacon/execute",
                {
                    "X-Beacon-Auth-Token": master_token,
                    "Content-Type": "application/json",
                },
                {
                    "industry_id": strategy["industry_id"],
                    "industry_name": strategy["industry_name"],
                    "target_region": strategy["target_region"],
                    "bottleneck_profile": strategy["bottleneck_profile"],
                },
            )
        )
        send_telegram_broadcast(
            "Beacon task instructions successfully dispatched down-mesh.", "verbose"
        )

    # 4. BEACON MONITOR
    if "beacon_monitor" in agent_list:
        if isolation_matrix.get(VENDOR_PHANTOMBUSTERS, False):
            send_telegram_broadcast(
                "Skipping Beacon Monitor lead scraping cycle. PhantomBusters circuit breaker is TRIPPED.",
                "critical",
            )
        else:
            url = os.getenv(
                "BEACON_MONITOR_API_URL", "https://polaris-beacon-monitor.onrender.com"
            ).rstrip("/")
            keyword_cap = get_phantombuster_keyword_cap(strategy.get("industry_name"))
            asyncio.create_task(
                async_http_post_proxy(
                    f"{url}/api/v1/beacon/execute",
                    {
                        "X-Beacon-Auth-Token": master_token,
                        "X-Helix-Keyword-Cap": str(keyword_cap),
                        "Content-Type": "application/json",
                    },
                    {
                        "industry_id": strategy["industry_id"],
                        "industry_name": strategy["industry_name"],
                        "target_region": strategy["target_region"],
                        "bottleneck_profile": strategy["bottleneck_profile"],
                    },
                )
            )
            send_telegram_broadcast(
                "Beacon Monitor lead scraping cycle successfully deployed down-mesh.",
                "verbose",
            )

    # 5. LISTENER SWEEP
    if "listener" in agent_list:
        url = os.getenv(
            "LISTENER_API_URL", "https://polaris-beacon-listener.onrender.com"
        ).rstrip("/")
        asyncio.create_task(
            async_http_post_proxy(
                f"{url}/api/v1/listener/sweep",
                {"X-Listener-Auth-Token": master_token},
                {},
            )
        )
        send_telegram_broadcast(
            "Listener sweep trigger successfully deployed down-mesh.", "verbose"
        )

    # 6. VANGUARD OUTBOUND SWEEP
    if "vanguard" in agent_list:
        if isolation_matrix.get(VENDOR_MYEMAILVERIFIER, False):
            send_telegram_broadcast(
                "Skipping Vanguard outbound campaign sweep. Validation credit ledger reported isolation.",
                "critical",
            )
        else:
            url = os.getenv(
                "VANGUARD_API_URL", "https://polaris-vanguard-core.onrender.com"
            ).rstrip("/")
            asyncio.create_task(
                async_http_post_proxy(
                    f"{url}/api/v1/dispatch/sweep",
                    {
                        "X-Vanguard-Auth-Token": master_token,
                        "Content-Type": "application/json",
                    },
                    {},
                )
            )

            # ✅ Indented to run ONLY on actual successful proxy dispatch
            send_telegram_broadcast(
                "Vanguard outbound campaign sweep initiated successfully.", "verbose"
            )


async def async_http_post_proxy(
    url: str, headers: dict, json_data: dict, max_retries: int = 3
) -> bool:  # 🩹 Return a boolean indicator
    """🚀 HIGH-PERFORMANCE PROXY PROBE WITH LIVE EXCEPTION VERIFICATION"""
    backoff = 2.0
    # 🩹 Fix: Drop HTTP2 requirements and extend timeout to 30s to catch sleeping servers
    async with httpx.AsyncClient(http2=False, timeout=30.0) as client:
        for attempt in range(1, max_retries + 1):
            try:
                response = await client.post(url, headers=headers, json=json_data)
                status_code = response.status_code

                if 200 <= status_code < 300:
                    return True

                # 🩹 Catch explicit validation layout errors
                if status_code == 422:
                    try:
                        error_details = response.json()
                    except Exception:
                        error_details = response.text

                    send_telegram_broadcast(
                        f"❌ *SCHEMA MISMATCH (422)*\n"
                        f"Target Node: `{url}` rejected payload validation.\n"
                        f"Details: `{error_details}`",
                        "critical",
                    )
                    return False  # 🩹 Failed validation gate

                # 408/429 are commonly transient and should be retried.
                if status_code in (408, 429):
                    raise httpx.HTTPStatusError(
                        f"Retryable Client Status ({status_code})",
                        request=response.request,
                        response=response,
                    )

                if 400 <= status_code < 500:
                    response_excerpt = (response.text or "")[:300]
                    send_telegram_broadcast(
                        f"⚠️ *DOWNSTREAM CLIENT REJECTION*\n"
                        f"Target Node: `{url}` returned status `{status_code}`.\n"
                        f"Payload was not accepted.\n"
                        f"Excerpt: `{response_excerpt}`",
                        "critical",
                    )
                    return False

                if status_code in [502, 503, 504] or 500 <= status_code < 600:
                    raise httpx.HTTPStatusError(
                        f"Edge Server Error ({status_code})",
                        request=response.request,
                        response=response,
                    )

                send_telegram_broadcast(
                    f"⚠️ *UNEXPECTED DOWNSTREAM RESPONSE*\n"
                    f"Target Node: `{url}` returned status `{status_code}`.",
                    "verbose",
                )
                return False
            except Exception as e:
                # 🩹 Trace the hidden background error directly to your screen
                send_telegram_broadcast(
                    f"⚠️ [PROXY EXCEPTION] Attempt {attempt} failed targeting `{url}`. Error Context: `{str(e)}`",
                    "verbose",
                )
                if attempt == max_retries:
                    send_telegram_broadcast(
                        f"💥 *MESH NODE UNREACHABLE*\nEndpoint: `{url}` failed all connection attempts.\nFinal Network Error: `{e}`",
                        "critical",
                    )
                else:
                    await asyncio.sleep(backoff**attempt)
        return False  # Failed after max retries


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db_pool = AsyncConnectionPool(
        conninfo=get_validated_database_url(),
        open=False,
        check=AsyncConnectionPool.check_connection,
        max_idle=270.0,
    )
    anthropic_key = os.getenv("ANTHROPIC_API_KEY", "")
    app.state.ai_client = Anthropic(api_key=anthropic_key) if anthropic_key else None
    app.state.phantombuster_remediation = {}
    app.state.vendor_matrix_degraded = False

    try:
        await app.state.db_pool.open(wait=True)
        async with app.state.db_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(HELIX_INBOUND_BLUEPRINTS_TABLE_SQL)
                await cur.execute(SCHEDULER_TABLE_SQL)
                await cur.execute(HELIX_INFRASTRUCTURE_ORACLE_LEDGER_SQL)
            await conn.commit()

        await seed_oracle_ledger_rows(app.state.db_pool)
        await sync_external_vendor_credits(app.state.db_pool)
        app.state.credit_sync_task = asyncio.create_task(
            credit_sync_scheduler_loop(app.state.db_pool)
        )
        app.state.awareness_task = asyncio.create_task(perpetual_awareness_loop())
        send_telegram_broadcast(
            "Helix application cloud lifespan engine initialized successfully.",
            "success",
        )
    except Exception as exc:
        traceback.print_exc()
        raise exc
    yield

    app.state.awareness_task.cancel()
    if hasattr(app.state, "credit_sync_task"):
        app.state.credit_sync_task.cancel()

    with suppress(asyncio.CancelledError):
        await app.state.awareness_task
    if hasattr(app.state, "credit_sync_task"):
        with suppress(asyncio.CancelledError):
            await app.state.credit_sync_task

    await app.state.db_pool.close()


app = FastAPI(title="Helix Strategic Orchestration Core API", lifespan=lifespan)


async def analyze_and_orchestrate_log(
    agent_name: str, log_level: str, message: str, metadata: dict | None
) -> None:
    norm_msg = message.lower()
    normalized_log_level = str(log_level or "INFO").upper()
    meta = metadata if isinstance(metadata, dict) else {}
    industry = meta.get("industry_name") or meta.get("industry")

    # FIX: Recursively scan everything inside metadata to prevent missing nested tokens
    meta_string_dump = json.dumps(meta).lower()
    extended_error_context = f"{norm_msg} {meta_string_dump}"

    try:
        async with app.state.db_pool.connection() as conn:
            async with conn.cursor() as cur:

                if normalized_log_level == "INFO":
                    # =====================================================================
                    # 🛰️ CLOSED-LOOP AGENT POST-RUN VALIDATION HANDOFF
                    # =====================================================================
                    if "execution status: success" in norm_msg:
                        send_telegram_broadcast(
                            f"Agent `{agent_name}` reported an internal loop *SUCCESS* status metric.",
                            "success",
                        )

                        # Only validate the primary data-ingestion core nodes
                        if agent_name in ["beacon", "harvester"]:
                            master_token = os.getenv("INBOUND_API_KEY", "")
                            nexus_base_url = os.getenv(
                                "NEXUS_NERVOUS_IMPULSE_URL",
                                "https://polaris-nexus.onrender.com",
                            ).rstrip("/")

                            # Construct a clean payload matching Nexus's ValidationRequestSchema
                            validation_payload = {
                                "agent_validated": agent_name,
                                "industry_name": (
                                    industry if industry else "General Operations"
                                ),
                                "target_region": meta.get("target_region")
                                or meta.get("region")
                                or "National",
                            }

                            # Fire an out-of-band non-blocking task straight down to Nexus's validation receptor
                            asyncio.create_task(
                                async_http_post_proxy(
                                    f"{nexus_base_url}/api/v1/nexus/validate",
                                    {
                                        "X-Nexus-Auth-Token": master_token,
                                        "Content-Type": "application/json",
                                    },
                                    validation_payload,
                                )
                            )

                            send_telegram_broadcast(
                                f"🔍 *NEXUS AUDIT DEPLOYED*\n"
                                f"Helix has tasked Nexus to execute post-flight troubleshooting and verification metrics for agent: `{agent_name}`.",
                                "orchestration",
                            )

                    # =====================================================================
                    # 🧠 CASCADING HARVESTER FEEDBACK LOOP (ORGANIZED CHAOS)
                    # =====================================================================
                    if (
                        agent_name == "vanguard"
                        and "successfully processed batch" in norm_msg
                    ):
                        records_found = int(meta.get("records_found", 0))
                        status_flag = str(meta.get("status", "")).upper()

                        # If it finished cleanly but didn't hit the 500 cap, fire the next row!
                        if status_flag == "COMPLETED" and records_found < 500:
                            send_telegram_broadcast(
                                f"📉 *PARTIAL HARVEST DETECTED*\n"
                                f"Last run only secured `{records_found}` unique clean leads.\n"
                                f"MoltSets limit was NOT met. Helix is autonomously advancing the queue...",
                                "orchestration",
                            )

                            # Load the live infrastructure status matrix
                            vendor_isolation_matrix = (
                                await load_vendor_isolation_matrix(app.state.db_pool)
                            )

                            # Spawning explicit dict_row factory to safely parse indices via string keys
                            async with app.state.db_pool.connection() as cascade_conn:
                                async with cascade_conn.cursor(
                                    row_factory=dict_row
                                ) as cascade_cur:

                                    # Pull the NEXT active strategy from the sliding window scheduler
                                    await cascade_cur.execute("""
                                        SELECT industry_id, industry_name, moltset_category, bottleneck_profile, pain_tokens, target_region
                                        FROM helix_brain_scheduler 
                                        WHERE is_active = TRUE 
                                        AND (last_targeted_at IS NULL OR last_targeted_at <= NOW() - (minimum_cooldown_hours || ' hours')::INTERVAL)
                                        ORDER BY priority_score DESC, last_targeted_at ASC NULLS FIRST 
                                        LIMIT 1;
                                    """)
                                    next_strategy = await cascade_cur.fetchone()

                                    if next_strategy:
                                        # Mark it as targeted so the sliding window slides
                                        await cascade_cur.execute(
                                            "UPDATE helix_brain_scheduler SET last_targeted_at = NOW() WHERE industry_id = %s;",
                                            (next_strategy["industry_id"],),
                                        )
                                        await cascade_conn.commit()

                                        send_telegram_broadcast(
                                            f"🔄 *AUTONOMOUS QUEUE ADVANCEMENT*\n"
                                            f"Deploying Harvester next job for niche: *{next_strategy['industry_name']}* ({next_strategy['target_region']})",
                                            "orchestration",
                                        )

                                        # Fire ONLY the harvester worker thread for the next target segment
                                        asyncio.create_task(
                                            trigger_downstream_mesh_with_payload(
                                                next_strategy,
                                                ["harvester"],
                                                vendor_isolation_matrix,
                                            )
                                        )
                                    else:
                                        send_telegram_broadcast(
                                            "🏁 Harvester queue evaluation ended: No further active scheduler target slots open in the sliding window.",
                                            "orchestration",
                                        )
                    # =====================================================================

                    if "phantombuster" in extended_error_context:
                        if is_phantombuster_zero_result_signal(message, meta):
                            policy_state = apply_phantombuster_zero_result_policy(
                                industry
                            )
                            if policy_state:
                                send_telegram_broadcast(
                                    f"🧭 *PHANTOMBUSTER POLICY ADJUSTMENT*\n"
                                    f"Industry: `{policy_state['industry']}`\n"
                                    f"State: `{policy_state['state']}`\n"
                                    f"Keyword cap: `{policy_state['keyword_cap']}`",
                                    "orchestration",
                                )
                        else:
                            reset_phantombuster_policy_on_success(industry)

                if normalized_log_level == "ERROR":
                    # Expanded checks to catch variations of credit exhaustions
                    if any(
                        t in extended_error_context
                        for t in (
                            "phantombuster",
                            "credit limit",
                            "exhausted",
                            "api_key_expired",
                            "execution ceiling",
                        )
                    ):
                        await cur.execute(
                            """
                            UPDATE helix_infrastructure_oracle_ledger 
                            SET is_circuit_broken = TRUE, credits_remaining = 0 
                            WHERE vendor_name = %s;
                        """,
                            (VENDOR_PHANTOMBUSTERS,),
                        )
                        await cur.execute(
                            "UPDATE helix_infrastructure_oracle_ledger SET last_updated_at = NOW() WHERE vendor_name = %s;",
                            (VENDOR_PHANTOMBUSTERS,),
                        )
                        if industry:
                            await cur.execute(
                                "UPDATE helix_brain_scheduler SET is_active = FALSE WHERE industry_name = %s;",
                                (industry,),
                            )
                        send_telegram_broadcast(
                            "🚨 [CIRCUIT BREAKER] PhantomBusters tokens exhausted. Isolation engaged. Targeted industry paused.",
                            "critical",
                        )
                        await conn.commit()
                        return

                    if any(
                        t in extended_error_context
                        for t in (
                            "myemailverifier",
                            "insufficient credits",
                            "out of tokens",
                            "token balance low",
                        )
                    ):
                        await cur.execute(
                            """
                            UPDATE helix_infrastructure_oracle_ledger 
                            SET is_circuit_broken = TRUE, credits_remaining = 0 
                            WHERE vendor_name = %s;
                        """,
                            (VENDOR_MYEMAILVERIFIER,),
                        )
                        await cur.execute(
                            "UPDATE helix_infrastructure_oracle_ledger SET last_updated_at = NOW() WHERE vendor_name = %s;",
                            (VENDOR_MYEMAILVERIFIER,),
                        )
                        send_telegram_broadcast(
                            "🚨 [CIRCUIT BREAKER] MyEmailVerifier ledger empty. Outbound sweeps hard-stopped.",
                            "critical",
                        )
                        await conn.commit()
                        return

                    # Default fallback logic for industry auto-healing rescues
                    if industry:
                        await cur.execute(
                            """
                            SELECT industry_id, industry_name, moltset_category, bottleneck_profile, pain_tokens, target_region
                            FROM helix_brain_scheduler WHERE industry_name = %s LIMIT 1;
                        """,
                            (industry,),
                        )
                        strategy = await cur.fetchone()

                        if strategy and agent_name in ["nexus", "harvester"]:
                            send_telegram_broadcast(
                                f"🩹 *AUTO-HEALING ACTIVATED*\n"
                                f"Agent `{agent_name}` threw an error during the `{industry}` cycle.\n"
                                f"Helix is deploying an out-of-band rescue impulse to re-verify execution...",
                                "orchestration",
                            )

                            master_token = os.getenv("INBOUND_API_KEY", "")

                            if agent_name == "nexus":
                                url = os.getenv(
                                    "NEXUS_NERVOUS_IMPULSE_URL",
                                    "https://polaris-nexus.onrender.com",
                                ).rstrip("/")
                                asyncio.create_task(
                                    async_http_post_proxy(
                                        f"{url}/api/v1/nexus/awaken",
                                        {
                                            "X-Nexus-Auth-Token": master_token,
                                            "Content-Type": "application/json",
                                        },
                                        {
                                            "trigger_reason": "helix_auto_rescue_escalation",
                                            "batch_size": 15,
                                            "rescue_mode": True,
                                            "industry_id": strategy["industry_id"],
                                            "industry_name": strategy["industry_name"],
                                            "target_niche": strategy["industry_name"],
                                            "target_region": strategy["target_region"],
                                            "bottleneck_profile": strategy[
                                                "bottleneck_profile"
                                            ],
                                            "pain_tokens": strategy["pain_tokens"],
                                        },
                                    )
                                )

                            elif agent_name == "harvester":
                                url = os.getenv(
                                    "HARVESTER_API_URL",
                                    "https://polaris-vanguard-harvester-udf7.onrender.com",
                                ).rstrip("/")
                                asyncio.create_task(
                                    async_http_post_proxy(
                                        f"{url}/api/v1/harvester/execute",
                                        {
                                            "X-Harvester-Auth-Token": master_token,
                                            "Content-Type": "application/json",
                                        },
                                        {**strategy, "rescue_mode": True},
                                    )
                                )

            await conn.commit()
    except Exception as exc:
        print(f"Background healer exception: {exc}")


@app.post("/api/v1/ingest")
async def ingest_leads(
    leads: List[LeadIngestModel],
    background_tasks: BackgroundTasks,
    x_ingest_key: str | None = Header(default=None),
):
    expected_key = os.getenv("INGEST_API_KEY")
    if expected_key and not (
        x_ingest_key and hmac.compare_digest(x_ingest_key, expected_key)
    ):
        raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        industry_counts = {}
        async with app.state.db_pool.connection() as conn:
            async with conn.cursor() as cur:
                for lead in leads:
                    await cur.execute(
                        """
                        INSERT INTO cold_leads (email, first_name, last_name, company, title, website, industry)
                        VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (email) DO NOTHING;
                    """,
                        (
                            lead.email,
                            lead.first_name,
                            lead.last_name,
                            lead.company,
                            lead.title,
                            lead.website,
                            lead.industry,
                        ),
                    )

                    if lead.industry:
                        industry_counts[lead.industry] = (
                            industry_counts.get(lead.industry, 0) + 1
                        )
            await conn.commit()

        for industry, count in industry_counts.items():
            await evaluate_lead_density_and_escalate(
                industry, count, app.state.db_pool, background_tasks
            )

        return {"status": "success", "processed": len(leads)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to ingest leads: {exc}")


@app.post("/api/v1/inbound-blueprints")
async def inbound_blueprints(
    payload: BlueprintInboundModel,
    background_tasks: BackgroundTasks,
    x_inbound_key: str | None = Header(default=None),
):
    expected_key = os.getenv("INBOUND_API_KEY")
    if expected_key and not (
        x_inbound_key and hmac.compare_digest(x_inbound_key, expected_key)
    ):
        raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        async with app.state.db_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO helix_inbound_blueprints (
                        headcount, weekly_hours_wasted, annual_wasted_hours, 
                        annual_financial_bleed, friction_points, name, 
                        company_name, email, status, industry_name
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """,
                    (
                        payload.headcount,
                        payload.weekly_hours_wasted,
                        payload.annual_wasted_hours,
                        payload.annual_financial_bleed,
                        Jsonb(payload.friction_points),
                        payload.name,
                        payload.company_name,
                        payload.email,
                        payload.status,
                        payload.industry_name,
                    ),
                )
            await conn.commit()

        if payload.industry_name and payload.industry_name != "Unassigned":
            await evaluate_lead_density_and_escalate(
                payload.industry_name, 10, app.state.db_pool, background_tasks
            )

        return {"status": "success"}
    except Exception as exc:
        raise HTTPException(
            status_code=500, detail=f"Failed to ingest inbound blueprint: {exc}"
        )


@app.post("/api/v1/leads/status-update")
async def update_lead_status_and_attribute(
    payload: LeadStatusUpdateModel,
    email: str = Query(...),
    x_vanguard_key: str | None = Header(default=None),
):
    expected_key = os.getenv("VANGUARD_API_KEY") or os.getenv("INBOUND_API_KEY")
    if expected_key and not (
        x_vanguard_key and hmac.compare_digest(x_vanguard_key, expected_key)
    ):
        raise HTTPException(status_code=401, detail="Unauthorized Tracking Call")

    try:
        async with app.state.db_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    UPDATE cold_leads 
                    SET status = %s, 
                        updated_at = NOW() 
                    WHERE email = %s;
                """,
                    (payload.status, email),
                )
            await conn.commit()

        asyncio.create_task(
            process_roi_attribution_boost(email, payload.status, app.state.db_pool)
        )
        return {"status": "attribution_processed"}
    except Exception as exc:
        raise HTTPException(
            status_code=500, detail=f"Attribution tracker exception: {exc}"
        )


@app.post("/api/v1/logs")
async def ingest_log(payload: LogIngestSchema):
    try:
        async with app.state.db_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "INSERT INTO agent_system_logs (agent_name, log_level, message, metadata) VALUES (%s, %s, %s, %s) RETURNING id;",
                    (
                        payload.agent_name,
                        payload.log_level,
                        payload.message,
                        Jsonb(payload.metadata) if payload.metadata else None,
                    ),
                )
                row = await cur.fetchone()
            await conn.commit()
        asyncio.create_task(
            analyze_and_orchestrate_log(
                payload.agent_name, payload.log_level, payload.message, payload.metadata
            )
        )
        return {"status": "success", "logged_id": row[0]}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/v1/logs/latest")
async def get_latest_agent_log(
    agent: str, x_inbound_key: str | None = Header(default=None)
):
    expected_key = os.getenv("INBOUND_API_KEY")
    if expected_key and not (
        x_inbound_key and hmac.compare_digest(x_inbound_key, expected_key)
    ):
        raise HTTPException(status_code=401, detail="Unauthorized")

    async with app.state.db_pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT id, agent_name, log_level, message, metadata, timestamp FROM agent_system_logs WHERE agent_name = %s ORDER BY id DESC LIMIT 1;",
                (agent,),
            )
            row = await cur.fetchone()
    if not row:
        return {"status": "empty", "message": "No logs found for this agent."}
    return {**row, "timestamp": row["timestamp"].isoformat()}


@app.post("/api/v1/orchestrator/run-cycle")
async def execute_orchestrated_master_cycle(
    background_tasks: BackgroundTasks,
    x_helix_auth_token: Optional[str] = Header(default=None),
):
    expected = os.getenv("HELIX_AUTH_TOKEN")
    if expected and not (
        x_helix_auth_token and hmac.compare_digest(x_helix_auth_token, expected)
    ):
        raise HTTPException(status_code=401, detail="Unauthorized System Override Call")

    async with app.state.db_pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            # ─── ITEM 4: SLIDING WINDOW WINDOW ADDED HERE TO MANUAL CYCLES ───
            await cur.execute("""
                SELECT industry_id, industry_name, moltset_category, bottleneck_profile, pain_tokens, target_region
                FROM helix_brain_scheduler 
                WHERE is_active = TRUE 
                AND (
                    last_targeted_at IS NULL 
                    OR last_targeted_at <= NOW() - (minimum_cooldown_hours || ' hours')::INTERVAL
                )
                ORDER BY priority_score DESC, last_targeted_at ASC NULLS FIRST 
                LIMIT 1;
            """)
            strategy = await cur.fetchone()
            if strategy:
                await cur.execute(
                    "UPDATE helix_brain_scheduler SET last_targeted_at = NOW() WHERE industry_id = %s;",
                    (strategy["industry_id"],),
                )
        await conn.commit()

    if not strategy:
        return {
            "status": "idle",
            "message": "Zero active targeting strategies configured.",
        }

    # 🩹 NEW ADDITION HERE: Build the live matrix for the manual trigger
    vendor_isolation_matrix = await load_vendor_isolation_matrix(app.state.db_pool)

    send_telegram_broadcast(
        f"Manual override master cycle triggered for niche: `{strategy['industry_name']}`",
        "orchestration",
    )

    background_tasks.add_task(
        trigger_downstream_mesh_with_payload,
        strategy,
        ["nexus", "beacon", "harvester", "vanguard", "listener"],
        vendor_isolation_matrix,  # 🩹 FIXED HERE: Passes real data instead of {}
    )
    return {
        "status": "orchestration_cycle_pushed",
        "active_strategy": strategy["industry_name"],
    }


@app.get("/api/v1/scheduler/current")
async def get_current_scheduler_target(
    x_inbound_key: str | None = Header(default=None),
):
    expected_key = os.getenv("INBOUND_API_KEY")
    if expected_key and not (
        x_inbound_key and hmac.compare_digest(x_inbound_key, expected_key)
    ):
        raise HTTPException(status_code=401, detail="Unauthorized")

    async with app.state.db_pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            # ─── ITEM 4: SLIDING WINDOW ALIGNMENT FOR DIAGNOSTIC ENDPOINTS ───
            await cur.execute("""
                SELECT industry_id, industry_name, moltset_category, bottleneck_profile, pain_tokens, target_region, last_targeted_at, priority_score
                FROM helix_brain_scheduler 
                WHERE is_active = TRUE
                AND (
                    last_targeted_at IS NULL 
                    OR last_targeted_at <= NOW() - (minimum_cooldown_hours || ' hours')::INTERVAL
                )
                ORDER BY priority_score DESC, last_targeted_at ASC NULLS FIRST LIMIT 1;
            """)
            row = await cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="No active strategy configured.")
    return {
        **row,
        "last_targeted_at": (
            row["last_targeted_at"].isoformat() if row.get("last_targeted_at") else None
        ),
    }


@app.get("/api/v1/orchestrator/dashboard-diagnostics")
async def get_dashboard_diagnostics(
    x_helix_auth_token: Optional[str] = Header(default=None),
):
    expected = os.getenv("HELIX_AUTH_TOKEN")
    if expected and not (
        x_helix_auth_token and hmac.compare_digest(x_helix_auth_token, expected)
    ):
        raise HTTPException(
            status_code=401, detail="Unauthorized Dashboard Gateway Access."
        )

    est_now = get_new_york_now()

    diagnostics = {
        "timestamp": est_now.isoformat(),
        "todays_achievements": {},
        "infrastructure_logjams": {},
        "pipeline_progress": {},
    }

    try:
        async with app.state.db_pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:

                await cur.execute("""
                    SELECT agent_name, COUNT(*) as action_count, MAX(message) as last_milestone
                    FROM agent_system_logs 
                    WHERE timestamp >= (NOW() AT TIME ZONE 'America/New_York')::date 
                    AND log_level = 'INFO'
                    GROUP BY agent_name;
                """)
                achievements = await cur.fetchall()
                for row in achievements:
                    diagnostics["todays_achievements"][row["agent_name"]] = {
                        "status": "COMPLETE" if row["action_count"] > 0 else "IDLE",
                        "actions_logged": row["action_count"],
                        "last_seen_milestone": row["last_milestone"],
                    }

                try:
                    await cur.execute("""
                        SELECT vendor_name, credits_remaining, is_circuit_broken 
                        FROM helix_infrastructure_oracle_ledger;
                    """)
                    vendors = await cur.fetchall()
                    for v in vendors:
                        status_label = (
                            "BLOCKED" if v["is_circuit_broken"] else "OPERATIONAL"
                        )
                        reason = "Healthy parameters"
                        action = "None required"

                        if (
                            v["vendor_name"] == VENDOR_PHANTOMBUSTERS
                            and v["is_circuit_broken"]
                        ):
                            reason = "API tokens exhausted or execution limit reached. Downstream triggers suppressed."
                            action = "Upgrade account to the $56 Starter Plan (20-hour monthly execution ceiling)."
                        elif (
                            v["vendor_name"] == VENDOR_MYEMAILVERIFIER
                            and v["is_circuit_broken"]
                        ):
                            reason = "Validation credit ledger reporting depletion. Outbound campaigns paused."
                            action = "Replenish verification balance to restore campaign flow."
                        elif (
                            v["vendor_name"] == VENDOR_MOLTSET
                            and v["is_circuit_broken"]
                        ):
                            reason = (
                                "Enterprise data limits hit or API token key invalid."
                            )
                            action = "Verify account token billing profiles on Moltset console."

                        diagnostics["infrastructure_logjams"][v["vendor_name"]] = {
                            "status": status_label,
                            "credits": (
                                float(v["credits_remaining"])
                                if v["credits_remaining"] is not None
                                else 0.0
                            ),
                            "bottleneck_reason": reason,
                            "required_resolution": action,
                        }
                except Exception as inner_db:
                    diagnostics["infrastructure_logjams"][
                        "error"
                    ] = f"Oracle ledger unreadable: {inner_db}"

        vanguard_url = os.getenv("VANGUARD_DATABASE_URL")
        if vanguard_url:
            if "?sslmode=require" not in vanguard_url:
                vanguard_url += "?sslmode=require"

            async with await psycopg.AsyncConnection.connect(vanguard_url) as v_conn:
                async with v_conn.cursor() as v_cur:
                    await v_cur.execute(
                        "SELECT COUNT(*) FROM cold_leads WHERE status = 'scraped';"
                    )
                    scraped_row = await v_cur.fetchone()
                    scraped_count = scraped_row[0] if scraped_row else 0

                    await v_cur.execute(
                        "SELECT COUNT(*) FROM cold_leads WHERE status = 'ready_to_send';"
                    )
                    ready_row = await v_cur.fetchone()
                    ready_count = ready_row[0] if ready_row else 0

                    vector_status = "STALLED / STAGING" if scraped_count > 0 else "IDLE"
                    vector_reason = "None"
                    if vector_status == "STALLED / STAGING":
                        vector_reason = f"{scraped_count} leads successfully mined. Processing held because outbound accounts are locked in Instantly warmup matrix."

                    diagnostics["pipeline_progress"] = {
                        "harvester_reservoir": {
                            "status": "ACTIVE",
                            "leads_cached_raw": scraped_count,
                        },
                        "vector_enrichment": {
                            "status": vector_status,
                            "leads_enriched_ready": ready_count,
                            "stalled_reason": vector_reason,
                        },
                    }
        else:
            diagnostics["pipeline_progress"][
                "error"
            ] = "VANGUARD_DATABASE_URL environment variable is missing."

        return diagnostics

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(
            status_code=500, detail=f"Dashboard telemetry compilation failure: {str(e)}"
        )


if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
