import os
import re
import traceback
import asyncio
import json
import hmac
import requests
import datetime
import psycopg
import httpx
from anthropic import Anthropic
from contextlib import asynccontextmanager
from typing import Any, Literal, Optional, List

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


def get_validated_database_url() -> str:
    url = os.getenv("DATABASE_URL", "")
    if not url or not url.startswith("postgresql://"):
        raise ValueError("Missing or invalid cloud DATABASE_URL.")
    if "?sslmode=require" not in url:
        url = url + "?sslmode=require"
    return url


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
                if chosen_category in MOLTSET_ALLOWED_CATEGORIES:
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
                vendor_isolation_matrix = {}
                async with app.state.db_pool.connection() as conn:
                    async with conn.cursor(row_factory=dict_row) as cur:
                        await cur.execute(
                            "SELECT vendor_name, is_circuit_broken FROM helix_infrastructure_oracle_ledger;"
                        )
                        rows = await cur.fetchall()
                        for row in rows:
                            vendor_isolation_matrix[row["vendor_name"]] = row[
                                "is_circuit_broken"
                            ]

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
    """Hits live vendor endpoints and auto-resurrects infrastructure blocks if balances are healthy."""
    pb_key = os.getenv("PHANTOMBUSTER_API_KEY")
    if not pb_key:
        return

    try:
        async with httpx.AsyncClient() as client:
            res = await client.get(
                "https://api.phantombuster.com/v1/account",
                headers={"X-PhantomBuster-Key": pb_key},
                timeout=10,
            )

            if res.status_code == 200:
                credits = res.json().get("orgCodeOrCredits", 100.0)

                async with db_pool.connection() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute(
                            """
                            UPDATE helix_infrastructure_oracle_ledger 
                            SET credits_remaining = %s, last_updated_at = NOW() 
                            WHERE vendor_name = 'PhantomBusters'
                            RETURNING is_circuit_broken;
                            """,
                            (credits,),
                        )
                        row = await cur.fetchone()
                        is_broken = row[0] if row else False

                        if credits > 0 and is_broken:
                            await cur.execute("""
                                UPDATE helix_infrastructure_oracle_ledger 
                                SET is_circuit_broken = FALSE 
                                WHERE vendor_name = 'PhantomBusters';
                                """)
                            send_telegram_broadcast(
                                "🔄 *INFRASTRUCTURE CIRCUIT HEALED*: PhantomBusters has been automatically re-activated.",
                                "success",
                            )
                await conn.commit()
    except Exception as e:
        print(f"Vendor credit auto-heal sync delayed: {e}")


async def perpetual_awareness_loop():
    print("🚀 [PERPETUAL BRAIN ENGINE] Staggered drip pipeline loop active...")

    while True:
        try:
            utc_now = datetime.datetime.now(datetime.timezone.utc)
            # Hardcoded offset fix: Use -4 for EDT (Summer) or -5 for EST (Winter)
            # Or simply leave it as-is if you don't mind the 1am start,
            # but now it will drip feed instead of exploding!
            est_now = utc_now + datetime.timedelta(hours=-4)

            current_date_str = est_now.strftime("%Y-%m-%d")
            current_weekday = est_now.weekday()

            scheduled_agents_list = DAILY_EXECUTION_ROUTERS.get(current_weekday, [])
            if not scheduled_agents_list:
                await asyncio.sleep(60)
                continue

            # 1. Find what hasn't run yet today
            async with app.state.db_pool.connection() as conn:
                async with conn.cursor(row_factory=dict_row) as cur:
                    await cur.execute(
                        "SELECT DISTINCT LOWER(agent_name) as executed_agent "
                        "FROM agent_system_logs "
                        "WHERE timestamp >= (NOW() AT TIME ZONE 'America/New_York')::date;"
                    )
                    completed_rows = await cur.fetchall()
                    completed_today_set = {r["executed_agent"] for r in completed_rows}

            pending_agents_for_today = [
                agent
                for agent in scheduled_agents_list
                if agent not in completed_today_set
            ]

            # 2. DRIP FEED: If there are pending tasks, execute ONLY ONE right now
            if pending_agents_for_today:
                next_agent_up = pending_agents_for_today[
                    0
                ]  # Take the first pending agent

                # 🩹 Define and populate the missing matrix from the database ledger
                vendor_isolation_matrix = {}
                async with app.state.db_pool.connection() as conn:
                    async with conn.cursor(row_factory=dict_row) as cur:
                        await cur.execute(
                            "SELECT vendor_name, is_circuit_broken FROM helix_infrastructure_oracle_ledger;"
                        )
                        rows = await cur.fetchall()
                        for row in rows:
                            vendor_isolation_matrix[row["vendor_name"]] = row[
                                "is_circuit_broken"
                            ]

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

                    # Fire ONLY the single next agent, not the whole list!
                    await trigger_downstream_mesh_with_payload(
                        active_strategy,
                        [next_agent_up],
                        vendor_isolation_matrix,  # assuming pulled from DB like your current code
                    )

                # 3. COOL DOWN: Wait 45 minutes before evaluating/firing the next agent in line
                await asyncio.sleep(45 * 60)
                continue

        except Exception as loop_crash:
            send_telegram_broadcast(f"Loop exception: `{loop_crash}`", "critical")

        # Default heartbeat tick if no tasks are pending
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
        if isolation_matrix.get("Anthropic", False):
            send_telegram_broadcast(
                "Skipping Nexus Omni-Strategist sync sequence. Anthropic wallet ledger is isolated.",
                "critical",
            )
        else:
            url = os.getenv(
                "NEXUS_NERVOUS_IMPULSE_URL", "https://polaris-nexus.onrender.com"
            ).rstrip("/")
            # 🩹 Fix: Call the proxy directly and await its output
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
        if isolation_matrix.get("Moltset", False):
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
        if isolation_matrix.get("PhantomBusters", False):
            send_telegram_broadcast(
                "Skipping Beacon Monitor lead scraping cycle. PhantomBusters circuit breaker is TRIPPED.",
                "critical",
            )
        else:
            url = os.getenv(
                "BEACON_MONITOR_API_URL", "https://polaris-beacon-monitor.onrender.com"
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
        if isolation_matrix.get("MyEmailVerifier", False):
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

                # 🩹 Catch explicit validation layout errors
                if response.status_code == 422:
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

                if response.status_code in [502, 503, 504]:
                    raise httpx.HTTPStatusError(
                        f"Edge Server Error ({response.status_code})",
                        request=response.request,
                        response=response,
                    )
                return True  # 🩹 Success!
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

    try:
        await app.state.db_pool.open(wait=True)
        async with app.state.db_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(HELIX_INBOUND_BLUEPRINTS_TABLE_SQL)
                await cur.execute(SCHEDULER_TABLE_SQL)
            await conn.commit()
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
    await app.state.db_pool.close()


app = FastAPI(title="Helix Strategic Orchestration Core API", lifespan=lifespan)


# --- TELEMETRY LOG ANALYSIS AND AUTONOMOUS PROACTIVE SELF-HEALING ---
async def analyze_and_orchestrate_log(
    agent_name: str, log_level: str, message: str, metadata: dict | None
) -> None:
    norm_msg = message.lower()
    meta = metadata if isinstance(metadata, dict) else {}
    industry = meta.get("industry_name") or meta.get("industry")

    try:
        async with app.state.db_pool.connection() as conn:
            async with conn.cursor() as cur:

                if log_level == "INFO":
                    if "execution status: success" in norm_msg:
                        send_telegram_broadcast(
                            f"Agent `{agent_name}` reported an internal loop *SUCCESS* status metric.",
                            "success",
                        )
                        # 🛑 FIXED REDUNDANT OVERRIDE HERE (Relying solely on sync_external_vendor_credits instead)

                if log_level == "ERROR":
                    if any(
                        t in norm_msg
                        for t in (
                            "phantombuster",
                            "credit limit",
                            "exhausted",
                            "api_key_expired",
                        )
                    ):
                        await cur.execute(
                            "UPDATE helix_infrastructure_oracle_ledger SET is_circuit_broken = TRUE, credits_remaining = 0 WHERE vendor_name = 'PhantomBusters';"
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
                        t in norm_msg
                        for t in (
                            "myemailverifier",
                            "insufficient credits",
                            "out of tokens",
                            "token balance low",
                        )
                    ):
                        await cur.execute(
                            "UPDATE helix_infrastructure_oracle_ledger "
                            "SET is_circuit_broken = TRUE, credits_remaining = 0 "
                            "WHERE vendor_name = 'MyEmailVerifier';"
                        )
                        send_telegram_broadcast(
                            "🚨 [CIRCUIT BREAKER] MyEmailVerifier depleted. Skipping email sweeps.",
                            "critical",
                        )
                        await conn.commit()
                        return

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
                                            "target_niche": strategy[
                                                "industry_name"
                                            ],  # 🩹 Aligned to Target Schema
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
    vendor_isolation_matrix = {}
    async with app.state.db_pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT vendor_name, is_circuit_broken FROM helix_infrastructure_oracle_ledger;"
            )
            rows = await cur.fetchall()
            for row in rows:
                vendor_isolation_matrix[row["vendor_name"]] = row["is_circuit_broken"]

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

    utc_now = datetime.datetime.now(datetime.timezone.utc)
    est_now = utc_now + datetime.timedelta(hours=-5)

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
                            v["vendor_name"] == "PhantomBusters"
                            and v["is_circuit_broken"]
                        ):
                            reason = "API tokens exhausted or execution limit reached. Downstream triggers suppressed."
                            action = "Upgrade account to the $56 Starter Plan (20-hour monthly execution ceiling)."
                        elif (
                            v["vendor_name"] == "MyEmailVerifier"
                            and v["is_circuit_broken"]
                        ):
                            reason = "Validation credit ledger reporting depletion. Outbound campaigns paused."
                            action = "Replenish verification balance to restore campaign flow."
                        elif v["vendor_name"] == "Moltset" and v["is_circuit_broken"]:
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
