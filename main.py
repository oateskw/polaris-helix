import os
import re
import traceback
import asyncio
import json
import hmac
import requests
from anthropic import Anthropic
from contextlib import asynccontextmanager
from typing import Any, Literal, Optional, List

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, Request, Header
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, ConfigDict, model_validator
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool
import uvicorn

load_dotenv()

# Strict Allowed Vendor Categories list for MoltSet Validation Mapping
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
    """Return a validated, SSL-enforced DATABASE_URL or raise descriptively."""
    url = os.getenv("DATABASE_URL", "")
    if not url or not url.startswith("postgresql://"):
        raise ValueError(
            "Missing or invalid cloud DATABASE_URL. Please set your Neon Postgres "
            "connection string in your environment or .env file."
        )
    if "?sslmode=require" not in url:
        url = url + "?sslmode=require"
    return url


def run_deterministic_moltset_mapping(ai_client: Anthropic, industry_name: str) -> str:
    """Uses a rigid, zero-creativity LLM node to resolve dynamic niches into vendor schemas."""

    prompt = f"""You are a strict data normalization engine built for API infrastructure pipelines.
    Map this specific business niche or industry name: "{industry_name}"
    To exactly ONE parent category from this strictly authorized vendor list: {MOLTSET_ALLOWED_CATEGORIES}
    
    CRITICAL RULES:
    - Output ONLY the exact text string of the matching category from the list.
    - Do NOT include any punctuation, quotes, introduction, explanation, or conversational prose.
    - Do NOT wrap the answer in Markdown formatting or backticks.
    - If no clear match exists, map it to the closest logical operational bucket (Default: Professional and Business Services).
    
    Example Output: Finance and Banking"""
    try:
        message = ai_client.messages.create(
            model=os.getenv("ANTHROPIC_MODEL", "claude-3-5-sonnet-latest"),
            max_tokens=50,
            temperature=0.0,  # Strict mathematical focus matching automation rules
            messages=[{"role": "user", "content": prompt}],
        )
        chosen_category = (
            message.content[0]
            .text.strip()
            .replace('"', "")
            .replace("'", "")
            .replace("`", "")
            .strip()
        )
        if chosen_category in MOLTSET_ALLOWED_CATEGORIES:
            return chosen_category
    except Exception as e:
        print(
            f"⚠️ [MOLTSET INTEGRATION MAPPING DROP] Internal classification failed: {e}"
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
                if "vector" in raw_agent:
                    data["agent_name"] = "vector"
                elif "nexus" in raw_agent:
                    data["agent_name"] = "nexus"
                else:
                    data["agent_name"] = raw_agent

            if "message" not in data or not data["message"]:
                event_name = data.get("event", "generic_background_telemetry")
                data["message"] = f"Background pipeline activity: {event_name}"

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


class SchedulerTargetUpdateModel(BaseModel):
    industry_name: str
    target_region: str


class SchedulerRecordIngestModel(BaseModel):
    industry_name: str
    bottleneck_profile: str
    pain_tokens: List[str]
    target_region: str = "Texas"
    priority_score: int = 1


INBOUND_BLUEPRINTS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS inbound_blueprints (
    id                     BIGSERIAL    PRIMARY KEY,
    headcount              INTEGER      NOT NULL,
    weekly_hours_wasted    INTEGER      NOT NULL,
    annual_wasted_hours    INTEGER      NOT NULL,
    annual_financial_bleed DOUBLE PRECISION NOT NULL,
    friction_points        JSONB        NOT NULL,
    name                   TEXT         NOT NULL,
    company_name           TEXT         NOT NULL,
    email                  TEXT         NOT NULL,
    created_at             TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_inbound_blueprints_email ON inbound_blueprints (email);
CREATE INDEX IF NOT EXISTS idx_inbound_blueprints_created_at ON inbound_blueprints (created_at DESC);
"""

# Upgraded to include strict vendor tracking arrays natively
SCHEDULER_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS helix_brain_scheduler (
    industry_id SERIAL PRIMARY KEY,
    industry_name VARCHAR(100) UNIQUE NOT NULL,
    moltset_category VARCHAR(100) DEFAULT 'Professional and Business Services',
    bottleneck_profile TEXT NOT NULL,
    pain_tokens TEXT[] NOT NULL,
    target_region VARCHAR(100) DEFAULT 'Texas',
    is_active BOOLEAN DEFAULT TRUE,
    last_targeted_at TIMESTAMP DEFAULT NULL,
    priority_score INT DEFAULT 1
);
CREATE INDEX IF NOT EXISTS idx_hbs_last_targeted ON helix_brain_scheduler (last_targeted_at DESC);
"""

SEED_SCHEDULER_SQL = """
INSERT INTO helix_brain_scheduler (industry_name, moltset_category, bottleneck_profile, pain_tokens, target_region)
VALUES
('Home Services', 'Professional and Business Services', 'Inbound emergency calls rolling to voicemail.', ARRAY['hvac', 'plumbing'], 'Texas'),
('Accounting Firms', 'Professional and Business Services', 'Manual client intake document tracking friction.', ARRAY['accounting', 'bookkeeping'], 'California'),
('Medical Clinics', 'Health and Pharmaceuticals', 'Open chair utilization inefficiencies.', ARRAY['dental', 'medical spa'], 'Florida')
ON CONFLICT (industry_name) DO NOTHING;
"""


async def log_helix_state(
    cur, strategic_mode: str, geo_lock: str, buying_signal: str
) -> None:
    """📡 HELIX SYSTEM STATE LOG TRANSMITTER"""
    metadata_payload = {
        "strategic_mode": strategic_mode,
        "geo_lock": geo_lock,
        "buying_signal": buying_signal,
    }
    await cur.execute(
        """
        INSERT INTO agent_system_logs (agent_name, log_level, message, metadata)
        VALUES ('helix', 'INFO', 'Global system parameters synchronized.', %s);
        """,
        (Jsonb(metadata_payload),),
    )
    print(f"[📡 HELIX TELEMETRY] Logged target adjustments: {metadata_payload}")


async def ensure_inbound_blueprints_table() -> None:
    async with app.state.db_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(INBOUND_BLUEPRINTS_TABLE_SQL)
        await conn.commit()


@asynccontextmanager
async def lifespan(app: FastAPI):
    database_url = get_validated_database_url()

    app.state.db_pool = AsyncConnectionPool(
        conninfo=database_url,
        open=False,
        check=AsyncConnectionPool.check_connection,
        max_idle=270.0,
    )

    anthropic_key = os.getenv("ANTHROPIC_API_KEY", "")
    app.state.ai_client = Anthropic(api_key=anthropic_key) if anthropic_key else None

    try:
        await app.state.db_pool.open(wait=True)
        await ensure_inbound_blueprints_table()
        async with app.state.db_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(SCHEDULER_TABLE_SQL)
                # Apply column patch if missing on active serverless connections safely
                await cur.execute(
                    "ALTER TABLE helix_brain_scheduler ADD COLUMN IF NOT EXISTS moltset_category VARCHAR(100) DEFAULT 'Professional and Business Services';"
                )
                await cur.execute(SEED_SCHEDULER_SQL)
            await conn.commit()
    except Exception as exc:
        print("\n[helix] CLOUD CONNECTION FAILED — Full trace:\n")
        traceback.print_exc()
        raise exc
    try:
        yield
    finally:
        await app.state.db_pool.close()


app = FastAPI(title="Helix Log Intake API", lifespan=lifespan)


from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse


# Expanded helper method inside main.py
async def evaluate_ecosystem_demand_and_signal_nexus() -> None:
    """🧠 THE BRAIN'S INTENT TRACKER
    Queries the reservoir state to check if processing thresholds are met,
    then dynamically fires a nervous impulse to Nexus out-of-band.
    """
    try:
        async with app.state.db_pool.connection() as conn:
            async with conn.cursor() as cur:
                # 1. Query the live staging state to check for addressable fuel
                cur.execute(
                    "SELECT COUNT(*) FROM cold_leads WHERE status = 'ready_to_send';"
                )
                ready_count = (await cur.fetchone())[0]

                # 2. Check for immediate abandonment rescue sessions
                cur.execute(
                    "SELECT COUNT(*) FROM cold_leads WHERE status = 'pending' AND processed_at < NOW() - INTERVAL '15 minutes';"
                )
                abandoned_count = (await cur.fetchone())[0]

        # 3. Dynamic Threshold Evaluation Gate
        if ready_count >= 25 or abandoned_count > 0:
            print(
                f"🧠 [HELIX ORACLE] Demand threshold crossed (Ready: {ready_count}, Abandoned: {abandoned_count}). Awakening Nexus!"
            )

            # 🧠 Updated key to match the new biological naming convention
            nexus_webhook_url = os.getenv(
                "NEXUS_NERVOUS_IMPULSE_URL", "https://polaris-nexus.onrender.com/"
            )
            headers = {"X-Nexus-Auth-Token": os.getenv("INBOUND_API_KEY", "")}
            payload = {
                "trigger_reason": "volumetric_threshold_satisfied",
                "batch_size": ready_count,
                "rescue_mode": abandoned_count > 0,
            }

            # Fire-and-forget impulse to Nexus to keep Helix API completely non-blocking
            asyncio.create_task(
                async_http_post_proxy(
                    nexus_webhook_url.rstrip("/") + "/api/v1/nexus/awaken",
                    headers,
                    payload,
                )
            )

    except Exception as exc:
        print(f"⚠️ Helix intent tracker failure: {exc}")


async def async_http_post_proxy(url: str, headers: dict, json_data: dict):
    try:
        requests.post(url, headers=headers, json=json_data, timeout=5)
    except Exception:
        pass  # Soft failure boundary preserved to guarantee platform uptime


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    body = await request.body()
    print(f"\n[422 VALIDATION ERROR] Caught a bad payload!")
    print(f"Errors: {exc.errors()}")
    return JSONResponse(
        status_code=422,
        content={"detail": exc.errors(), "body": body.decode("utf-8", errors="ignore")},
    )


# Add this endpoint in main.py to allow Nexus to read vendor health states dynamically
@app.get("/api/v1/oracle/vendor-health")
async def get_vendor_health(x_inbound_key: str | None = Header(default=None)):
    expected_key = os.getenv("INBOUND_API_KEY")
    if expected_key and not (
        x_inbound_key and hmac.compare_digest(x_inbound_key, expected_key)
    ):
        raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        async with app.state.db_pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                # Updated table name to follow helix_ prefixing rules
                await cur.execute(
                    "SELECT vendor_name, credits_remaining, is_circuit_broken FROM helix_infrastructure_oracle_ledger;"
                )
                rows = await cur.fetchall()
        return {
            row["vendor_name"]: {
                "credits": float(row["credits_remaining"]),
                "isolated": row["is_circuit_broken"],
            }
            for row in rows
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Oracle read failure: {exc}")


# Proactive Burn Rate & Credit Tracking Engine inside main.py
async def analyze_and_orchestrate_log(
    agent_name: str,
    log_level: str,
    message: str,
    metadata: dict | None,
) -> None:
    metadata_dict = metadata if isinstance(metadata, dict) else {}
    normalized_message = message.lower()
    industry_name = metadata_dict.get("industry_name") or metadata_dict.get("industry")

    try:
        async with app.state.db_pool.connection() as conn:
            async with conn.cursor() as cur:

                # ─── 1. PROACTIVE TRACKING: DEDUCT ON SUCCESSFUL RUNS ───
                if log_level == "INFO" and agent_name == "nexus":
                    # When Nexus reports a successful run, deduct the standard PhantomBuster execution cost
                    # Let's assume a standard profile scrape run burns roughly 10 minutes of execution time
                    if "execution status: success" in normalized_message:
                        await cur.execute("""
                            UPDATE helix_infrastructure_oracle_ledger
                            SET credits_remaining = GREATEST(credits_remaining - 10.00, 0),
                                is_circuit_broken = CASE WHEN credits_remaining - 10.00 <= 0 THEN TRUE ELSE is_circuit_broken END,
                                last_updated_at = NOW()
                            WHERE vendor_name = 'PhantomBusters'
                            RETURNING credits_remaining, is_circuit_broken;
                            """)
                        ledger_check = await cur.fetchone()
                        if ledger_check and ledger_check[1]:
                            print(
                                "🚨 [PROACTIVE HEALER] PhantomBuster calculated tokens dropped to 0. Circuit breaker automatically closed!"
                            )
                            # Optional: trigger_telegram_alert("⚠️ *Oracle Warning:* Calculated PhantomBuster minutes exhausted. Pipeline isolated preemptively.")

                # ─── 2. REACTIVE TRACKING: FALLBACK ERROR INTERCEPTION ───
                has_pb_credit_error = any(
                    token in normalized_message
                    for token in (
                        "phantombuster",
                        "credit limit",
                        "payment required",
                        "exhausted",
                        "api_key_expired",
                    )
                )

                if log_level == "ERROR" and has_pb_credit_error:
                    await cur.execute("""
                        UPDATE helix_infrastructure_oracle_ledger
                        SET is_circuit_broken = TRUE,
                            credits_remaining = 0,
                            last_updated_at = NOW()
                        WHERE vendor_name = 'PhantomBusters';
                        """)
                    print(
                        "🚨 [ORACLE CIRCUIT BREAKER] PhantomBusters isolated due to credit exhaustion."
                    )

                    if industry_name:
                        await cur.execute(
                            "UPDATE helix_brain_scheduler SET is_active = FALSE, priority_score = GREATEST(priority_score - 2, 1) WHERE industry_name = %s;",
                            (industry_name,),
                        )

                # --- MYEMAILVERIFIER CREDIT INTERCEPTION ---
                has_mev_credit_error = any(
                    token in normalized_message
                    for token in (
                        "myemailverifier",
                        "insufficient credits",
                        "out of tokens",
                        "token balance low",
                    )
                )

                if log_level == "ERROR" and has_mev_credit_error:
                    await cur.execute("""
                        UPDATE helix_infrastructure_oracle_ledger
                        SET is_circuit_broken = TRUE, credits_remaining = 0, last_updated_at = NOW()
                        WHERE vendor_name = 'MyEmailVerifier';
                        """)
                    print(
                        "🚨 [ORACLE CIRCUIT BREAKER] MyEmailVerifier isolated! Outbound email gates closed."
                    )

                # --- PRE-EXISTING VANGUARD ORCHESTRATION ---
                if agent_name == "vanguard" and metadata_dict.get("event") in {
                    "campaign_drop",
                    "link_click",
                }:
                    if industry_name:
                        await cur.execute(
                            "UPDATE helix_brain_scheduler SET priority_score = priority_score + 1 WHERE industry_name = %s AND is_active = TRUE;",
                            (industry_name,),
                        )

                has_infra_block = any(
                    token in normalized_message
                    for token in ("instantly", "billing", "infrastructure", "blocked")
                )
                if log_level == "ERROR" and has_infra_block:
                    if industry_name:
                        await cur.execute(
                            "UPDATE helix_brain_scheduler SET priority_score = GREATEST(priority_score - 1, 1), is_active = FALSE WHERE industry_name = %s;",
                            (industry_name,),
                        )
            await conn.commit()
    except Exception as exc:
        print(f"[orchestrator] background log analysis failed: {exc}")


@app.post("/api/v1/logs")
async def ingest_log(payload: LogIngestSchema):
    try:
        async with app.state.db_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO agent_system_logs (agent_name, log_level, message, metadata)
                    VALUES (%s, %s, %s, %s)
                    RETURNING id;
                    """,
                    (
                        payload.agent_name,
                        payload.log_level,
                        payload.message,
                        (
                            Jsonb(payload.metadata)
                            if payload.metadata is not None
                            else None
                        ),
                    ),
                )
                row = await cur.fetchone()
            await conn.commit()

        # Run background analysis for errors or campaign updates
        asyncio.create_task(
            analyze_and_orchestrate_log(
                payload.agent_name,
                payload.log_level,
                payload.message,
                payload.metadata,
            )
        )

        # --- NEW: Trigger the automated pre-flight intent tracker ---
        asyncio.create_task(evaluate_ecosystem_demand_and_signal_nexus())

        return {"status": "success", "logged_id": row[0]}
    except Exception as exc:
        print(f"\n❌ [CRITICAL ENDPOINT FAILURE] /api/v1/logs crashed!")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Failed to log event: {exc}")


@app.get("/api/v1/logs/latest")
async def get_latest_agent_log(
    agent: Literal[
        "vanguard",
        "beacon",
        "echo",
        "shadow",
        "aura",
        "prism",
        "vector",
        "nexus",
        "helix",
    ],
    x_inbound_key: str | None = Header(default=None),
    authorization: str | None = Header(default=None),
):
    expected_key = os.getenv("INBOUND_API_KEY")
    inbound_key_valid = bool(
        expected_key
        and x_inbound_key
        and hmac.compare_digest(x_inbound_key, expected_key)
    )
    if not inbound_key_valid:
        raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        async with app.state.db_pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    """
                    SELECT id, agent_name, log_level, message, metadata, timestamp
                    FROM agent_system_logs
                    WHERE agent_name = %s
                    ORDER BY id DESC
                    LIMIT 1;
                    """,
                    (agent,),
                )
                row = await cur.fetchone()

        if row is None:
            return {
                "status": "empty",
                "agent_name": agent,
                "records_found": 0,
                "message": "No historical logs available for this agent.",
            }

        records_found = 0
        raw_message = str(row.get("message", ""))

        if isinstance(row.get("metadata"), dict) and "records_found" in row["metadata"]:
            records_found = int(row["metadata"]["records_found"])
        else:
            match_str = re.search(
                r"(?:processed rows:|processed)\s*(\d+)", raw_message, re.IGNORECASE
            )
            if match_str:
                records_found = int(match_str.group(1))

        return {
            "id": row["id"],
            "agent_name": row["agent_name"],
            "log_level": row["log_level"],
            "message": raw_message,
            "records_found": records_found,
            "timestamp": row["timestamp"].isoformat(),
        }
    except Exception as exc:
        raise HTTPException(
            status_code=500, detail=f"Failed to fetch latest agent metrics: {exc}"
        )


@app.post("/api/v1/ingest")
async def ingest_leads(
    leads: List[LeadIngestModel],
    x_ingest_key: str | None = Header(default=None),
):
    expected_key = os.getenv("INGEST_API_KEY")
    if expected_key and not (
        x_ingest_key and hmac.compare_digest(x_ingest_key, expected_key)
    ):
        raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        async with app.state.db_pool.connection() as conn:
            async with conn.cursor() as cur:
                for lead in leads:
                    await cur.execute(
                        """
                        INSERT INTO cold_leads
                        (email, first_name, last_name, company, title, website, industry)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (email) DO NOTHING;
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
            await conn.commit()
        return {"status": "success", "processed": len(leads)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to ingest leads: {exc}")


@app.post("/api/v1/inbound-blueprints")
async def inbound_blueprints(
    payload: BlueprintInboundModel,
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
                    INSERT INTO inbound_blueprints (
                        headcount, weekly_hours_wasted, annual_wasted_hours, annual_financial_bleed, friction_points, name, company_name, email
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
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
                    ),
                )
            await conn.commit()
        return {"status": "success"}
    except Exception as exc:
        raise HTTPException(
            status_code=500, detail=f"Failed to ingest inbound blueprint: {exc}"
        )


@app.post("/api/v1/scheduler/rotate")
async def rotate_scheduler_target(
    payload: SchedulerTargetUpdateModel,
    x_inbound_key: str | None = Header(default=None),
):
    expected_key = os.getenv("INBOUND_API_KEY")
    if expected_key and not (
        x_inbound_key and hmac.compare_digest(x_inbound_key, expected_key)
    ):
        raise HTTPException(status_code=401, detail="Unauthorized")

    # 🟩 ON INGESTION: Run LLM category mapping prior to updating database
    resolved_category = "Professional and Business Services"
    if app.state.ai_client:
        resolved_category = run_deterministic_moltset_mapping(
            app.state.ai_client, payload.industry_name
        )

    try:
        async with app.state.db_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    UPDATE helix_brain_scheduler
                    SET target_region = COALESCE(NULLIF(%s, ''), target_region),
                        moltset_category = %s,
                        last_targeted_at = NOW()
                    WHERE industry_name = %s
                    RETURNING industry_id, industry_name, target_region, last_targeted_at, moltset_category;
                    """,
                    (payload.target_region, resolved_category, payload.industry_name),
                )
                updated = await cur.fetchone()
                if updated is not None:
                    await log_helix_state(
                        cur=cur,
                        strategic_mode=f"Target Cycle: {updated[1]}",
                        geo_lock=updated[2],
                        buying_signal=f"#{''.join(updated[1].split()).lower()}life",
                    )
            await conn.commit()

        if updated is None:
            raise HTTPException(
                status_code=404, detail=f"Industry '{payload.industry_name}' not found"
            )

        return {
            "status": "success",
            "industry_id": updated[0],
            "industry_name": updated[1],
            "target_region": updated[2],
            "moltset_category": updated[4],
            "last_targeted_at": (
                updated[3].isoformat() if updated[3] is not None else None
            ),
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=500, detail=f"Failed to rotate scheduler target: {exc}"
        )


@app.get("/api/v1/scheduler/current")
async def get_current_scheduler_target(
    x_inbound_key: str | None = Header(default=None),
):
    expected_key = os.getenv("INBOUND_API_KEY")
    if expected_key and not (
        x_inbound_key and hmac.compare_digest(x_inbound_key, expected_key)
    ):
        raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        async with app.state.db_pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute("""
                    SELECT industry_id, industry_name, moltset_category, bottleneck_profile, pain_tokens, target_region, last_targeted_at, priority_score
                    FROM helix_brain_scheduler
                    WHERE is_active = TRUE
                    ORDER BY last_targeted_at ASC NULLS FIRST, priority_score DESC
                    LIMIT 1;
                    """)
                row = await cur.fetchone()

        if row is None:
            raise HTTPException(
                status_code=404, detail="No active scheduler target found"
            )

        return {
            **row,
            "last_targeted_at": (
                row["last_targeted_at"].isoformat()
                if row.get("last_targeted_at")
                else None
            ),
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=500, detail=f"Failed to fetch current scheduler target: {exc}"
        )


@app.post("/api/v1/scheduler/expand-horizons")
async def expand_target_industries(
    x_inbound_key: str | None = Header(default=None),
):
    expected_key = os.getenv("INBOUND_API_KEY")
    if expected_key and not (
        x_inbound_key and hmac.compare_digest(x_inbound_key, expected_key)
    ):
        raise HTTPException(status_code=401, detail="Unauthorized")

    async with app.state.db_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT industry_name FROM helix_brain_scheduler;")
            existing_rows = await cur.fetchall()
            existing_industries = [row[0] for row in existing_rows]

    anthropic_key = os.getenv("ANTHROPIC_API_KEY", "")
    anthropic_model = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-5")

    if not anthropic_key or not app.state.ai_client:
        raise HTTPException(
            status_code=500, detail="Missing Anthropic Key for industry discovery"
        )

    prompt = f"""You are a B2B market expansion strategist specializing in AI Automation Agents.
    Current targets already in database: {existing_industries}

    Task: Identify exactly 3 new, highly-specific industry verticals or niches where AI agents can heavily solve an operational bottleneck.
    
    Rules:
    - Do not repeat or overlap with the current targets.
    - Provide deep, tangible, pain-focused profiles.
    - Return ONLY a valid JSON array of objects, with no markdown fences, matching this structure:
    [
      {{
        "industry_name": "Industry Niche",
        "bottleneck_profile": "Clear sentence describing the manual, high-friction problem an AI voice/chat agent solves.",
        "pain_tokens": ["token1", "token2"],
        "target_region": "US State matching where this industry density is high"
      }}
    ]"""

    try:
        message = app.state.ai_client.messages.create(
            model=anthropic_model,
            max_tokens=1000,
            temperature=0.7,
            messages=[{"role": "user", "content": prompt}],
        )

        raw_json = message.content[0].text.strip()
        if raw_json.startswith("```"):
            raw_json = "\n".join(raw_json.splitlines()[1:-1]).strip()

        new_sectors = json.loads(raw_json)

        async with app.state.db_pool.connection() as conn:
            async with conn.cursor() as cur:
                for sector in new_sectors:
                    # 🟩 AUTOMATED ITERATION: Run mapping logic to ensure newly minted categories match schemas perfectly
                    resolved_cat = run_deterministic_moltset_mapping(
                        app.state.ai_client, sector["industry_name"]
                    )

                    await cur.execute(
                        """
                        INSERT INTO helix_brain_scheduler (
                            industry_name, moltset_category, bottleneck_profile, pain_tokens, target_region, priority_score
                        ) VALUES (%s, %s, %s, %s::text[], %s, 1)
                        ON CONFLICT (industry_name) DO NOTHING;
                        """,
                        (
                            sector["industry_name"],
                            resolved_cat,
                            sector["bottleneck_profile"],
                            sector["pain_tokens"],
                            sector.get("target_region", "Texas"),
                        ),
                    )

                if new_sectors:
                    await log_helix_state(
                        cur=cur,
                        strategic_mode=f"Expanding: {new_sectors[0]['industry_name']}",
                        geo_lock=new_sectors[0].get("target_region", "Texas"),
                        buying_signal=f"#{new_sectors[0]['pain_tokens'][0] if new_sectors[0]['pain_tokens'] else 'automation'}",
                    )
            await conn.commit()
        return {
            "status": "success",
            "discovered_and_added": [s["industry_name"] for s in new_sectors],
        }
    except Exception as e:
        print(
            f"\n❌ [CRITICAL ENDPOINT FAILURE] /api/v1/scheduler/expand-horizons crashed!"
        )
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Discovery failed: {e}")


@app.post("/api/v1/scheduler/add")
async def add_scheduler_record(
    payload: SchedulerRecordIngestModel,
    x_inbound_key: str | None = Header(default=None),
):
    expected_key = os.getenv("INBOUND_API_KEY")
    if expected_key and not (
        x_inbound_key and hmac.compare_digest(x_inbound_key, expected_key)
    ):
        raise HTTPException(status_code=401, detail="Unauthorized")

    resolved_category = "Professional and Business Services"
    if app.state.ai_client:
        resolved_category = run_deterministic_moltset_mapping(
            app.state.ai_client, payload.industry_name
        )

    try:
        async with app.state.db_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO helix_brain_scheduler (
                        industry_name, moltset_category, bottleneck_profile, pain_tokens, target_region, priority_score
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (industry_name) DO UPDATE
                    SET bottleneck_profile = EXCLUDED.bottleneck_profile,
                        pain_tokens = EXCLUDED.pain_tokens,
                        moltset_category = EXCLUDED.moltset_category,
                        target_region = EXCLUDED.target_region;
                    """,
                    (
                        payload.industry_name,
                        resolved_category,
                        payload.bottleneck_profile,
                        payload.pain_tokens,
                        payload.target_region,
                        payload.priority_score,
                    ),
                )
            await conn.commit()
        return {"status": "success", "industry_name": payload.industry_name}
    except Exception as exc:
        raise HTTPException(
            status_code=500, detail=f"Failed to add scheduler record: {exc}"
        )


@app.get("/api/v1/stream")
async def stream_logs(
    request: Request,
    agent_name: Literal["vanguard", "beacon"] | None = Query(default=None),
):
    async def log_publisher(agent_name: Optional[str] = None):
        last_seen_id = 0
        try:
            while True:
                if await request.is_disconnected():
                    break

                async with app.state.db_pool.connection() as conn:
                    async with conn.cursor(row_factory=dict_row) as cur:
                        if agent_name is not None:
                            await cur.execute(
                                """
                                SELECT id, agent_name, log_level, message, timestamp
                                FROM agent_system_logs
                                WHERE id > %s AND agent_name = %s
                                ORDER BY id ASC;
                                """,
                                (last_seen_id, agent_name),
                            )
                        else:
                            await cur.execute(
                                """
                                SELECT id, agent_name, log_level, message, timestamp
                                FROM agent_system_logs
                                WHERE id > %s
                                ORDER BY id ASC;
                                """,
                                (last_seen_id,),
                            )
                        rows = await cur.fetchall()

                if rows:
                    last_seen_id = max(row["id"] for row in rows)
                    for row in rows:
                        payload = {
                            "id": row["id"],
                            "agent_name": row["agent_name"],
                            "log_level": row["log_level"],
                            "message": row["message"],
                            "timestamp": row["timestamp"].isoformat(),
                        }
                        yield f"data: {json.dumps(payload)}\n\n"
                else:
                    await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            return

    return StreamingResponse(
        log_publisher(agent_name=agent_name),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/api/v1/leads/unprocessed")
async def get_unprocessed_leads(
    x_inbound_key: str | None = Header(default=None),
):
    expected_key = os.getenv("INBOUND_API_KEY")
    if expected_key and not (
        x_inbound_key and hmac.compare_digest(x_inbound_key, expected_key)
    ):
        raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        async with app.state.db_pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute("""
                    SELECT id, email, first_name, last_name, company, title, website, industry, status, created_at
                    FROM cold_leads
                    WHERE status = 'social_hook_captured' OR status = 'scraped'
                    ORDER BY created_at ASC
                    LIMIT 50;
                    """)
                rows = await cur.fetchall()
        return [
            {
                **row,
                "created_at": (
                    row["created_at"].isoformat() if row.get("created_at") else None
                ),
            }
            for row in rows
        ]
    except Exception as exc:
        raise HTTPException(
            status_code=500, detail=f"Failed to fetch unprocessed leads: {exc}"
        )


@app.patch("/api/v1/leads/{lead_id}")
async def update_lead_status(
    lead_id: int,
    payload: LeadStatusUpdateModel,
    x_inbound_key: str | None = Header(default=None),
    authorization: str | None = Header(default=None),
):
    expected_key = os.getenv("INBOUND_API_KEY")
    helix_api_token = os.getenv("HELIX_API_TOKEN")

    inbound_key_valid = bool(
        expected_key
        and x_inbound_key
        and hmac.compare_digest(x_inbound_key, expected_key)
    )
    bearer_token_valid = False
    if helix_api_token and authorization:
        scheme, _, token = authorization.partition(" ")
        if scheme.lower() == "bearer" and token:
            bearer_token_valid = hmac.compare_digest(token, helix_api_token)

    if not (inbound_key_valid or bearer_token_valid):
        raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        async with app.state.db_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    UPDATE cold_leads
                    SET status = %s,
                        email_body = COALESCE(%s, email_body)
                    WHERE id = %s
                    RETURNING id;
                    """,
                    (payload.status, payload.email_body, lead_id),
                )
                updated = await cur.fetchone()
            await conn.commit()

        if updated is None:
            raise HTTPException(status_code=404, detail=f"Lead {lead_id} not found")
        return {"status": "success"}
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=500, detail=f"Failed to update lead status: {exc}"
        )


if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
