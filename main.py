import os
import traceback
import asyncio
import json
import hmac
from contextlib import asynccontextmanager
from typing import Any, Literal, Optional, List

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, Request, Header
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, ConfigDict
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool
import uvicorn

load_dotenv()


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


class LogIngestSchema(BaseModel):
    agent_name: Literal["vanguard", "beacon", "echo", "shadow", "aura"]
    log_level: str = "INFO"
    message: str
    metadata: dict[str, Any] | None = None


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


SCHEDULER_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS helix_brain_scheduler (
    industry_id SERIAL PRIMARY KEY,
    industry_name VARCHAR(100) UNIQUE NOT NULL,
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
INSERT INTO helix_brain_scheduler (industry_name, bottleneck_profile, pain_tokens, target_region)
VALUES
('Home Services', 'Inbound emergency calls rolling to voicemail.', ARRAY['hvac', 'plumbing'], 'Texas'),
('Accounting Firms', 'Manual client intake document tracking friction.', ARRAY['accounting', 'bookkeeping'], 'California'),
('Medical Clinics', 'Open chair utilization inefficiencies.', ARRAY['dental', 'medical spa'], 'Florida')
ON CONFLICT (industry_name) DO NOTHING;
"""


async def ensure_inbound_blueprints_table() -> None:
    async with app.state.db_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(INBOUND_BLUEPRINTS_TABLE_SQL)
        await conn.commit()


@asynccontextmanager
async def lifespan(app: FastAPI):
    database_url = get_validated_database_url()

    # 🎯 SELF-HEALING ASYNC CONNECTION POOL CONFIGURATION
    app.state.db_pool = AsyncConnectionPool(
        conninfo=database_url,
        open=False,
        # Forces the pool to test the socket before lending it to an API route
        check=AsyncConnectionPool.check_connection,
        # Automatically drops and recreates idle connections older than 4.5 minutes
        max_idle=270.0,
    )
    try:
        await app.state.db_pool.open(wait=True)
        await ensure_inbound_blueprints_table()
        async with app.state.db_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(SCHEDULER_TABLE_SQL)
                await cur.execute(SEED_SCHEDULER_SQL)
            await conn.commit()
    except Exception as exc:
        print(
            "\n[helix] CLOUD CONNECTION FAILED — possible causes: wrong credentials, "
            "DNS resolution failure, or network timeout. Full trace:\n"
        )
        traceback.print_exc()
        print(f"\n[helix] Error detail: {exc}\n")
        raise
    try:
        yield
    finally:
        await app.state.db_pool.close()


app = FastAPI(title="Helix Log Intake API", lifespan=lifespan)


from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    body = await request.body()
    print(f"\n[422 VALIDATION ERROR] Caught a bad payload!")
    print(f"Errors: {exc.errors()}")
    print(f"Raw Body Received: {body.decode('utf-8', errors='ignore')}\n")

    return JSONResponse(
        status_code=422,
        content={"detail": exc.errors(), "body": body.decode("utf-8", errors="ignore")},
    )


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
                if agent_name == "vanguard" and metadata_dict.get("event") in {
                    "campaign_drop",
                    "link_click",
                }:
                    if industry_name:
                        await cur.execute(
                            """
                            UPDATE helix_brain_scheduler
                            SET priority_score = priority_score + 1
                            WHERE industry_name = %s AND is_active = TRUE;
                            """,
                            (industry_name,),
                        )
                        print(
                            "[orchestrator] vanguard scale sync: "
                            f"raised priority for industry={industry_name}"
                        )
                    else:
                        print(
                            "[orchestrator] vanguard scale sync skipped: "
                            "missing industry_name in metadata"
                        )

                has_infra_block = any(
                    token in normalized_message
                    for token in ("instantly", "billing", "infrastructure", "blocked")
                )
                if log_level == "ERROR" and has_infra_block:
                    if industry_name:
                        await cur.execute(
                            """
                            UPDATE helix_brain_scheduler
                            SET priority_score = GREATEST(priority_score - 1, 1),
                                is_active = FALSE
                            WHERE industry_name = %s;
                            """,
                            (industry_name,),
                        )
                        print(
                            "[orchestrator] quarantine gate: "
                            f"paused track and reduced priority for industry={industry_name}"
                        )
                    else:
                        print(
                            "[orchestrator] quarantine gate skipped: "
                            "missing industry_name in metadata"
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

        asyncio.create_task(
            analyze_and_orchestrate_log(
                payload.agent_name,
                payload.log_level,
                payload.message,
                payload.metadata,
            )
        )

        return {"status": "success", "logged_id": row[0]}
    except Exception as exc:
        raise HTTPException(
            status_code=500, detail=f"Failed to log event: {exc}"
        ) from exc


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
        raise HTTPException(
            status_code=500, detail=f"Failed to ingest leads: {exc}"
        ) from exc


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

    print(
        "[ALERT][PRIORITY=HIGH] Inbound blueprint received "
        f"name={payload.name} company={payload.company_name} email={payload.email} "
        f"annual_financial_bleed={payload.annual_financial_bleed}"
    )

    try:
        async with app.state.db_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO inbound_blueprints (
                        headcount,
                        weekly_hours_wasted,
                        annual_wasted_hours,
                        annual_financial_bleed,
                        friction_points,
                        name,
                        company_name,
                        email
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
            status_code=500,
            detail=f"Failed to ingest inbound blueprint: {exc}",
        ) from exc


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

    try:
        async with app.state.db_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    UPDATE helix_brain_scheduler
                    SET target_region = %s,
                        last_targeted_at = NOW()
                    WHERE industry_name = %s
                    RETURNING industry_id, industry_name, target_region, last_targeted_at;
                    """,
                    (payload.target_region, payload.industry_name),
                )
                updated = await cur.fetchone()
            await conn.commit()

        if updated is None:
            raise HTTPException(
                status_code=404,
                detail=f"Industry '{payload.industry_name}' not found",
            )

        return {
            "status": "success",
            "industry_id": updated[0],
            "industry_name": updated[1],
            "target_region": updated[2],
            "last_targeted_at": (
                updated[3].isoformat() if updated[3] is not None else None
            ),
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to rotate scheduler target: {exc}",
        ) from exc


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
                    SELECT industry_id, industry_name, target_region, last_targeted_at, priority_score
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
            status_code=500,
            detail=f"Failed to fetch current scheduler target: {exc}",
        ) from exc


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

    try:
        async with app.state.db_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO helix_brain_scheduler (
                        industry_name,
                        bottleneck_profile,
                        pain_tokens,
                        target_region,
                        priority_score
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (industry_name) DO UPDATE
                    SET bottleneck_profile = EXCLUDED.bottleneck_profile,
                        pain_tokens = EXCLUDED.pain_tokens,
                        target_region = EXCLUDED.target_region;
                    """,
                    (
                        payload.industry_name,
                        payload.bottleneck_profile,
                        payload.pain_tokens,
                        payload.target_region,
                        payload.priority_score,
                    ),
                )
            await conn.commit()

        return {
            "status": "success",
            "industry_name": payload.industry_name,
        }
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to add scheduler record: {exc}",
        ) from exc


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
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
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
                    SELECT id, email, first_name, last_name, company, title,
                           website, industry, status, created_at
                    FROM cold_leads
                    WHERE status = 'scraped'
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
        ) from exc


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

        print(f"[STATUS UPDATE] Lead {lead_id} transitioned to {payload.status}")

        return {"status": "success"}
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=500, detail=f"Failed to update lead status: {exc}"
        ) from exc


if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
