# main.py
import os
import hmac
import datetime
import asyncio
import psycopg
from typing import List, Optional, Literal
from contextlib import asynccontextmanager, suppress

from config import settings
from config.settings import MOLTSET_ALLOWED_CATEGORIES
from database.connection import create_async_db_pool
from helix_optimizer import reactive_optimizer_loop, run_daily_prompt_optimization
from dotenv import load_dotenv
from fastapi import (
    FastAPI,
    HTTPException,
    Query,
    Request,
    Response,
    Header,
    BackgroundTasks,
)
from pydantic import BaseModel
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from anthropic import Anthropic
from telegram_orchestrator import router as telegram_router

# Submodule Imports
from services.telemetry import (
    send_telegram_broadcast,
    analyze_and_orchestrate_log,
    evaluate_lead_density_and_escalate,
    process_roi_attribution_boost,
    get_new_york_now,
    log_helix_state,
)
from services.mesh_client import trigger_downstream_mesh_with_payload
from services.credit_sync import (
    sync_external_vendor_credits,
    credit_sync_scheduler_loop,
)
from core.engine import perpetual_awareness_loop, load_vendor_isolation_matrix

load_dotenv()

app = FastAPI(title="Helix Strategic Orchestration Core API")
app.include_router(telegram_router)


# Modify the lifespan context manager
@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db_pool = create_async_db_pool()
    anthropic_key = os.getenv("ANTHROPIC_API_KEY", "")
    app.state.ai_client = Anthropic(api_key=anthropic_key) if anthropic_key else None
    app.state.phantombuster_remediation = {}
    app.state.last_optimization_date = None

    await app.state.db_pool.open(wait=True)
    await sync_external_vendor_credits(app.state.db_pool)

    # Background Tasks
    app.state.awareness_task = asyncio.create_task(perpetual_awareness_loop(app))
    app.state.credit_sync_task = asyncio.create_task(
        credit_sync_scheduler_loop(app.state.db_pool)
    )
    # New: Add the intelligent prompt tuning loop directly into the app state
    app.state.optimizer_task = asyncio.create_task(
        reactive_optimizer_loop(app.state.db_pool)
    )

    send_telegram_broadcast(
        "🚀 *Helix Orchestrator Booted successfully with Autonomous Self-Healing Optimizer!*",
        "success",
    )
    yield

    # Clean teardown
    app.state.awareness_task.cancel()
    app.state.credit_sync_task.cancel()
    app.state.optimizer_task.cancel()
    with suppress(asyncio.CancelledError):
        await app.state.awareness_task
    with suppress(asyncio.CancelledError):
        await app.state.credit_sync_task
    with suppress(asyncio.CancelledError):
        await app.state.optimizer_task
    await app.state.db_pool.close()


app.router.lifespan_context = lifespan


# --- API MODELS ---
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
        "pulsar",
        "radar",
        "nova",
        "chiron",
    ]
    log_level: str = "INFO"
    message: str
    metadata: Optional[dict] = None


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


# --- API ENDPOINTS ---
@app.get("/", status_code=200)
@app.head("/", status_code=200)
async def root_heartbeat(response: Response):
    """
    Catches automatic platform uptime pings (GET/HEAD)
    to keep logs clean and confirm the container is alive.
    """
    # Using a minimal payload or empty body keeps responses fast and lightweight
    return {"status": "healthy", "service": "polaris-helix"}


@app.get("/api/v1/health")
async def health_check():
    return {"status": "healthy", "timestamp": get_new_york_now().isoformat()}


@app.post("/api/v1/ingest")
async def ingest_leads(
    leads: List[LeadIngestModel],
    background_tasks: BackgroundTasks,
    x_helix_auth_token: str | None = Header(default=None),
):
    expected = os.getenv("HELIX_AUTH_TOKEN")
    if expected and not (
        x_helix_auth_token and hmac.compare_digest(x_helix_auth_token, expected)
    ):
        raise HTTPException(status_code=401, detail="Unauthorized")
    try:
        industry_counts = {}
        async with app.state.db_pool.connection() as conn:
            async with conn.cursor() as cur:
                for lead in leads:
                    await cur.execute(
                        "INSERT INTO cold_leads (email, first_name, last_name, company, title, website, industry) VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (email) DO NOTHING;",
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
                industry, count, app.state.db_pool, background_tasks, app.state
            )
        return {"status": "success", "processed": len(leads)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/api/v1/inbound-blueprints")
async def inbound_blueprints(
    payload: BlueprintInboundModel,
    background_tasks: BackgroundTasks,
    x_helix_auth_token: str | None = Header(default=None),
):
    expected = os.getenv("HELIX_AUTH_TOKEN")
    if expected and not (
        x_helix_auth_token and hmac.compare_digest(x_helix_auth_token, expected)
    ):
        raise HTTPException(status_code=401, detail="Unauthorized")
    try:
        async with app.state.db_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO helix_inbound_blueprints (headcount, weekly_hours_wasted, annual_wasted_hours, annual_financial_bleed, friction_points, name, company_name, email, status, industry_name)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
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
                payload.industry_name,
                10,
                app.state.db_pool,
                background_tasks,
                app.state,
            )
        return {"status": "success"}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/api/v1/leads/status-update")
async def update_lead_status(
    payload: LeadStatusUpdateModel,
    email: str = Query(...),
    x_vanguard_key: str | None = Header(default=None),
):
    expected = os.getenv("VANGUARD_API_KEY") or os.getenv("HELIX_AUTH_TOKEN")
    if expected and not (
        x_vanguard_key and hmac.compare_digest(x_vanguard_key, expected)
    ):
        raise HTTPException(status_code=401, detail="Unauthorized Tracking Call")
    try:
        async with app.state.db_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "UPDATE cold_leads SET status = %s, updated_at = NOW() WHERE email = %s;",
                    (payload.status, email),
                )
            await conn.commit()
        asyncio.create_task(
            process_roi_attribution_boost(email, payload.status, app.state.db_pool)
        )
        return {"status": "attribution_processed"}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


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

        if not row:
            raise Exception("No ID returned from log insertion.")

        asyncio.create_task(
            analyze_and_orchestrate_log(
                app.state.db_pool,
                app.state,
                row[0],
                payload.agent_name,
                payload.log_level,
                payload.message,
                payload.metadata,
            )
        )
        return {"status": "success", "logged_id": row[0]}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/v1/logs/latest")
async def get_latest_log(agent: str, x_inbound_key: str | None = Header(default=None)):
    expected = os.getenv("HELIX_AUTH_TOKEN")
    if expected and not (
        x_inbound_key and hmac.compare_digest(x_inbound_key, expected)
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
        return {"status": "empty", "message": "No logs found."}
    return {**row, "timestamp": row["timestamp"].isoformat()}


@app.get("/api/v1/scheduler/current")
async def get_current_scheduler_target(
    x_inbound_key: str | None = Header(default=None),
):
    expected = os.getenv("HELIX_AUTH_TOKEN")
    if expected and not (
        x_inbound_key and hmac.compare_digest(x_inbound_key, expected)
    ):
        raise HTTPException(status_code=401, detail="Unauthorized")
    async with app.state.db_pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("""
                SELECT industry_id, industry_name, moltset_category, bottleneck_profile, pain_tokens, target_region, last_targeted_at, priority_score
                FROM helix_brain_scheduler WHERE is_active = TRUE AND (last_targeted_at IS NULL OR last_targeted_at <= NOW() - (minimum_cooldown_hours || ' hours')::INTERVAL)
                ORDER BY priority_score DESC, last_targeted_at ASC NULLS FIRST LIMIT 1;
            """)
            row = await cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="No active targets.")
    return {
        **row,
        "last_targeted_at": (
            row["last_targeted_at"].isoformat() if row.get("last_targeted_at") else None
        ),
    }


@app.post("/api/v1/scheduler/rotate")
async def rotate_scheduler_target(
    payload: SchedulerTargetUpdateModel,
    x_inbound_key: str | None = Header(default=None),
):
    expected = os.getenv("HELIX_AUTH_TOKEN")
    if expected and not (
        x_inbound_key and hmac.compare_digest(x_inbound_key, expected)
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
                    UPDATE helix_brain_scheduler SET target_region = COALESCE(NULLIF(%s, ''), target_region), moltset_category = %s, last_targeted_at = NOW()
                    WHERE industry_name = %s RETURNING industry_id, industry_name, target_region, last_targeted_at, moltset_category;
                    """,
                    (payload.target_region, resolved_category, payload.industry_name),
                )
                updated = await cur.fetchone()
                if updated is not None:
                    await log_helix_state(
                        cur,
                        f"Target Cycle: {updated[1]}",
                        updated[2],
                        f"#{''.join(updated[1].split()).lower()}life",
                    )
            await conn.commit()

        if updated is None:
            raise HTTPException(status_code=404, detail="Niche target not found.")
        return {
            "status": "success",
            "industry_id": updated[0],
            "industry_name": updated[1],
            "target_region": updated[2],
            "moltset_category": updated[4],
            "last_targeted_at": updated[3].isoformat() if updated[3] else None,
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/v1/oracle/vendor-health")
async def get_vendor_health(x_inbound_key: str | None = Header(default=None)):
    expected = os.getenv("HELIX_AUTH_TOKEN")
    if expected and not (
        x_inbound_key and hmac.compare_digest(x_inbound_key, expected)
    ):
        raise HTTPException(status_code=401, detail="Unauthorized")
    try:
        async with app.state.db_pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
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
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/api/v1/orchestrator/optimize-now")
async def force_prompt_optimization(
    x_helix_auth_token: Optional[str] = Header(default=None),
):
    """
    Manual bypass endpoint to instantly force Claude 3.5 to analyze
    logs and re-calibrate agent prompts, ignoring the anomaly threshold.
    """
    expected = os.getenv("HELIX_AUTH_TOKEN")
    if expected and not (
        x_helix_auth_token and hmac.compare_digest(x_helix_auth_token, expected)
    ):
        raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        # Run optimization immediately, setting force=True to skip log counts
        await run_daily_prompt_optimization(app.state.db_pool, force=True)

        send_telegram_broadcast(
            "🧠 *Manual Optimization Override Complete:* All agent prompts evaluated and re-tuned.",
            "orchestration",
        )
        return {"status": "optimization_cycle_completed", "forced": True}
    except Exception as exc:
        raise HTTPException(
            status_code=500, detail=f"Forced prompt optimization failure: {str(exc)}"
        )


@app.post("/api/v1/orchestrator/run-cycle")
async def execute_orchestrated_master_cycle(
    background_tasks: BackgroundTasks,
    x_helix_auth_token: Optional[str] = Header(default=None),
):
    expected = os.getenv("HELIX_AUTH_TOKEN")
    if expected and not (
        x_helix_auth_token and hmac.compare_digest(x_helix_auth_token, expected)
    ):
        raise HTTPException(status_code=401, detail="Unauthorized")

    async with app.state.db_pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("""
                SELECT industry_id, industry_name, moltset_category, bottleneck_profile, pain_tokens, target_region
                FROM helix_brain_scheduler WHERE is_active = TRUE AND (last_targeted_at IS NULL OR last_targeted_at <= NOW() - (minimum_cooldown_hours || ' hours')::INTERVAL)
                ORDER BY priority_score DESC, last_targeted_at ASC NULLS FIRST LIMIT 1;
            """)
            strategy = await cur.fetchone()
            if strategy:
                await cur.execute(
                    "UPDATE helix_brain_scheduler SET last_targeted_at = NOW() WHERE industry_id = %s;",
                    (strategy["industry_id"],),
                )
        await conn.commit()

    if not strategy:
        return {"status": "idle", "message": "No active targets configured."}

    vendor_isolation_matrix = await load_vendor_isolation_matrix(app.state.db_pool)
    send_telegram_broadcast(
        f"Manual cycle override for `{strategy['industry_name']}` triggered.",
        "orchestration",
    )

    background_tasks.add_task(
        trigger_downstream_mesh_with_payload,
        app.state.db_pool,
        app.state,
        strategy,
        ["nexus", "beacon", "nova", "vanguard", "pulsar"],
        vendor_isolation_matrix,
    )
    return {
        "status": "orchestration_cycle_pushed",
        "active_strategy": strategy["industry_name"],
    }


@app.get("/api/v1/orchestrator/dashboard-diagnostics")
async def get_dashboard_diagnostics(
    x_helix_auth_token: Optional[str] = Header(default=None),
):
    expected = os.getenv("HELIX_AUTH_TOKEN")
    if expected and not (
        x_helix_auth_token and hmac.compare_digest(x_helix_auth_token, expected)
    ):
        raise HTTPException(status_code=401, detail="Unauthorized Access.")

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
                    FROM agent_system_logs WHERE timestamp >= (NOW() AT TIME ZONE 'America/New_York')::date AND log_level = 'INFO'
                    GROUP BY agent_name;
                """)
                achievements = await cur.fetchall()
                for row in achievements:
                    diagnostics["todays_achievements"][row["agent_name"]] = {
                        "status": "COMPLETE",
                        "actions_logged": row["action_count"],
                        "last_seen_milestone": row["last_milestone"],
                    }

                await cur.execute(
                    "SELECT vendor_name, credits_remaining, is_circuit_broken FROM helix_infrastructure_oracle_ledger;"
                )
                vendors = await cur.fetchall()
                for v in vendors:
                    status_label = (
                        "BLOCKED" if v["is_circuit_broken"] else "OPERATIONAL"
                    )
                    reason = "Healthy parameters"
                    action = "None required"

                    if v["vendor_name"] == "PhantomBusters" and v["is_circuit_broken"]:
                        reason = "API tokens exhausted or execution limit reached."
                        action = "Upgrade account to the $56 Starter Plan."
                    elif (
                        v["vendor_name"] == "MyEmailVerifier" and v["is_circuit_broken"]
                    ):
                        reason = "Validation credit ledger reporting depletion."
                        action = "Replenish verification balance."

                    diagnostics["infrastructure_logjams"][v["vendor_name"]] = {
                        "status": status_label,
                        "credits": float(v["credits_remaining"]),
                        "bottleneck_reason": reason,
                        "required_resolution": action,
                    }

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

                    diagnostics["pipeline_progress"] = {
                        "nova_reservoir": {
                            "status": "ACTIVE",
                            "leads_cached_raw": scraped_count,
                        },
                        "vector_enrichment": {
                            "status": "STALLED" if scraped_count > 0 else "IDLE",
                            "leads_enriched_ready": ready_count,
                        },
                    }
        return diagnostics
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Dashboard telemetries compilation failure: {str(e)}",
        )
