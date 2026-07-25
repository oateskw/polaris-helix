# main.py
import os
import hmac
import asyncio
import psycopg
import json
import re
import traceback
import httpx
from typing import List, Optional, Literal
from contextlib import asynccontextmanager

from config.settings import MOLTSET_ALLOWED_CATEGORIES
from database.connection import create_async_db_pool
from helix_optimizer import reactive_optimizer_loop, run_daily_prompt_optimization
from dotenv import load_dotenv
from fastapi import (
    FastAPI,
    HTTPException,
    Query,
    Response,
    Header,
    BackgroundTasks,
)
from pydantic import BaseModel, model_validator
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from anthropic import Anthropic
from telegram_orchestrator import router as telegram_router

# Submodule Imports
from core.engine import perpetual_awareness_loop, load_vendor_isolation_matrix
from services.telemetry import (
    ExecutiveNotificationEvent,
    emit_executive_notification,
    analyze_and_orchestrate_log,
    evaluate_lead_density_and_escalate,
    process_roi_attribution_boost,
    get_new_york_now,
    log_helix_state,
    is_vendor_circuit_healthy,
    send_executive_summary_from_logs,
    write_helix_command_center_log,
)
from services.mesh_client import trigger_downstream_mesh_with_payload
from services.credit_sync import (
    sync_external_vendor_credits,
    credit_sync_scheduler_loop,
)
from core.auditor import (
    evaluate_agent_performance,
    dispatch_chiron_optimization,
    run_agent_performance_audit_loop,
)

# --- GLOBAL SCHEDULE ENFORCER CORE MUTATION ---
load_dotenv()

app = FastAPI(title="Helix Strategic Orchestration Core API")
app.include_router(telegram_router)


async def record_background_task_exit(task: asyncio.Task, db_pool) -> None:
    if task.cancelled():
        return
    exception = task.exception()
    if exception is not None:
        message = f"[WORKER FAILED] {task.get_name()}: {exception}"
        action = "HELIX_WORKER_FAILED"
        level = "CRITICAL"
        trace = "".join(
            traceback.format_exception(
                type(exception), exception, exception.__traceback__
            )
        )
    else:
        message = f"[WORKER STOPPED] {task.get_name()} exited unexpectedly."
        action = "HELIX_WORKER_STOPPED"
        level = "ERROR"
        trace = None

    print(message)
    await write_helix_command_center_log(
        db_pool,
        level,
        message,
        action,
        task.get_name(),
        {"traceback": trace},
    )
    emit_executive_notification(
        ExecutiveNotificationEvent(
            source_agent="helix",
            event_type="critical_failure",
            severity="critical",
            headline="Helix Background Worker Stopped",
            outcome=message,
            next_action="Inspect agent_system_logs and Render traceback output.",
        )
    )


def report_background_task_exit(task: asyncio.Task, db_pool) -> None:
    if not task.cancelled():
        asyncio.create_task(record_background_task_exit(task, db_pool))


async def executive_digest_loop(app: FastAPI) -> None:
    interval_seconds = int(os.getenv("HELIX_EXECUTIVE_DIGEST_SECONDS", "10800"))
    while True:
        await asyncio.sleep(interval_seconds)
        await send_executive_summary_from_logs(
            app.state.db_pool,
            lookback_minutes=max(1, interval_seconds // 60),
            ai_client=app.state.ai_client,
        )


async def ensure_observability_schema(db_pool) -> None:
    async with db_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_asl_event_id
                ON agent_system_logs ((metadata->>'event_id'))
                WHERE metadata->>'event_id' IS NOT NULL;

                DO $$
                BEGIN
                    IF to_regclass('helix_agent_manifests') IS NOT NULL THEN
                        ALTER TABLE helix_agent_manifests
                            ADD COLUMN IF NOT EXISTS optimization_status TEXT NOT NULL DEFAULT 'IDLE',
                            ADD COLUMN IF NOT EXISTS last_optimized_at TIMESTAMPTZ;

                        UPDATE helix_agent_manifests
                        SET optimization_status = 'IDLE'
                        WHERE optimization_status IS NULL;

                        EXECUTE 'CREATE INDEX IF NOT EXISTS idx_ham_chiron_eligibility '
                                'ON helix_agent_manifests '
                                '(is_active, optimization_status, last_optimized_at, LOWER(agent_name))';
                    END IF;
                END $$;
            """)
        await conn.commit()


async def observability_watchdog_loop(app: FastAPI) -> None:
    interval_seconds = int(os.getenv("HELIX_WATCHDOG_INTERVAL_SECONDS", "300"))
    stale_minutes = int(os.getenv("NOVA_STALE_RUN_MINUTES", "120"))
    nova_url = os.getenv("NOVA_API_URL", "https://polaris-nova.onrender.com").rstrip(
        "/"
    )
    while True:
        await asyncio.sleep(interval_seconds)
        await write_helix_command_center_log(
            app.state.db_pool,
            "INFO",
            "Helix observability heartbeat confirmed.",
            "HELIX_HEARTBEAT",
            "observability-watchdog",
        )

        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(nova_url)
                response.raise_for_status()
        except Exception as exc:
            await write_helix_command_center_log(
                app.state.db_pool,
                "CRITICAL",
                f"Nova health probe failed: {exc}",
                "NOVA_HEALTH_PROBE_FAILED",
                "nova",
            )
            emit_executive_notification(
                ExecutiveNotificationEvent(
                    source_agent="nova",
                    event_type="critical_failure",
                    severity="critical",
                    headline="Nova Health Probe Failed",
                    outcome="Helix could not reach Nova's health endpoint.",
                    impact=str(exc),
                    next_action="Inspect Nova Render service health and logs.",
                )
            )

        async with app.state.db_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT started.metadata->>'run_id'
                    FROM agent_system_logs AS started
                    WHERE started.agent_name = 'nova'
                      AND started.metadata->>'lifecycle_event' = 'RUN_STARTED'
                      AND started.timestamp < NOW() - (%s || ' minutes')::INTERVAL
                      AND NOT EXISTS (
                          SELECT 1 FROM agent_system_logs AS terminal
                          WHERE terminal.metadata->>'run_id' = started.metadata->>'run_id'
                            AND terminal.metadata->>'lifecycle_event' IN (
                                'RUN_COMPLETED', 'RUN_RATE_LIMITED', 'RUN_FAILED'
                            )
                      )
                      AND NOT EXISTS (
                          SELECT 1 FROM agent_system_logs AS alerted
                          WHERE alerted.metadata->>'run_id' = started.metadata->>'run_id'
                            AND alerted.metadata->'command_center_view'->>'action_taken' = 'NOVA_STALE_RUN_ALERT'
                      )
                    ORDER BY started.timestamp ASC
                    LIMIT 1;
                    """,
                    (stale_minutes,),
                )
                stale_row = await cur.fetchone()

        if stale_row:
            stale_run_id = stale_row[0]
            await write_helix_command_center_log(
                app.state.db_pool,
                "CRITICAL",
                f"Nova run {stale_run_id} has no terminal lifecycle event.",
                "NOVA_STALE_RUN_ALERT",
                "nova",
                {"run_id": stale_run_id},
            )
            emit_executive_notification(
                ExecutiveNotificationEvent(
                    source_agent="nova",
                    event_type="critical_failure",
                    severity="critical",
                    headline="Nova Run Stalled",
                    outcome=f"Run {stale_run_id} exceeded {stale_minutes} minutes without a terminal event.",
                    next_action="Inspect Nova server logs using the run ID.",
                )
            )


# Modify the lifespan context manager
@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db_pool = create_async_db_pool()
    anthropic_key = os.getenv("ANTHROPIC_API_KEY", "")
    app.state.ai_client = Anthropic(api_key=anthropic_key) if anthropic_key else None
    app.state.phantombuster_remediation = {}
    app.state.last_optimization_date = None

    await app.state.db_pool.open(wait=True)
    await ensure_observability_schema(app.state.db_pool)
    await sync_external_vendor_credits(app.state.db_pool)

    # Background Tasks - Ensure tasks respect operational constraints
    app.state.auditor_task = asyncio.create_task(
        run_agent_performance_audit_loop(app.state.db_pool), name="performance-auditor"
    )
    app.state.awareness_task = asyncio.create_task(
        perpetual_awareness_loop(app), name="awareness-dispatcher"
    )
    app.state.credit_sync_task = asyncio.create_task(
        credit_sync_scheduler_loop(app.state.db_pool), name="vendor-credit-sync"
    )
    app.state.optimizer_task = asyncio.create_task(
        reactive_optimizer_loop(app.state.db_pool), name="prompt-optimizer"
    )
    app.state.digest_task = asyncio.create_task(
        executive_digest_loop(app), name="executive-digest"
    )
    app.state.watchdog_task = asyncio.create_task(
        observability_watchdog_loop(app), name="observability-watchdog"
    )

    for task in (
        app.state.auditor_task,
        app.state.awareness_task,
        app.state.credit_sync_task,
        app.state.optimizer_task,
        app.state.digest_task,
        app.state.watchdog_task,
    ):
        task.add_done_callback(
            lambda completed_task, pool=app.state.db_pool: report_background_task_exit(
                completed_task, pool
            )
        )

    emit_executive_notification(
        ExecutiveNotificationEvent(
            source_agent="helix",
            event_type="pipeline_milestone",
            severity="success",
            headline="Helix Orchestrator Online",
            outcome="Core orchestrator, auditor, credit sync, and optimizer workers started.",
            next_action="Monitoring active agent pipeline in real time.",
        )
    )
    yield

    # Clean teardown with explicit cancellation order including auditor_task
    tasks_to_cancel = [
        app.state.auditor_task,
        app.state.awareness_task,
        app.state.credit_sync_task,
        app.state.optimizer_task,
        app.state.digest_task,
        app.state.watchdog_task,
    ]

    for task in tasks_to_cancel:
        if task and not task.done():
            task.cancel()

    await asyncio.gather(*tasks_to_cancel, return_exceptions=True)
    await app.state.db_pool.close()


app.router.lifespan_context = lifespan


# --- API MODELS ---
class LogMetadataSchema(BaseModel):
    event_id: Optional[str] = None
    run_id: Optional[str] = None
    lifecycle_event: Optional[str] = None
    industry_name: Optional[str] = "General Operations"
    target_region: Optional[str] = "Global"
    records_found: Optional[int] = 0
    status: Optional[str] = "COMPLETED"
    traceback: Optional[str] = None
    extra_context: Optional[dict] = None


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
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    message: str
    metadata: Optional[LogMetadataSchema] = None

    @model_validator(mode="after")
    def require_python_traceback_for_errors(self):
        if self.log_level != "ERROR":
            return self

        traceback_text = self.metadata.traceback if self.metadata else None
        if not traceback_text or not traceback_text.strip():
            raise ValueError("metadata.traceback is required for ERROR logs")
        if not re.search(r'File "[^"]+", line \d+', traceback_text):
            raise ValueError(
                "metadata.traceback must contain a parseable Python traceback frame"
            )
        return self


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


def run_deterministic_moltset_mapping(ai_client: Anthropic, industry_name: str) -> str:
    """Deterministically map an incoming industry name to allowed scheduler categories."""
    normalized = (industry_name or "").strip().lower()
    if not normalized:
        return "Professional and Business Services"

    for category in MOLTSET_ALLOWED_CATEGORIES:
        if normalized == category.lower():
            return category

    keyword_map = {
        "it": "Information Technology",
        "software": "Information Technology",
        "bank": "Finance and Banking",
        "finance": "Finance and Banking",
        "school": "Education",
        "health": "Health and Pharmaceuticals",
        "medical": "Health and Pharmaceuticals",
        "manufact": "Manufacturing",
        "retail": "Retail",
        "food": "Food and Beverage",
        "nonprofit": "Non-Profit and Social Services",
        "transport": "Transportation and Logistics",
        "construction": "Construction",
        "travel": "Tourism and Hospitality",
        "energy": "Energy",
        "marketing": "Marketing & Advertising",
        "telecom": "Telecommunications",
        "real estate": "Real Estate",
        "auto": "Automotive",
        "media": "Media and Publishing",
        "agri": "Agriculture",
    }

    for token, mapped in keyword_map.items():
        if token in normalized:
            return mapped

    return "Professional and Business Services"


# --- API ENDPOINTS ---
@app.get("/", status_code=200)
@app.head("/", status_code=200)
async def root_heartbeat(response: Response):
    """
    Catches automatic platform uptime pings (GET/HEAD)
    to keep logs clean and confirm the container is alive.
    """
    return {"status": "healthy", "service": "polaris-helix"}


@app.post("/api/v1/audit/evaluate-agent")
async def audit_agent_endpoint(
    agent_name: str = Query(...),
    x_helix_auth_token: Optional[str] = Header(default=None),
):
    """Manually forces a performance audit on an agent and triggers Chiron if underperforming."""
    expected = os.getenv("HELIX_AUTH_TOKEN")
    if expected and not (
        x_helix_auth_token and hmac.compare_digest(x_helix_auth_token, expected)
    ):
        raise HTTPException(status_code=401, detail="Unauthorized")

    audit = await evaluate_agent_performance(app.state.db_pool, agent_name)
    dispatched = False

    if audit.get("status") == "UNDERPERFORMING":
        dispatched = await dispatch_chiron_optimization(
            app.state.db_pool, agent_name, audit
        )

    return {
        "status": "completed",
        "audit_summary": audit,
        "chiron_dispatched": dispatched,
    }


@app.get("/api/v1/health")
async def health_check():
    workers = {}
    for worker_name, state_name in (
        ("performance_auditor", "auditor_task"),
        ("awareness_dispatcher", "awareness_task"),
        ("vendor_credit_sync", "credit_sync_task"),
        ("prompt_optimizer", "optimizer_task"),
    ):
        task = getattr(app.state, state_name, None)
        if task is None:
            workers[worker_name] = "not_started"
        elif task.done():
            workers[worker_name] = "stopped"
        else:
            workers[worker_name] = "running"

    overall_status = (
        "healthy"
        if all(status == "running" for status in workers.values())
        else "degraded"
    )
    return {
        "status": overall_status,
        "timestamp": get_new_york_now().isoformat(),
        "workers": workers,
    }


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
async def ingest_log(
    payload: LogIngestSchema,
    x_helix_auth_token: str | None = Header(default=None),
):
    expected = os.getenv("HELIX_AUTH_TOKEN")
    if not expected or not (
        x_helix_auth_token and hmac.compare_digest(x_helix_auth_token, expected)
    ):
        raise HTTPException(status_code=401, detail="Unauthorized")
    try:
        clean_metadata = None
        if payload.metadata:
            if hasattr(payload.metadata, "model_dump"):
                clean_metadata = payload.metadata.model_dump()
            elif hasattr(payload.metadata, "dict"):
                clean_metadata = payload.metadata.dict()
            elif isinstance(payload.metadata, dict):
                clean_metadata = payload.metadata
            else:
                try:
                    clean_metadata = json.loads(
                        json.dumps(payload.metadata, default=str)
                    )
                except Exception:
                    clean_metadata = {"raw_fallback": str(payload.metadata)}

        async with app.state.db_pool.connection() as conn:
            async with conn.cursor() as cur:
                event_id = clean_metadata.get("event_id") if clean_metadata else None
                if event_id:
                    await cur.execute(
                        "SELECT id FROM agent_system_logs WHERE metadata->>'event_id' = %s LIMIT 1;",
                        (event_id,),
                    )
                    existing_row = await cur.fetchone()
                    if existing_row:
                        return {"status": "duplicate", "logged_id": existing_row[0]}
                await cur.execute(
                    "INSERT INTO agent_system_logs (agent_name, log_level, message, metadata) VALUES (%s, %s, %s, %s) RETURNING id;",
                    (
                        payload.agent_name,
                        payload.log_level,
                        payload.message,
                        Jsonb(clean_metadata) if clean_metadata else None,
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
                clean_metadata,
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


@app.get("/api/v1/telemetry/errors-count")
async def get_agent_errors_count(
    agent: str, x_helix_auth_token: Optional[str] = Header(default=None)
):
    """Calculates active error/critical logs count for an agent within the last 24 hours."""
    expected = os.getenv("HELIX_AUTH_TOKEN")
    if expected and not (
        x_helix_auth_token and hmac.compare_digest(x_helix_auth_token, expected)
    ):
        raise HTTPException(status_code=401, detail="Unauthorized")

    async with app.state.db_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT COUNT(*) 
                FROM agent_system_logs 
                WHERE agent_name = %s 
                  AND log_level IN ('ERROR', 'CRITICAL')
                  AND timestamp >= NOW() - INTERVAL '24 hours';
                """,
                (agent.lower(),),
            )
            row = await cur.fetchone()
            count = row[0] if row else 0

    return {"agent": agent, "error_count": count}


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

    anthropic_healthy = await is_vendor_circuit_healthy(app.state.db_pool, "Anthropic")

    if app.state.ai_client and anthropic_healthy:
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
        await run_daily_prompt_optimization(app.state.db_pool, force=True)

        emit_executive_notification(
            ExecutiveNotificationEvent(
                source_agent="helix",
                event_type="manual_command_ack",
                severity="success",
                headline="Manual Optimization Completed",
                outcome="Prompt optimization override finished successfully.",
                next_action="Continue monitoring downstream agent quality signals.",
            )
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
                SELECT industry_id, industry_name, moltset_category, bottleneck_profile, pain_tokens, target_region, campaign_strategy
                FROM helix_brain_scheduler WHERE is_active = TRUE AND (last_targeted_at IS NULL OR last_targeted_at <= NOW() - (minimum_cooldown_hours || ' hours')::INTERVAL)
                ORDER BY priority_score DESC, last_targeted_at ASC NULLS FIRST LIMIT 1;
            """)
            strategy = await cur.fetchone()
            if strategy and "industry_id" in strategy:
                await cur.execute(
                    "UPDATE helix_brain_scheduler SET last_targeted_at = NOW() WHERE industry_id = %s;",
                    (strategy["industry_id"],),
                )
        await conn.commit()

    if strategy and ("industry_id" not in strategy or "industry_name" not in strategy):
        return {
            "status": "standdown",
            "message": "Standdown enforced for unassigned day: no runnable scheduler target.",
        }

    if not strategy:
        return {"status": "idle", "message": "No active targets configured."}

    vendor_isolation_matrix = await load_vendor_isolation_matrix(app.state.db_pool)
    emit_executive_notification(
        ExecutiveNotificationEvent(
            source_agent="helix",
            event_type="manual_command_ack",
            severity="info",
            headline="Manual Orchestration Cycle Triggered",
            outcome=f"Pipeline initiated for {strategy['industry_name']}.",
            impact=f"Region: {strategy.get('target_region') or 'Global'}",
            next_action="Track next completion milestone in the Helix dashboard.",
        )
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
