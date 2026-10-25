import asyncio
import datetime
import html
import os
import statistics
from typing import Any
from zoneinfo import ZoneInfo

from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from services.telemetry import (
    ExecutiveNotificationEvent,
    emit_executive_notification,
    write_helix_command_center_log,
)

EASTERN_TZ = ZoneInfo("America/New_York")
REVIEW_MARKER = "HELIX_STRATEGY_REVIEW_RUN"
REVIEW_WEEKDAY = 0  # Monday
REVIEW_HOUR_ET = 7
REVIEW_CHECK_INTERVAL_SECONDS = 30 * 60
REVIEW_WINDOW_DAYS = 28
PROPOSAL_EXPIRY_DAYS = 14

# Same weights as process_roi_attribution_boost, so the scorecard and the live
# priority bumps agree on what a reply, meeting and deal are worth.
POINTS_REPLY = 15
POINTS_MEETING = 40
POINTS_DEAL = 100

PRIORITY_BOOST = 10
PRIORITY_CUT_NEGATIVE = 10
PRIORITY_CUT_NO_RESULTS = 5
PAUSE_LEAD_MULTIPLIER = 3


def _int_env(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


def _float_env(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except ValueError:
        return default


def min_leads() -> int:
    return max(1, _int_env("HELIX_STRATEGY_MIN_LEADS", 30))


def max_negative_rate() -> float:
    return _float_env("HELIX_STRATEGY_MAX_NEGATIVE_RATE", 0.05)


def review_is_due(
    now_et: datetime.datetime, last_run_at: datetime.datetime | None
) -> bool:
    """True once per week, from Monday 07:00 ET, including after a long sleep."""
    days_since_monday = (now_et.weekday() - REVIEW_WEEKDAY) % 7
    week_start = (now_et - datetime.timedelta(days=days_since_monday)).replace(
        hour=REVIEW_HOUR_ET, minute=0, second=0, microsecond=0
    )
    if now_et < week_start:
        return False
    if last_run_at is None:
        return True
    return last_run_at < week_start


def current_week_start(now_et: datetime.datetime) -> datetime.datetime:
    """Monday 07:00 ET of the week containing now_et."""
    days_since_monday = (now_et.weekday() - REVIEW_WEEKDAY) % 7
    return (now_et - datetime.timedelta(days=days_since_monday)).replace(
        hour=REVIEW_HOUR_ET, minute=0, second=0, microsecond=0
    )


def next_check_delay(now_et: datetime.datetime, checked_this_week: bool) -> float:
    """Seconds to sleep before the loop needs to look at anything again.

    The loop touches the database only when a review could be due, so Neon can
    scale to zero the rest of the week: before Monday 07:00, sleep until it; once
    this week is settled, sleep until next Monday 07:00. Capped at 12 hours, and a
    failed check retries on the normal interval."""
    week_start = current_week_start(now_et)
    if now_et < week_start:
        target = week_start
    elif checked_this_week:
        target = week_start + datetime.timedelta(days=7)
    else:
        return float(REVIEW_CHECK_INTERVAL_SECONDS)
    return min(max(60.0, (target - now_et).total_seconds()), 12 * 3600.0)


async def get_last_review_at(db_pool) -> datetime.datetime | None:
    async with db_pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT MAX(timestamp) AS last_run_at
                FROM agent_system_logs
                WHERE agent_name = 'helix'
                  AND metadata->'command_center_view'->>'action_taken' = %s;
                """,
                (REVIEW_MARKER,),
            )
            row = await cur.fetchone()
    return row["last_run_at"] if row else None


async def compute_scorecard(
    db_pool, window_days: int = REVIEW_WINDOW_DAYS
) -> list[dict[str, Any]]:
    """Per-industry funnel summary. Rates use leads harvested as the denominator,
    because send counts live in other services."""
    days = max(1, int(window_days))
    async with db_pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT industry_name, stage, COUNT(*) AS n,
                       COALESCE(SUM(amount), 0) AS revenue
                FROM helix_outcome_events
                WHERE occurred_at >= NOW() - (%s || ' days')::INTERVAL
                  AND industry_name IS NOT NULL
                GROUP BY industry_name, stage;
                """,
                (days,),
            )
            event_rows = await cur.fetchall()
            await cur.execute(
                """
                SELECT industry, COUNT(*) AS n
                FROM cold_leads
                WHERE created_at >= NOW() - (%s || ' days')::INTERVAL
                  AND industry IS NOT NULL
                GROUP BY industry;
                """,
                (days,),
            )
            lead_rows = await cur.fetchall()
            await cur.execute(
                "SELECT industry_name, priority_score, is_active FROM helix_brain_scheduler;"
            )
            sched_rows = await cur.fetchall()

    rows: dict[str, dict[str, Any]] = {}
    for sched in sched_rows:
        rows[sched["industry_name"]] = {
            "industry_name": sched["industry_name"],
            "priority_score": sched["priority_score"],
            "is_active": bool(sched["is_active"]),
            "leads": 0,
            "replies": 0,
            "meetings": 0,
            "deals": 0,
            "revenue": 0.0,
            "bounces": 0,
            "opt_outs": 0,
            "blueprints": 0,
        }
    for lead in lead_rows:
        if lead["industry"] in rows:
            rows[lead["industry"]]["leads"] = int(lead["n"])
    stage_field = {
        "reply": "replies",
        "positive_reply": "replies",
        "meeting_booked": "meetings",
        "deal_won": "deals",
        "bounced": "bounces",
        "opt_out": "opt_outs",
        "blueprint_submitted": "blueprints",
    }
    for event in event_rows:
        row = rows.get(event["industry_name"])
        field = stage_field.get(event["stage"])
        if row is None or field is None:
            continue
        row[field] += int(event["n"])
        if event["stage"] == "deal_won":
            row["revenue"] += float(event["revenue"])

    for row in rows.values():
        row["points"] = (
            POINTS_REPLY * row["replies"]
            + POINTS_MEETING * row["meetings"]
            + POINTS_DEAL * row["deals"]
        )
        leads = row["leads"]
        row["yield_per_100"] = round(row["points"] / leads * 100, 1) if leads else 0.0
        row["negative_rate"] = (
            round((row["bounces"] + row["opt_outs"]) / leads, 4) if leads else 0.0
        )
    return sorted(rows.values(), key=lambda r: (-r["points"], r["industry_name"]))


def _subject(payload: dict[str, Any]) -> str:
    """What a proposal is about: an industry, or for Chiron briefs the target agent."""
    return payload.get("industry_name") or payload.get("target_agent") or ""


def _evidence(row: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "leads",
        "replies",
        "meetings",
        "deals",
        "revenue",
        "bounces",
        "opt_outs",
        "points",
        "yield_per_100",
        "negative_rate",
        "priority_score",
    )
    return {key: row[key] for key in keys}


def generate_proposals(
    rows: list[dict[str, Any]],
    *,
    min_lead_sample: int | None = None,
    negative_rate_limit: float | None = None,
) -> list[dict[str, Any]]:
    """Deterministic rules. At most one proposal per industry per run."""
    floor = min_lead_sample if min_lead_sample is not None else min_leads()
    neg_limit = (
        negative_rate_limit if negative_rate_limit is not None else max_negative_rate()
    )
    eligible = [r for r in rows if r["is_active"] and r["leads"] >= floor]
    median_yield = (
        statistics.median(r["yield_per_100"] for r in eligible)
        if len(eligible) >= 2
        else None
    )

    proposals: list[dict[str, Any]] = []
    for row in rows:
        name = row["industry_name"]
        evidence = _evidence(row)

        if not row["is_active"]:
            if row["points"] > 0:
                proposals.append(
                    {
                        "kind": "resume_industry",
                        "payload": {"industry_name": name},
                        "rationale": (
                            f"{name} is paused but produced {row['replies']} replies, "
                            f"{row['meetings']} meetings and {row['deals']} deals in the window."
                        ),
                        "evidence": evidence,
                    }
                )
            continue
        if row["leads"] < floor:
            continue

        if row["negative_rate"] > neg_limit:
            proposals.append(
                {
                    "kind": "adjust_priority",
                    "payload": {
                        "industry_name": name,
                        "delta": -PRIORITY_CUT_NEGATIVE,
                    },
                    "rationale": (
                        f"{name} had {row['bounces']} bounces and {row['opt_outs']} opt-outs "
                        f"on {row['leads']} leads ({row['negative_rate']:.1%}, limit {neg_limit:.1%})."
                    ),
                    "evidence": evidence,
                }
            )
        elif row["points"] == 0:
            if row["leads"] >= PAUSE_LEAD_MULTIPLIER * floor:
                proposals.append(
                    {
                        "kind": "pause_industry",
                        "payload": {"industry_name": name},
                        "rationale": (
                            f"{name} produced no replies, meetings or deals from {row['leads']} leads."
                        ),
                        "evidence": evidence,
                    }
                )
            elif row["priority_score"] > 1:
                proposals.append(
                    {
                        "kind": "adjust_priority",
                        "payload": {
                            "industry_name": name,
                            "delta": -PRIORITY_CUT_NO_RESULTS,
                        },
                        "rationale": (
                            f"{name} produced no replies, meetings or deals from {row['leads']} leads."
                        ),
                        "evidence": evidence,
                    }
                )
        elif median_yield is not None and row["yield_per_100"] > median_yield:
            proposals.append(
                {
                    "kind": "adjust_priority",
                    "payload": {"industry_name": name, "delta": PRIORITY_BOOST},
                    "rationale": (
                        f"{name} yields {row['yield_per_100']} points per 100 leads "
                        f"against a median of {median_yield}."
                    ),
                    "evidence": evidence,
                }
            )
    return proposals


async def create_proposals(
    db_pool, proposals: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Insert proposals, skipping any with an open one for the same industry and kind."""
    created: list[dict[str, Any]] = []
    if not proposals:
        return created
    async with db_pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            for proposal in proposals:
                await cur.execute(
                    """
                    SELECT 1 FROM helix_strategy_proposals
                    WHERE status = 'pending' AND kind = %s
                      AND COALESCE(payload->>'industry_name', payload->>'target_agent') = %s
                    LIMIT 1;
                    """,
                    (proposal["kind"], _subject(proposal["payload"])),
                )
                if await cur.fetchone():
                    continue
                await cur.execute(
                    """
                    INSERT INTO helix_strategy_proposals (kind, payload, rationale, evidence)
                    VALUES (%s, %s, %s, %s) RETURNING id;
                    """,
                    (
                        proposal["kind"],
                        Jsonb(proposal["payload"]),
                        proposal["rationale"],
                        Jsonb(proposal["evidence"]),
                    ),
                )
                row = await cur.fetchone()
                created.append({**proposal, "id": row["id"]})
        await conn.commit()
    return created


async def expire_old_proposals(db_pool) -> None:
    async with db_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE helix_strategy_proposals
                SET status = 'expired', decided_at = NOW()
                WHERE status = 'pending'
                  AND created_at < NOW() - (%s || ' days')::INTERVAL;
                """,
                (PROPOSAL_EXPIRY_DAYS,),
            )
        await conn.commit()


async def list_pending_proposals(db_pool) -> list[dict[str, Any]]:
    async with db_pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT id, kind, payload, rationale, evidence
                FROM helix_strategy_proposals
                WHERE status = 'pending'
                ORDER BY id;
                """
            )
            return list(await cur.fetchall())


async def apply_proposal(db_pool, proposal_id: int) -> dict[str, Any]:
    """Apply one pending proposal atomically. Safe to call twice."""
    async with db_pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT id, kind, payload, status
                FROM helix_strategy_proposals WHERE id = %s FOR UPDATE;
                """,
                (proposal_id,),
            )
            proposal = await cur.fetchone()
            if not proposal:
                return {"ok": False, "reason": "not_found"}
            if proposal["status"] != "pending":
                return {"ok": False, "reason": f"already_{proposal['status']}"}

            payload = proposal["payload"]
            industry = payload.get("industry_name")
            kind = proposal["kind"]
            if kind == "chiron_brief":
                # Imported here: core.auditor and services.briefs both import this module.
                from core.auditor import send_optimization_to_chiron

                outcome = await send_optimization_to_chiron(db_pool, payload)
                if outcome["status"] not in {"queued", "already_queued"}:
                    await cur.execute(
                        "UPDATE helix_strategy_proposals SET result = %s WHERE id = %s;",
                        (Jsonb(outcome), proposal_id),
                    )
                    await conn.commit()
                    return {
                        "ok": False,
                        "reason": f"chiron_{outcome['status']}",
                        "detail": outcome["detail"],
                    }
                await cur.execute(
                    """
                    UPDATE helix_strategy_proposals
                    SET status = 'applied', decided_at = NOW(), result = %s
                    WHERE id = %s;
                    """,
                    (Jsonb(outcome), proposal_id),
                )
                await conn.commit()
                return {
                    "ok": True,
                    "kind": kind,
                    "result": {
                        "industry_name": payload["target_agent"],
                        "chiron_status": outcome["status"],
                    },
                }
            if kind == "adjust_priority":
                await cur.execute(
                    """
                    UPDATE helix_brain_scheduler
                    SET priority_score = GREATEST(priority_score + %s, 1)
                    WHERE industry_name = %s
                    RETURNING industry_name, priority_score, is_active;
                    """,
                    (int(payload["delta"]), industry),
                )
            elif kind in {"pause_industry", "resume_industry"}:
                await cur.execute(
                    """
                    UPDATE helix_brain_scheduler SET is_active = %s
                    WHERE industry_name = %s
                    RETURNING industry_name, priority_score, is_active;
                    """,
                    (kind == "resume_industry", industry),
                )
            else:
                return {"ok": False, "reason": "unknown_kind"}

            updated = await cur.fetchone()
            if not updated:
                return {"ok": False, "reason": "industry_not_found"}
            await cur.execute(
                """
                UPDATE helix_strategy_proposals
                SET status = 'applied', decided_at = NOW() WHERE id = %s;
                """,
                (proposal_id,),
            )
        await conn.commit()
    return {"ok": True, "kind": kind, "result": updated}


async def reject_proposal(db_pool, proposal_id: int) -> dict[str, Any]:
    async with db_pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                UPDATE helix_strategy_proposals
                SET status = 'rejected', decided_at = NOW()
                WHERE id = %s AND status = 'pending' RETURNING id;
                """,
                (proposal_id,),
            )
            row = await cur.fetchone()
        await conn.commit()
    return {"ok": row is not None, "reason": None if row else "not_pending"}


def format_scorecard(rows: list[dict[str, Any]], window_days: int, top: int = 10) -> str:
    if not rows:
        return f"No industries configured. Window: last {window_days} days."
    lines = [f"Last {window_days} days, top {min(top, len(rows))} industries by points:"]
    for row in rows[:top]:
        flag = "" if row["is_active"] else " [paused]"
        lines.append(
            f"{row['industry_name']}{flag}: {row['leads']} leads, "
            f"{row['replies']} replies, {row['meetings']} meetings, "
            f"{row['deals']} deals (${row['revenue']:,.0f}), "
            f"{row['bounces'] + row['opt_outs']} bounce/opt-out, "
            f"priority {row['priority_score']}"
        )
    return "\n".join(lines)


def format_proposals(proposals: list[dict[str, Any]]) -> str:
    if not proposals:
        return "No pending proposals."
    lines = []
    for p in proposals:
        payload = p["payload"]
        if p["kind"] == "chiron_brief":
            lines.append(
                f"#{p['id']} Chiron brief for {payload.get('target_agent')} "
                f"({payload.get('focus_area')}). {p['rationale']}"
            )
            continue
        change = {
            "adjust_priority": f"priority {payload.get('delta', 0):+d}",
            "pause_industry": "pause",
            "resume_industry": "resume",
        }.get(p["kind"], p["kind"])
        lines.append(
            f"#{p['id']} {payload.get('industry_name')}: {change}. {p['rationale']}"
        )
    lines.append("Reply /approve id or /reject id.")
    return "\n".join(lines)


async def run_strategy_review(
    db_pool, window_days: int = REVIEW_WINDOW_DAYS
) -> dict[str, Any]:
    await expire_old_proposals(db_pool)
    scorecard = await compute_scorecard(db_pool, window_days)
    proposals = generate_proposals(scorecard)
    pr_report = ""
    # Imported here: services.briefs imports helpers from this module.
    from services import briefs

    if briefs.briefs_enabled():
        try:
            brief_run = await briefs.propose_briefs(db_pool, window_days)
            proposals += brief_run["proposals"]
            pr_report = briefs.format_pr_states(brief_run["pr_states"])
        except Exception as exc:
            print(f"⚠️ [STRATEGY REVIEW] Brief generation skipped: {exc}")
    created = await create_proposals(db_pool, proposals)
    pending = await list_pending_proposals(db_pool)
    return {
        "scorecard": scorecard,
        "created": created,
        "pending": pending,
        "pr_report": pr_report,
    }


def review_has_news(result: dict[str, Any]) -> bool:
    """False for an empty database: nothing to report, so no Telegram message."""
    return bool(result["pending"]) or any(
        row["leads"] or row["points"] for row in result["scorecard"]
    )


def _review_notification(result: dict[str, Any]) -> ExecutiveNotificationEvent:
    return ExecutiveNotificationEvent(
        source_agent="helix",
        event_type="strategy_review",
        severity="info",
        headline="Weekly Strategy Review",
        outcome=html.escape(format_scorecard(result["scorecard"], REVIEW_WINDOW_DAYS)),
        impact=html.escape(
            chr(10).join(
                part
                for part in (
                    format_proposals(result["pending"]),
                    result.get("pr_report", ""),
                )
                if part
            )
        ),
        next_action="Approve or reject each proposal in Telegram.",
    )


async def strategy_review_loop(app) -> None:
    """Weekly review with catch-up: safe on a host that sleeps and wakes (Render free tier).
    Touches the database at most once per week unless a check fails."""
    print("[STRATEGY REVIEW] Weekly review loop initialized.")
    settled_week = None  # week_start we already know is reviewed, so no query is needed
    while True:
        delay = float(REVIEW_CHECK_INTERVAL_SECONDS)
        try:
            if os.getenv("HELIX_STRATEGY_REVIEW_ENABLED", "true").strip().lower() == "false":
                delay = 3600.0
            else:
                db_pool = app.state.db_pool
                now_et = datetime.datetime.now(EASTERN_TZ)
                week_start = current_week_start(now_et)
                # review_is_due(now, None) is False before Monday 07:00, with no query.
                if settled_week != week_start and review_is_due(now_et, None):
                    if review_is_due(now_et, await get_last_review_at(db_pool)):
                        result = await run_strategy_review(db_pool)
                        if review_has_news(result):
                            emit_executive_notification(_review_notification(result))
                        await write_helix_command_center_log(
                            db_pool,
                            "INFO",
                            f"Weekly strategy review completed: {len(result['created'])} new proposals, "
                            f"{len(result['pending'])} pending.",
                            REVIEW_MARKER,
                            "System Core",
                        )
                    settled_week = week_start
                delay = next_check_delay(
                    datetime.datetime.now(EASTERN_TZ),
                    settled_week == week_start,
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(f"⚠️ [STRATEGY REVIEW] Loop iteration anomaly: {exc}")
        await asyncio.sleep(delay)
