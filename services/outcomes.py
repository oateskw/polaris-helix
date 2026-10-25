import re
from typing import Any

from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

OUTCOME_STAGES = frozenset(
    {
        "reply",
        "positive_reply",
        "meeting_booked",
        "deal_won",
        "bounced",
        "opt_out",
        "blueprint_submitted",
    }
)
OUTCOME_SOURCES = frozenset({"vanguard_status", "telegram", "webhook", "blueprint"})


def stage_from_status(status: str | None) -> str | None:
    """Map a free-form lead status string onto a ledger stage, or None."""
    normalized = (status or "").lower().strip()
    if "bounce" in normalized:
        return "bounced"
    if re.search(r"opt[\s_-]?out|unsubscrib|do[\s_-]?not[\s_-]?contact", normalized):
        return "opt_out"
    if "meeting" in normalized or "booked" in normalized:
        return "meeting_booked"
    if "won" in normalized or "converted" in normalized:
        return "deal_won"
    if "reply" in normalized or "replied" in normalized:
        return "positive_reply" if "positive" in normalized else "reply"
    return None


async def record_outcome_event(
    db_pool,
    *,
    lead_email: str,
    stage: str,
    source: str,
    amount: float | None = None,
    source_event_id: str = "",
    industry_name: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Insert one outcome event, idempotent on (email, stage, source_event_id).

    Region and campaign strategy are snapshotted from the current scheduler row for
    the lead's industry, so attribution is approximate if those change later.
    Never raises: failures return {"recorded": False, "error": ...}.
    """
    email = (lead_email or "").strip().lower()
    if not email:
        return {"recorded": False, "error": "missing email"}
    if stage not in OUTCOME_STAGES:
        return {"recorded": False, "error": f"unknown stage '{stage}'"}
    if source not in OUTCOME_SOURCES:
        return {"recorded": False, "error": f"unknown source '{source}'"}

    try:
        async with db_pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                industry = industry_name
                if not industry:
                    await cur.execute(
                        "SELECT industry FROM cold_leads WHERE email = %s LIMIT 1;",
                        (email,),
                    )
                    lead_row = await cur.fetchone()
                    industry = lead_row["industry"] if lead_row else None

                region = strategy = None
                if industry:
                    await cur.execute(
                        """
                        SELECT target_region, campaign_strategy
                        FROM helix_brain_scheduler
                        WHERE industry_name = %s LIMIT 1;
                        """,
                        (industry,),
                    )
                    sched_row = await cur.fetchone()
                    if sched_row:
                        region = sched_row["target_region"]
                        strategy = sched_row["campaign_strategy"]

                await cur.execute(
                    """
                    INSERT INTO helix_outcome_events
                        (lead_email, industry_name, target_region, campaign_strategy,
                         stage, amount, source, source_event_id, metadata)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (lead_email, stage, source_event_id) DO NOTHING
                    RETURNING id;
                    """,
                    (
                        email,
                        industry,
                        region,
                        strategy,
                        stage,
                        amount,
                        source,
                        source_event_id or "",
                        Jsonb(metadata or {}),
                    ),
                )
                inserted = await cur.fetchone()
            await conn.commit()
        return {
            "recorded": inserted is not None,
            "duplicate": inserted is None,
            "industry_name": industry,
        }
    except Exception as exc:
        print(f"⚠️ [OUTCOMES] Failed to record {stage} for {email}: {exc}")
        return {"recorded": False, "error": str(exc)}
