"""Turns weak spots in the outcome funnel into briefs for Chiron.

Helix states the goal and the evidence; Chiron decides the code change. Briefs
become `chiron_brief` proposals that the owner approves in Telegram. Nothing here
calls an LLM, so the same funnel always yields the same briefs.
"""

import datetime
import os
import re
from typing import Any

import httpx
from psycopg.rows import dict_row

from core.auditor import get_chiron_audit_candidates
from services.strategy import _float_env, _int_env, max_negative_rate, min_leads

LOW_HARVEST_DEFAULT = 100
LOW_REPLY_RATE_DEFAULT = 0.02
REPLIES_WITHOUT_MEETING_MIN = 5
LOW_BLUEPRINTS_DEFAULT = 1

PR_URL_PATTERN = re.compile(r"github\.com/([^/]+)/([^/]+)/pull/(\d+)")

CONSTRAINTS = (
    "Change prompts and logic only. Keep public function signatures. Do not touch "
    "authentication, secrets, sending limits or unsubscribe handling. Keep existing "
    "tests passing and add a test for any new behavior."
)
HONESTY = (
    "The weak result may be caused by data or market conditions rather than code. "
    "If you find no change that is likely to help, make no change and say so."
)
VERIFICATION = (
    "In the pull request description, state which file and behavior changed, which "
    "number above it is meant to move, and how a reviewer can check the change."
)

# One entry per rule. `agent` is the repository module that owns that funnel stage.
RULES: dict[str, dict[str, str]] = {
    "low_harvest": {
        "agent": "nova",
        "focus_area": "lead_discovery_yield",
        "goal": "Raise the number of qualified leads harvested per week.",
        "hint": "Look at search criteria, query broadening when the pool is depleted, and the parameters sent to the enrichment vendor.",
    },
    "high_negative_rate": {
        "agent": "nova",
        "focus_area": "lead_quality",
        "goal": "Lower the share of leads that bounce or opt out.",
        "hint": "Look at email validation, role-account filtering, and targeting rules that let poor-fit contacts through.",
    },
    "low_reply_rate": {
        "agent": "vanguard",
        "focus_area": "email_copy",
        "goal": "Raise the reply rate on outreach emails.",
        "hint": "Look at subject lines, the opening line, and how specific the email is to the lead's industry and pain. Prefer small, testable changes.",
    },
    "replies_without_meetings": {
        "agent": "vanguard",
        "focus_area": "call_to_action",
        "goal": "Turn replies into booked meetings.",
        "hint": "Look at the call to action, how a meeting is offered, and follow-up wording after a positive reply.",
    },
    "low_blueprints": {
        "agent": "beacon",
        "focus_area": "inbound_conversion",
        "goal": "Raise the number of inbound blueprint submissions.",
        "hint": "Look at post hooks, call-to-action wording, and how posts point readers to the blueprint tool.",
    },
}


def harvest_floor() -> int:
    return max(1, _int_env("HELIX_BRIEFS_MIN_HARVEST", LOW_HARVEST_DEFAULT))


def reply_rate_floor() -> float:
    return _float_env("HELIX_BRIEFS_MIN_REPLY_RATE", LOW_REPLY_RATE_DEFAULT)


def blueprint_floor() -> int:
    return max(0, _int_env("HELIX_BRIEFS_MIN_BLUEPRINTS", LOW_BLUEPRINTS_DEFAULT))


def briefs_enabled() -> bool:
    return os.getenv("HELIX_BRIEFS_ENABLED", "false").strip().lower() == "true"


async def compute_funnel(db_pool, window_days: int) -> dict[str, Any]:
    """Whole-business counts for the window. Rates use leads harvested as the
    denominator because send counts live in other services."""
    days = max(1, int(window_days))
    async with db_pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT stage, COUNT(*) AS n
                FROM helix_outcome_events
                WHERE occurred_at >= NOW() - (%s || ' days')::INTERVAL
                GROUP BY stage;
                """,
                (days,),
            )
            stage_rows = await cur.fetchall()
            await cur.execute(
                """
                SELECT COUNT(*) AS n FROM cold_leads
                WHERE created_at >= NOW() - (%s || ' days')::INTERVAL;
                """,
                (days,),
            )
            lead_row = await cur.fetchone()

    counts = {row["stage"]: int(row["n"]) for row in stage_rows}
    return {
        "window_days": days,
        "leads": int(lead_row["n"]) if lead_row else 0,
        "replies": counts.get("reply", 0) + counts.get("positive_reply", 0),
        "meetings": counts.get("meeting_booked", 0),
        "deals": counts.get("deal_won", 0),
        "bounces": counts.get("bounced", 0),
        "opt_outs": counts.get("opt_out", 0),
        "blueprints": counts.get("blueprint_submitted", 0),
    }


def _triggered_rules(funnel: dict[str, Any]) -> list[tuple[str, str]]:
    """(rule, explanation) pairs in priority order. Needs a minimum sample of leads
    for every rule except low_harvest, where too few leads is the finding."""
    floor = min_leads()
    leads = funnel["leads"]
    window = funnel["window_days"]
    found: list[tuple[str, str]] = []

    if leads < harvest_floor():
        found.append(
            (
                "low_harvest",
                f"Only {leads} leads were harvested in {window} days (target at least {harvest_floor()}).",
            )
        )
    if leads < floor:
        return found

    negative = funnel["bounces"] + funnel["opt_outs"]
    if negative / leads > max_negative_rate():
        found.append(
            (
                "high_negative_rate",
                f"{negative} bounces and opt-outs on {leads} leads "
                f"({negative / leads:.1%}, limit {max_negative_rate():.1%}).",
            )
        )
    if funnel["replies"] / leads < reply_rate_floor():
        found.append(
            (
                "low_reply_rate",
                f"{funnel['replies']} replies on {leads} leads "
                f"({funnel['replies'] / leads:.1%}, target at least {reply_rate_floor():.1%}).",
            )
        )
    elif funnel["replies"] >= REPLIES_WITHOUT_MEETING_MIN and funnel["meetings"] == 0:
        found.append(
            (
                "replies_without_meetings",
                f"{funnel['replies']} replies but no booked meetings in {window} days.",
            )
        )
    if funnel["blueprints"] < blueprint_floor():
        found.append(
            (
                "low_blueprints",
                f"{funnel['blueprints']} inbound blueprint submissions in {window} days "
                f"(target at least {blueprint_floor()}).",
            )
        )
    return found


def build_specification(rule: str, explanation: str, funnel: dict[str, Any]) -> str:
    spec = RULES[rule]
    numbers = ", ".join(
        f"{key} {funnel[key]}"
        for key in ("leads", "replies", "meetings", "deals", "bounces", "opt_outs", "blueprints")
    )
    return "\n\n".join(
        [
            f"GOAL: {spec['goal']}",
            f"EVIDENCE (last {funnel['window_days']} days): {explanation} Funnel: {numbers}.",
            f"WHERE TO LOOK: {spec['hint']}",
            f"CONSTRAINTS: {CONSTRAINTS}",
            f"HONESTY: {HONESTY}",
            f"VERIFICATION: {VERIFICATION}",
        ]
    )


def generate_brief_proposals(
    funnel: dict[str, Any], *, eligible_agents: set[str], blocked_agents: set[str]
) -> list[dict[str, Any]]:
    """At most one brief per target agent, from the earliest matching rule.

    eligible_agents: active, past cooldown, not queued or running (from the auditor).
    blocked_agents: agents whose last optimization PR is still open.
    """
    proposals: list[dict[str, Any]] = []
    taken: set[str] = set()
    for rule, explanation in _triggered_rules(funnel):
        spec = RULES[rule]
        agent = spec["agent"]
        if agent in taken or agent in blocked_agents or agent not in eligible_agents:
            continue
        taken.add(agent)
        proposals.append(
            {
                "kind": "chiron_brief",
                "payload": {
                    "target_agent": agent,
                    "focus_area": spec["focus_area"],
                    "reason": explanation,
                    "issues_detected": [explanation],
                    "metrics_context": {
                        key: funnel[key] for key in funnel if key != "window_days"
                    },
                    "optimization_specification": build_specification(
                        rule, explanation, funnel
                    ),
                    "rule": rule,
                },
                "rationale": f"{agent}: {spec['goal']} {explanation}",
                "evidence": dict(funnel),
            }
        )
    return proposals


def parse_pr_url(pr_url: str | None) -> tuple[str, str, int] | None:
    match = PR_URL_PATTERN.search(pr_url or "")
    if not match:
        return None
    return match.group(1), match.group(2), int(match.group(3))


async def get_pr_state(pr_url: str | None) -> dict[str, Any]:
    """open | merged | closed | unknown, and the merge time when merged. Read-only.
    Without GITHUB_TOKEN, or on any error, the state is 'unknown'."""
    parsed = parse_pr_url(pr_url)
    token = os.getenv("GITHUB_TOKEN", "")
    if not parsed or not token:
        return {"state": "unknown", "merged_at": None}
    owner, repo, number = parsed
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            res = await client.get(
                f"https://api.github.com/repos/{owner}/{repo}/pulls/{number}",
                headers={
                    "Authorization": f"Bearer {token}",
                    "Accept": "application/vnd.github+json",
                },
            )
        if res.status_code != 200:
            return {"state": "unknown", "merged_at": None}
        data = res.json()
    except Exception as exc:
        print(f"⚠️ [BRIEFS] PR state check failed: {exc}")
        return {"state": "unknown", "merged_at": None}
    if data.get("merged_at"):
        return {"state": "merged", "merged_at": data["merged_at"]}
    return {"state": data.get("state", "unknown"), "merged_at": None}


async def get_agent_pr_states(db_pool) -> dict[str, dict[str, Any]]:
    """Latest optimization PR per active agent, with its GitHub state."""
    async with db_pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT DISTINCT ON (LOWER(agent_name))
                       LOWER(agent_name) AS agent_name, last_optimization_pr_url
                FROM helix_agent_manifests
                WHERE is_active = TRUE AND last_optimization_pr_url IS NOT NULL
                ORDER BY LOWER(agent_name), last_optimized_at DESC NULLS LAST;
                """
            )
            rows = await cur.fetchall()
    states: dict[str, dict[str, Any]] = {}
    for row in rows:
        state = await get_pr_state(row["last_optimization_pr_url"])
        states[row["agent_name"]] = {**state, "pr_url": row["last_optimization_pr_url"]}
    return states


async def propose_briefs(db_pool, window_days: int) -> dict[str, Any]:
    """Everything the weekly review needs: proposals to create and PR states to report."""
    funnel = await compute_funnel(db_pool, window_days)
    pr_states = await get_agent_pr_states(db_pool)
    eligible = set(await get_chiron_audit_candidates(db_pool))
    blocked = {agent for agent, info in pr_states.items() if info["state"] == "open"}
    await attach_merge_funnels(db_pool, pr_states)
    return {
        "funnel": funnel,
        "pr_states": pr_states,
        "proposals": generate_brief_proposals(
            funnel, eligible_agents=eligible, blocked_agents=blocked
        ),
    }


async def funnel_since_merge(db_pool, merged_at: str) -> dict[str, Any] | None:
    """Funnel counts since a PR merged, so the next review can compare before and after."""
    try:
        merged = datetime.datetime.fromisoformat(merged_at.replace("Z", "+00:00"))
    except ValueError:
        return None
    days = max(1, (datetime.datetime.now(datetime.timezone.utc) - merged).days)
    return await compute_funnel(db_pool, days)


async def attach_merge_funnels(db_pool, pr_states: dict[str, dict[str, Any]]) -> None:
    """Adds funnel counts since the merge to each merged PR, in place."""
    for info in pr_states.values():
        if info["state"] == "merged" and info.get("merged_at"):
            info["since_merge"] = await funnel_since_merge(db_pool, info["merged_at"])


def format_pr_states(pr_states: dict[str, dict[str, Any]]) -> str:
    if not pr_states:
        return ""
    lines = ["Chiron pull requests:"]
    for agent, info in sorted(pr_states.items()):
        line = f"{agent}: {info['state']} {info['pr_url']}"
        since = info.get("since_merge")
        if since:
            line += (
                f" | since merge ({since['window_days']}d): {since['leads']} leads, "
                f"{since['replies']} replies, {since['meetings']} meetings. "
                "Low volume makes this noisy; do not read it as proof."
            )
        lines.append(line)
    return chr(10).join(lines)
