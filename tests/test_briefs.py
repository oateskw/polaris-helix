from unittest.mock import AsyncMock

import pytest

from services import briefs
from services.briefs import (
    RULES,
    briefs_enabled,
    build_specification,
    generate_brief_proposals,
    get_pr_state,
    parse_pr_url,
)
from services.strategy import apply_proposal, create_proposals, format_proposals

pytestmark = pytest.mark.anyio

ALL_AGENTS = {"nova", "vanguard", "beacon"}


def funnel(**overrides):
    base = {
        "window_days": 28,
        "leads": 300,
        "replies": 30,
        "meetings": 3,
        "deals": 1,
        "bounces": 3,
        "opt_outs": 1,
        "blueprints": 4,
    }
    base.update(overrides)
    return base


def generate(f, eligible=ALL_AGENTS, blocked=frozenset()):
    return generate_brief_proposals(
        f, eligible_agents=set(eligible), blocked_agents=set(blocked)
    )


def by_agent(proposals):
    return {p["payload"]["target_agent"]: p for p in proposals}


def test_healthy_funnel_produces_no_briefs():
    assert generate(funnel()) == []


def test_each_weak_stage_maps_to_its_agent():
    cases = {
        "low_harvest": (funnel(leads=40, replies=5), "nova"),
        "high_negative_rate": (funnel(bounces=20, opt_outs=5), "nova"),
        "low_reply_rate": (funnel(replies=2), "vanguard"),
        "replies_without_meetings": (funnel(replies=20, meetings=0), "vanguard"),
        "low_blueprints": (funnel(blueprints=0), "beacon"),
    }
    for rule, (f, agent) in cases.items():
        proposals = by_agent(generate(f))
        assert agent in proposals, rule
        assert RULES[rule]["agent"] == agent


def test_small_lead_sample_only_flags_low_harvest():
    proposals = generate(funnel(leads=10, replies=0, blueprints=0))
    assert [p["payload"]["rule"] for p in proposals] == ["low_harvest"]


def test_one_brief_per_agent_earliest_rule_wins():
    # Low reply rate and no meetings both point at vanguard; the earlier stage wins.
    proposals = generate(funnel(replies=2, meetings=0))
    vanguard = [p for p in proposals if p["payload"]["target_agent"] == "vanguard"]
    assert len(vanguard) == 1
    assert vanguard[0]["payload"]["rule"] == "low_reply_rate"


def test_guards_exclude_ineligible_and_blocked_agents():
    weak = funnel(replies=2, blueprints=0)
    assert set(by_agent(generate(weak, eligible={"beacon"}))) == {"beacon"}
    assert set(by_agent(generate(weak, blocked={"vanguard"}))) == {"beacon"}
    assert generate(weak, eligible=set()) == []


def test_specification_has_goal_evidence_constraints_and_verification():
    proposal = generate(funnel(replies=2))[0]
    spec = proposal["payload"]["optimization_specification"]
    for heading in ("GOAL:", "EVIDENCE", "CONSTRAINTS:", "HONESTY:", "VERIFICATION:"):
        assert heading in spec
    assert "2 replies" in spec
    assert "authentication" in spec
    assert proposal["payload"]["metrics_context"]["leads"] == 300
    assert "window_days" not in proposal["payload"]["metrics_context"]


def test_build_specification_names_the_window():
    spec = build_specification("low_blueprints", "none", funnel(window_days=7))
    assert "last 7 days" in spec


def test_flag_defaults_off(monkeypatch):
    monkeypatch.delenv("HELIX_BRIEFS_ENABLED", raising=False)
    assert briefs_enabled() is False
    monkeypatch.setenv("HELIX_BRIEFS_ENABLED", "true")
    assert briefs_enabled() is True


def test_parse_pr_url():
    assert parse_pr_url("https://github.com/oateskw/polaris-vanguard/pull/12") == (
        "oateskw",
        "polaris-vanguard",
        12,
    )
    assert parse_pr_url("not a pr") is None
    assert parse_pr_url(None) is None


async def test_pr_state_unknown_without_token(monkeypatch):
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    state = await get_pr_state("https://github.com/o/r/pull/1")
    assert state == {"state": "unknown", "merged_at": None}


async def test_run_strategy_review_skips_briefs_when_flag_off(monkeypatch):
    from services import strategy

    propose = AsyncMock()
    monkeypatch.delenv("HELIX_BRIEFS_ENABLED", raising=False)
    monkeypatch.setattr(briefs, "propose_briefs", propose)
    monkeypatch.setattr(strategy, "expire_old_proposals", AsyncMock())
    monkeypatch.setattr(strategy, "compute_scorecard", AsyncMock(return_value=[]))
    monkeypatch.setattr(strategy, "create_proposals", AsyncMock(return_value=[]))
    monkeypatch.setattr(strategy, "list_pending_proposals", AsyncMock(return_value=[]))

    await strategy.run_strategy_review(object())

    propose.assert_not_called()


async def test_run_strategy_review_adds_briefs_when_flag_on(monkeypatch):
    from services import strategy

    brief = {"kind": "chiron_brief", "payload": {"target_agent": "vanguard"}}
    create = AsyncMock(return_value=[])
    monkeypatch.setenv("HELIX_BRIEFS_ENABLED", "true")
    monkeypatch.setattr(
        briefs,
        "propose_briefs",
        AsyncMock(return_value={"proposals": [brief], "pr_states": {}}),
    )
    monkeypatch.setattr(strategy, "expire_old_proposals", AsyncMock())
    monkeypatch.setattr(strategy, "compute_scorecard", AsyncMock(return_value=[]))
    monkeypatch.setattr(strategy, "create_proposals", create)
    monkeypatch.setattr(strategy, "list_pending_proposals", AsyncMock(return_value=[]))

    await strategy.run_strategy_review(object())

    assert create.call_args.args[1] == [brief]


async def test_brief_failure_does_not_break_the_review(monkeypatch):
    from services import strategy

    monkeypatch.setenv("HELIX_BRIEFS_ENABLED", "true")
    monkeypatch.setattr(
        briefs, "propose_briefs", AsyncMock(side_effect=RuntimeError("boom"))
    )
    monkeypatch.setattr(strategy, "expire_old_proposals", AsyncMock())
    monkeypatch.setattr(strategy, "compute_scorecard", AsyncMock(return_value=[]))
    monkeypatch.setattr(strategy, "create_proposals", AsyncMock(return_value=[]))
    monkeypatch.setattr(strategy, "list_pending_proposals", AsyncMock(return_value=[]))

    result = await strategy.run_strategy_review(object())

    assert result["created"] == []


def brief_row(status="pending"):
    return {
        "id": 9,
        "kind": "chiron_brief",
        "payload": {"target_agent": "vanguard", "focus_area": "email_copy"},
        "status": status,
    }


async def test_approving_a_brief_dispatches_and_marks_applied(
    db_pool_factory, monkeypatch
):
    pool, connection, cursors = db_pool_factory(fetchone_sequence=[brief_row()])
    send = AsyncMock(return_value={"status": "queued", "detail": {}})
    monkeypatch.setattr("core.auditor.send_optimization_to_chiron", send)

    result = await apply_proposal(pool, 9)

    assert result["ok"] is True
    assert result["result"]["industry_name"] == "vanguard"
    send.assert_awaited_once()
    sql = " ".join(q for q, _ in cursors[0].executed)
    assert "status = 'applied'" in sql
    assert connection.commit_calls == 1


async def test_unreachable_chiron_leaves_the_brief_pending(db_pool_factory, monkeypatch):
    pool, connection, cursors = db_pool_factory(fetchone_sequence=[brief_row()])
    monkeypatch.setattr(
        "core.auditor.send_optimization_to_chiron",
        AsyncMock(return_value={"status": "unreachable", "detail": "timeout"}),
    )

    result = await apply_proposal(pool, 9)

    assert result["ok"] is False
    assert result["reason"] == "chiron_unreachable"
    sql = " ".join(q for q, _ in cursors[0].executed)
    assert "status = 'applied'" not in sql
    assert "SET result" in sql


async def test_already_applied_brief_is_not_sent_twice(db_pool_factory, monkeypatch):
    pool, _, _ = db_pool_factory(fetchone_sequence=[brief_row("applied")])
    send = AsyncMock()
    monkeypatch.setattr("core.auditor.send_optimization_to_chiron", send)

    result = await apply_proposal(pool, 9)

    assert result == {"ok": False, "reason": "already_applied"}
    send.assert_not_called()


async def test_brief_dedupe_keys_on_target_agent(db_pool_factory):
    pool, _, cursors = db_pool_factory(fetchone_sequence=[(1,)])
    proposal = {
        "kind": "chiron_brief",
        "payload": {"target_agent": "vanguard"},
        "rationale": "r",
        "evidence": {},
    }

    created = await create_proposals(pool, [proposal])

    assert created == []
    query, params = cursors[0].executed[0]
    assert "target_agent" in query
    assert params == ("chiron_brief", "vanguard")


def test_format_proposals_shows_brief_target():
    text = format_proposals(
        [
            {
                "id": 9,
                "kind": "chiron_brief",
                "payload": {"target_agent": "vanguard", "focus_area": "email_copy"},
                "rationale": "vanguard: low replies",
            }
        ]
    )
    assert "#9 Chiron brief for vanguard (email_copy)" in text


class FailingClient:
    calls = 0

    def __init__(self, *args, **kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def post(self, *args, **kwargs):
        FailingClient.calls += 1
        raise TimeoutError("service is waking")


async def test_dispatch_retries_once_then_reports_unreachable(
    db_pool_factory, monkeypatch
):
    from core.auditor import send_optimization_to_chiron

    pool, _, cursors = db_pool_factory()
    FailingClient.calls = 0
    monkeypatch.setattr(
        "core.auditor.is_vendor_circuit_healthy", AsyncMock(return_value=True)
    )
    monkeypatch.setattr("core.auditor.httpx.AsyncClient", FailingClient)

    outcome = await send_optimization_to_chiron(pool, {"target_agent": "vanguard"})

    assert outcome["status"] == "unreachable"
    assert FailingClient.calls == 2
    assert cursors[0].executed == []


async def test_dispatch_sends_only_fields_chiron_reads(db_pool_factory, monkeypatch):
    from core.auditor import send_optimization_to_chiron

    sent = {}

    class Client(FailingClient):
        async def post(self, url, **kwargs):
            sent.update(kwargs["json"])

            class Res:
                status_code = 200
                content = b"{}"
                text = "{}"

                def json(self):
                    return {"status": "optimization_analysis_queued"}

            return Res()

    pool, _, _ = db_pool_factory()
    monkeypatch.setattr(
        "core.auditor.is_vendor_circuit_healthy", AsyncMock(return_value=True)
    )
    monkeypatch.setattr("core.auditor.httpx.AsyncClient", Client)

    outcome = await send_optimization_to_chiron(
        pool, {"target_agent": "vanguard", "rule": "low_reply_rate", "focus_area": "x"}
    )

    assert outcome["status"] == "queued"
    assert "rule" not in sent
    assert sent["focus_area"] == "x"
