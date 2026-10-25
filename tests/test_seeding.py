import json
import re
from pathlib import Path

import pytest

import init_db
from database.seeding import (
    SEED_DIR,
    SEED_FILES,
    SeedError,
    apply_seed_data,
    export_seed_data,
    load_all_seed_data,
    load_seed_file,
    validate_manifest,
    validate_prompt,
    validate_schedule,
    validate_scheduler,
)


class FakeCursor:
    """Sync cursor stand-in that records SQL and reports canned results."""

    def __init__(self, existing_manifests=(), rowcount=1):
        self.executed: list[tuple[str, object]] = []
        self.existing_manifests = {m.lower() for m in existing_manifests}
        self.rowcount = rowcount
        self._last_manifest = None

    def execute(self, query, params=None):
        self.executed.append((query, params))
        if "SELECT 1 FROM helix_agent_manifests" in query:
            self._last_manifest = params[0]

    def fetchone(self):
        return (1,) if self._last_manifest in self.existing_manifests else None

    def sql(self):
        return "\n".join(q for q, _ in self.executed)


def write(tmp_path: Path, name: str, data) -> Path:
    (tmp_path / f"{name}.json").write_text(json.dumps(data), encoding="utf-8")
    return tmp_path


def test_shipped_seed_files_are_valid():
    seed = load_all_seed_data()
    names = {(r["agent_name"], r["prompt_key"]) for r in seed["helix_agent_prompts"]}
    assert ("chiron", "reactive_healer") in names
    assert ("chiron", "proactive_optimizer") in names
    agents = {r["agent_name"] for r in seed["helix_agent_manifests"]}
    assert agents == {
        "helix", "nexus", "beacon", "radar", "pulsar",
        "nova", "vanguard", "vector", "aura", "chiron",
    }


def test_chiron_prompts_demand_the_format_chiron_parses():
    pattern = r"<<<<<<< SEARCH\n(.*?)\n=======\n(.*?)\n>>>>>>> REPLACE"
    for row in load_seed_file("helix_agent_prompts"):
        prompt = row["system_prompt"]
        assert "<<<<<<< SEARCH" in prompt and ">>>>>>> REPLACE" in prompt
        # The template in the prompt itself must be matchable by Chiron's regex.
        assert re.search(pattern, prompt, re.DOTALL)


def test_only_audited_agents_are_active_in_the_manifest():
    audited = {"nexus", "beacon", "radar", "pulsar", "nova", "vanguard"}
    for row in load_seed_file("helix_agent_manifests"):
        assert row["is_active"] is (row["agent_name"] in audited), row["agent_name"]


def test_manifest_repositories_are_repo_names_not_files():
    for row in load_seed_file("helix_agent_manifests"):
        assert row["target_repository"].startswith("polaris-")
        assert ".py" not in row["target_repository"]
        assert row["target_files"] and all(f.endswith(".py") for f in row["target_files"])


@pytest.mark.parametrize("table", ["helix_brain_scheduler", "helix_agent_schedule"])
def test_example_templates_pass_validation(table):
    example = json.loads((SEED_DIR / f"{table}.example.json").read_text("utf-8"))
    for index, row in enumerate(example):
        SEED_FILES[table](row, f"{table}.example row {index + 1}")


def test_example_files_are_never_loaded(tmp_path):
    write_name = tmp_path / "helix_brain_scheduler.example.json"
    write_name.write_text(
        json.dumps([{"industry_name": "X", "bottleneck_profile": "b", "pain_tokens": []}]),
        encoding="utf-8",
    )
    assert load_seed_file("helix_brain_scheduler", tmp_path) == []


def test_missing_file_loads_as_empty(tmp_path):
    assert load_seed_file("helix_agent_prompts", tmp_path) == []


@pytest.mark.parametrize(
    "validator,row,fragment",
    [
        (validate_prompt, {"agent_name": "x", "prompt_key": "k"}, "system_prompt"),
        (validate_prompt, {"agent_name": "", "prompt_key": "k", "system_prompt": "s"}, "agent_name"),
        (validate_prompt, {"agent_name": "x", "prompt_key": "k", "system_prompt": "s", "extra": 1}, "unknown"),
        (validate_manifest, {"agent_name": "x", "is_active": "yes"}, "is_active"),
        (validate_manifest, {"agent_name": "x", "target_files": "a.py"}, "target_files"),
        (
            validate_scheduler,
            {"industry_name": "X", "bottleneck_profile": "b", "pain_tokens": [], "moltset_category": "Nope"},
            "moltset_category",
        ),
        (
            validate_scheduler,
            {"industry_name": "X", "bottleneck_profile": "b", "pain_tokens": [], "campaign_strategy": "RANDOM"},
            "campaign_strategy",
        ),
        (
            validate_scheduler,
            {"industry_name": "X", "bottleneck_profile": "b", "pain_tokens": [], "assigned_days": [7]},
            "assigned_days",
        ),
        (validate_scheduler, {"industry_name": "X", "bottleneck_profile": "b"}, "pain_tokens"),
        (validate_schedule, {"agent_name": "x", "weekday_int": 9}, "weekday_int"),
        (validate_schedule, {"agent_name": "x", "weekday_int": 1, "scheduled_time": "25:00"}, "scheduled_time"),
        (validate_schedule, {"agent_name": "x", "weekday_int": 1, "max_daily_runs": -1}, "max_daily_runs"),
    ],
)
def test_validators_reject_bad_rows(validator, row, fragment):
    with pytest.raises(SeedError) as excinfo:
        validator(row, "file row 1")
    assert fragment in str(excinfo.value)
    assert "file row 1" in str(excinfo.value)


def test_invalid_json_and_non_list_are_reported(tmp_path):
    (tmp_path / "helix_agent_prompts.json").write_text("{not json", encoding="utf-8")
    with pytest.raises(SeedError, match="invalid JSON"):
        load_seed_file("helix_agent_prompts", tmp_path)

    write(tmp_path, "helix_agent_prompts", {"a": 1})
    with pytest.raises(SeedError, match="JSON list"):
        load_seed_file("helix_agent_prompts", tmp_path)


def test_agent_names_are_lowercased_and_defaults_applied(tmp_path):
    write(
        tmp_path,
        "helix_brain_scheduler",
        [{"industry_name": "Retail", "bottleneck_profile": "b", "pain_tokens": ["x"]}],
    )
    row = load_seed_file("helix_brain_scheduler", tmp_path)[0]
    assert row["target_region"] == "Texas"
    assert row["priority_score"] == 1
    assert row["minimum_cooldown_hours"] == 24
    assert row["is_active"] is True
    assert row["moltset_category"] == "Professional and Business Services"

    prompt = validate_prompt(
        {"agent_name": "Chiron", "prompt_key": "k", "system_prompt": "  keep spacing\n"}, "w"
    )
    assert prompt["agent_name"] == "chiron"
    assert prompt["system_prompt"] == "  keep spacing\n"


def test_default_load_keeps_existing_rows():
    seed = {
        "helix_agent_prompts": [
            {"agent_name": "chiron", "prompt_key": "k", "system_prompt": "s"}
        ],
        "helix_brain_scheduler": [
            validate_scheduler(
                {"industry_name": "Retail", "bottleneck_profile": "b", "pain_tokens": []}, "w"
            )
        ],
        "helix_agent_schedule": [
            validate_schedule({"agent_name": "nova", "weekday_int": 1}, "w")
        ],
    }
    cur = FakeCursor()
    apply_seed_data(cur, seed)
    sql = cur.sql()
    assert sql.count("DO NOTHING") == 3
    assert "DO UPDATE" not in sql


def test_overwrite_updates_existing_rows():
    seed = {
        "helix_agent_prompts": [
            {"agent_name": "chiron", "prompt_key": "k", "system_prompt": "s"}
        ],
        "helix_agent_manifests": [validate_manifest({"agent_name": "nova"}, "w")],
    }
    cur = FakeCursor(existing_manifests=["nova"])
    results = apply_seed_data(cur, seed, overwrite=True)
    sql = cur.sql()
    assert "DO UPDATE SET system_prompt" in sql
    assert "UPDATE helix_agent_manifests" in sql
    assert results["helix_agent_manifests"] == (1, 0)


def test_manifest_inserts_new_and_skips_existing_by_default():
    seed = {
        "helix_agent_manifests": [
            validate_manifest({"agent_name": "nova"}, "w"),
            validate_manifest({"agent_name": "NEXUS"}, "w"),
        ]
    }
    cur = FakeCursor(existing_manifests=["nova"])
    results = apply_seed_data(cur, seed)
    sql = cur.sql()
    assert sql.count("INSERT INTO helix_agent_manifests") == 1
    assert "UPDATE helix_agent_manifests" not in sql
    assert results["helix_agent_manifests"] == (1, 1)


def test_empty_tables_are_skipped():
    cur = FakeCursor()
    assert apply_seed_data(cur, {"helix_agent_prompts": []}) == {}
    assert cur.executed == []


def test_dry_run_validates_without_connecting(monkeypatch, capsys):
    def boom(*args, **kwargs):
        raise AssertionError("dry run must not connect")

    monkeypatch.setattr(init_db.psycopg, "connect", boom)
    init_db.main(["--dry-run"])
    out = capsys.readouterr().out
    assert "helix_agent_prompts: 2 rows" in out
    assert "helix_agent_manifests: 10 rows" in out
    assert "No database was contacted" in out


def test_bad_seed_stops_before_any_connection(monkeypatch, tmp_path, capsys):
    write(tmp_path, "helix_agent_prompts", [{"agent_name": "x"}])
    monkeypatch.setattr("init_db.load_all_seed_data", lambda: load_all_seed_data(tmp_path))

    def boom(*args, **kwargs):
        raise AssertionError("must not connect when seed data is invalid")

    monkeypatch.setattr(init_db.psycopg, "connect", boom)
    with pytest.raises(SystemExit):
        init_db.run_provisioning()
    assert "SEED DATA ERROR" in capsys.readouterr().err


class ExportCursor:
    def __init__(self, rows_by_table):
        self.rows_by_table = rows_by_table
        self._rows = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, query):
        for table, rows in self.rows_by_table.items():
            if f"FROM {table}" in query:
                self._rows = rows

    def fetchall(self):
        return self._rows


class ExportConn:
    def __init__(self, rows_by_table):
        self.cursor_obj = ExportCursor(rows_by_table)

    def cursor(self, row_factory=None):
        return self.cursor_obj


def test_export_round_trips_through_the_validators(tmp_path):
    import datetime

    rows = {
        "helix_agent_prompts": [
            {"agent_name": "nova", "prompt_key": "main", "system_prompt": "hello"}
        ],
        "helix_agent_manifests": [
            {
                "agent_name": "nova", "is_active": True, "purpose": "p",
                "dependencies": None, "critical_paths": None,
                "target_repository": "polaris-vanguard", "target_files": ["nova_agent.py"],
            }
        ],
        "helix_brain_scheduler": [
            {
                "industry_name": "Retail", "moltset_category": "Retail",
                "bottleneck_profile": "b", "pain_tokens": ["x"], "target_region": "Texas",
                "is_active": True, "priority_score": 5, "minimum_cooldown_hours": 24,
                "assigned_days": [1], "campaign_strategy": None,
            }
        ],
        "helix_agent_schedule": [
            {
                "agent_name": "beacon", "weekday_int": 2, "is_enabled": True,
                "max_daily_runs": 1, "scheduled_time": datetime.time(9, 30),
            }
        ],
    }

    counts = export_seed_data(ExportConn(rows), tmp_path)

    assert counts == {table: 1 for table in rows}
    reloaded = load_all_seed_data(tmp_path)
    assert reloaded["helix_agent_schedule"][0]["scheduled_time"] == "09:30:00"
    assert reloaded["helix_brain_scheduler"][0]["priority_score"] == 5
    assert reloaded["helix_agent_prompts"][0]["system_prompt"] == "hello"


def test_seeded_vendor_names_match_the_code_constants_exactly():
    from core.engine import (
        VENDOR_ANTHROPIC,
        VENDOR_MOLTSET,
        VENDOR_MYEMAILVERIFIER,
        VENDOR_PHANTOMBUSTERS,
    )
    from database.schemas import SEED_SQL

    # The circuit matrix is keyed by the ledger's vendor_name and looked up with these
    # constants, so a case mismatch silently disables a circuit breaker.
    for name in (
        VENDOR_ANTHROPIC,
        VENDOR_MOLTSET,
        VENDOR_MYEMAILVERIFIER,
        VENDOR_PHANTOMBUSTERS,
    ):
        assert f"('{name}'," in SEED_SQL, name
