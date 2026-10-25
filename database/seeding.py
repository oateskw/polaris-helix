"""Load and export the irreplaceable configuration rows as JSON files.

Seed files live in seed_data/ and are loaded by init_db.py. By default an
existing row is never changed, because Helix's optimizer rewrites prompts and
the owner edits schedules at runtime; pass overwrite=True to replace rows.
Files named *.example.json are templates and are never loaded.
"""

import json
from pathlib import Path
from typing import Any, Callable

from psycopg.rows import dict_row

from config.settings import MOLTSET_ALLOWED_CATEGORIES

SEED_DIR = Path(__file__).resolve().parent.parent / "seed_data"
VALID_CAMPAIGN_STRATEGIES = frozenset(
    {"LIVE_AUDIT", "OBJECTION_KILLER", "BUILD_IN_PUBLIC", "BEFORE_AFTER"}
)


class SeedError(ValueError):
    """A seed file is malformed. The message names the file, row and field."""


def _require_str(row: dict, key: str, where: str, *, allow_empty: bool = False) -> str:
    value = row.get(key)
    if not isinstance(value, str) or (not allow_empty and not value.strip()):
        raise SeedError(f"{where}: '{key}' must be a non-empty string")
    return value.strip() if key != "system_prompt" else value


def _optional_str(row: dict, key: str, where: str) -> str | None:
    value = row.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise SeedError(f"{where}: '{key}' must be a string or null")
    return value


def _str_list(row: dict, key: str, where: str, *, required: bool = False) -> list[str]:
    value = row.get(key)
    if value is None:
        if required:
            raise SeedError(f"{where}: '{key}' is required")
        return []
    if not isinstance(value, list) or not all(isinstance(v, str) for v in value):
        raise SeedError(f"{where}: '{key}' must be a list of strings")
    return value


def _check_unknown_keys(row: dict, allowed: set[str], where: str) -> None:
    unknown = set(row) - allowed - {"_note"}
    if unknown:
        raise SeedError(f"{where}: unknown field(s) {sorted(unknown)}")


def validate_prompt(row: dict, where: str) -> dict:
    _check_unknown_keys(row, {"agent_name", "prompt_key", "system_prompt"}, where)
    agent = _require_str(row, "agent_name", where).lower()
    key = _require_str(row, "prompt_key", where)
    if len(agent) > 50 or len(key) > 100:
        raise SeedError(f"{where}: agent_name max 50 chars, prompt_key max 100")
    return {
        "agent_name": agent,
        "prompt_key": key,
        "system_prompt": _require_str(row, "system_prompt", where),
    }


def validate_manifest(row: dict, where: str) -> dict:
    allowed = {
        "agent_name",
        "is_active",
        "purpose",
        "dependencies",
        "critical_paths",
        "target_repository",
        "target_files",
    }
    _check_unknown_keys(row, allowed, where)
    is_active = row.get("is_active", True)
    if not isinstance(is_active, bool):
        raise SeedError(f"{where}: 'is_active' must be true or false")
    return {
        "agent_name": _require_str(row, "agent_name", where).lower(),
        "is_active": is_active,
        "purpose": _optional_str(row, "purpose", where),
        "dependencies": _optional_str(row, "dependencies", where),
        "critical_paths": _optional_str(row, "critical_paths", where),
        "target_repository": _optional_str(row, "target_repository", where),
        "target_files": _str_list(row, "target_files", where),
    }


def validate_scheduler(row: dict, where: str) -> dict:
    allowed = {
        "industry_name",
        "moltset_category",
        "bottleneck_profile",
        "pain_tokens",
        "target_region",
        "is_active",
        "priority_score",
        "minimum_cooldown_hours",
        "assigned_days",
        "campaign_strategy",
    }
    _check_unknown_keys(row, allowed, where)
    category = row.get("moltset_category", "Professional and Business Services")
    if category not in MOLTSET_ALLOWED_CATEGORIES:
        raise SeedError(
            f"{where}: 'moltset_category' must be one of the allowed MoltSet categories"
        )
    strategy = row.get("campaign_strategy")
    if strategy is not None and strategy not in VALID_CAMPAIGN_STRATEGIES:
        raise SeedError(
            f"{where}: 'campaign_strategy' must be null or one of "
            f"{sorted(VALID_CAMPAIGN_STRATEGIES)}"
        )
    days = row.get("assigned_days", [])
    if (
        not isinstance(days, list)
        or not all(isinstance(d, int) and not isinstance(d, bool) for d in days)
        or any(d < 0 or d > 6 for d in days)
    ):
        raise SeedError(f"{where}: 'assigned_days' must be a list of integers 0 to 6")
    for key, default in (
        ("priority_score", 1),
        ("minimum_cooldown_hours", 24),
    ):
        value = row.get(key, default)
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise SeedError(f"{where}: '{key}' must be a non-negative integer")
    is_active = row.get("is_active", True)
    if not isinstance(is_active, bool):
        raise SeedError(f"{where}: 'is_active' must be true or false")
    return {
        "industry_name": _require_str(row, "industry_name", where),
        "moltset_category": category,
        "bottleneck_profile": _require_str(row, "bottleneck_profile", where),
        "pain_tokens": _str_list(row, "pain_tokens", where, required=True),
        "target_region": _optional_str(row, "target_region", where) or "Texas",
        "is_active": is_active,
        "priority_score": row.get("priority_score", 1),
        "minimum_cooldown_hours": row.get("minimum_cooldown_hours", 24),
        "assigned_days": days,
        "campaign_strategy": strategy,
    }


def validate_schedule(row: dict, where: str) -> dict:
    allowed = {
        "agent_name",
        "weekday_int",
        "is_enabled",
        "max_daily_runs",
        "scheduled_time",
    }
    _check_unknown_keys(row, allowed, where)
    weekday = row.get("weekday_int")
    if (
        not isinstance(weekday, int)
        or isinstance(weekday, bool)
        or not 0 <= weekday <= 6
    ):
        raise SeedError(f"{where}: 'weekday_int' must be an integer 0 (Sunday) to 6")
    enabled = row.get("is_enabled", True)
    if not isinstance(enabled, bool):
        raise SeedError(f"{where}: 'is_enabled' must be true or false")
    max_runs = row.get("max_daily_runs")
    if max_runs is not None and (
        not isinstance(max_runs, int) or isinstance(max_runs, bool) or max_runs < 0
    ):
        raise SeedError(f"{where}: 'max_daily_runs' must be a non-negative integer")
    scheduled = row.get("scheduled_time")
    if scheduled is not None:
        parts = scheduled.split(":") if isinstance(scheduled, str) else []
        if not (
            len(parts) in (2, 3)
            and all(p.isdigit() for p in parts)
            and int(parts[0]) < 24
            and int(parts[1]) < 60
            and (len(parts) == 2 or int(parts[2]) < 60)
        ):
            raise SeedError(f"{where}: 'scheduled_time' must look like 'HH:MM' (24h, ET)")
    return {
        "agent_name": _require_str(row, "agent_name", where).lower(),
        "weekday_int": weekday,
        "is_enabled": enabled,
        "max_daily_runs": max_runs,
        "scheduled_time": scheduled,
    }


# Order matters: nothing depends on another table, but prompts first keeps output stable.
SEED_FILES: dict[str, Callable[[dict, str], dict]] = {
    "helix_agent_prompts": validate_prompt,
    "helix_agent_manifests": validate_manifest,
    "helix_brain_scheduler": validate_scheduler,
    "helix_agent_schedule": validate_schedule,
}


def load_seed_file(table: str, seed_dir: Path = SEED_DIR) -> list[dict]:
    """Read and validate one seed file. Returns [] when the file does not exist."""
    path = seed_dir / f"{table}.json"
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise SeedError(f"{path.name}: invalid JSON ({exc})") from exc
    if not isinstance(data, list):
        raise SeedError(f"{path.name}: must contain a JSON list of rows")
    validator = SEED_FILES[table]
    rows = []
    for index, row in enumerate(data):
        where = f"{path.name} row {index + 1}"
        if not isinstance(row, dict):
            raise SeedError(f"{where}: must be an object")
        rows.append(validator(row, where))
    return rows


def load_all_seed_data(seed_dir: Path = SEED_DIR) -> dict[str, list[dict]]:
    """Validate every seed file before touching the database."""
    return {table: load_seed_file(table, seed_dir) for table in SEED_FILES}


def _apply_prompts(cur, rows: list[dict], overwrite: bool) -> tuple[int, int]:
    changed = 0
    for row in rows:
        action = (
            "DO UPDATE SET system_prompt = EXCLUDED.system_prompt, updated_at = NOW()"
            if overwrite
            else "DO NOTHING"
        )
        cur.execute(
            f"""
            INSERT INTO helix_agent_prompts (agent_name, prompt_key, system_prompt)
            VALUES (%(agent_name)s, %(prompt_key)s, %(system_prompt)s)
            ON CONFLICT (agent_name, prompt_key) {action};
            """,
            row,
        )
        changed += cur.rowcount
    return changed, len(rows) - changed


def _apply_manifests(cur, rows: list[dict], overwrite: bool) -> tuple[int, int]:
    changed = 0
    for row in rows:
        cur.execute(
            "SELECT 1 FROM helix_agent_manifests WHERE LOWER(agent_name) = %s LIMIT 1;",
            (row["agent_name"],),
        )
        exists = cur.fetchone() is not None
        if exists and not overwrite:
            continue
        if exists:
            cur.execute(
                """
                UPDATE helix_agent_manifests
                SET is_active = %(is_active)s, purpose = %(purpose)s,
                    dependencies = %(dependencies)s, critical_paths = %(critical_paths)s,
                    target_repository = %(target_repository)s,
                    target_files = %(target_files)s
                WHERE LOWER(agent_name) = %(agent_name)s;
                """,
                row,
            )
        else:
            cur.execute(
                """
                INSERT INTO helix_agent_manifests
                    (agent_name, is_active, purpose, dependencies, critical_paths,
                     target_repository, target_files)
                VALUES (%(agent_name)s, %(is_active)s, %(purpose)s, %(dependencies)s,
                        %(critical_paths)s, %(target_repository)s, %(target_files)s);
                """,
                row,
            )
        changed += 1
    return changed, len(rows) - changed


def _apply_scheduler(cur, rows: list[dict], overwrite: bool) -> tuple[int, int]:
    changed = 0
    for row in rows:
        action = (
            """DO UPDATE SET moltset_category = EXCLUDED.moltset_category,
                bottleneck_profile = EXCLUDED.bottleneck_profile,
                pain_tokens = EXCLUDED.pain_tokens,
                target_region = EXCLUDED.target_region,
                is_active = EXCLUDED.is_active,
                priority_score = EXCLUDED.priority_score,
                minimum_cooldown_hours = EXCLUDED.minimum_cooldown_hours,
                assigned_days = EXCLUDED.assigned_days,
                campaign_strategy = EXCLUDED.campaign_strategy"""
            if overwrite
            else "DO NOTHING"
        )
        cur.execute(
            f"""
            INSERT INTO helix_brain_scheduler
                (industry_name, moltset_category, bottleneck_profile, pain_tokens,
                 target_region, is_active, priority_score, minimum_cooldown_hours,
                 assigned_days, campaign_strategy)
            VALUES (%(industry_name)s, %(moltset_category)s, %(bottleneck_profile)s,
                    %(pain_tokens)s, %(target_region)s, %(is_active)s, %(priority_score)s,
                    %(minimum_cooldown_hours)s, %(assigned_days)s, %(campaign_strategy)s)
            ON CONFLICT (industry_name) {action};
            """,
            row,
        )
        changed += cur.rowcount
    return changed, len(rows) - changed


def _apply_schedule(cur, rows: list[dict], overwrite: bool) -> tuple[int, int]:
    changed = 0
    for row in rows:
        action = (
            """DO UPDATE SET is_enabled = EXCLUDED.is_enabled,
                max_daily_runs = EXCLUDED.max_daily_runs,
                scheduled_time = EXCLUDED.scheduled_time"""
            if overwrite
            else "DO NOTHING"
        )
        cur.execute(
            f"""
            INSERT INTO helix_agent_schedule
                (agent_name, weekday_int, is_enabled, max_daily_runs, scheduled_time)
            VALUES (%(agent_name)s, %(weekday_int)s, %(is_enabled)s,
                    %(max_daily_runs)s, %(scheduled_time)s)
            ON CONFLICT (agent_name, weekday_int) {action};
            """,
            row,
        )
        changed += cur.rowcount
    return changed, len(rows) - changed


_APPLIERS = {
    "helix_agent_prompts": _apply_prompts,
    "helix_agent_manifests": _apply_manifests,
    "helix_brain_scheduler": _apply_scheduler,
    "helix_agent_schedule": _apply_schedule,
}


def apply_seed_data(
    cur, seed: dict[str, list[dict]], *, overwrite: bool = False
) -> dict[str, tuple[int, int]]:
    """Write seed rows with an open synchronous cursor.

    Returns {table: (rows_written, rows_left_alone)}. The caller owns the
    transaction, so a failure part-way leaves the database unchanged.
    """
    return {
        table: _APPLIERS[table](cur, rows, overwrite)
        for table, rows in seed.items()
        if rows
    }


# --- Export -----------------------------------------------------------------

_EXPORT_QUERIES = {
    "helix_agent_prompts": """
        SELECT agent_name, prompt_key, system_prompt
        FROM helix_agent_prompts ORDER BY agent_name, prompt_key;
    """,
    "helix_agent_manifests": """
        SELECT agent_name, is_active, purpose, dependencies, critical_paths,
               target_repository, target_files
        FROM helix_agent_manifests ORDER BY LOWER(agent_name);
    """,
    "helix_brain_scheduler": """
        SELECT industry_name, moltset_category, bottleneck_profile, pain_tokens,
               target_region, is_active, priority_score, minimum_cooldown_hours,
               assigned_days, campaign_strategy
        FROM helix_brain_scheduler ORDER BY industry_name;
    """,
    "helix_agent_schedule": """
        SELECT agent_name, weekday_int, is_enabled, max_daily_runs, scheduled_time
        FROM helix_agent_schedule ORDER BY agent_name, weekday_int;
    """,
}


def _json_ready(value: Any) -> Any:
    if hasattr(value, "strftime") and hasattr(value, "hour"):  # datetime.time
        return value.strftime("%H:%M:%S")
    return value


def export_seed_data(conn, seed_dir: Path = SEED_DIR) -> dict[str, int]:
    """Write the live configuration tables to seed_data/*.json. Returns row counts."""
    seed_dir.mkdir(parents=True, exist_ok=True)
    counts: dict[str, int] = {}
    with conn.cursor(row_factory=dict_row) as cur:
        for table, query in _EXPORT_QUERIES.items():
            cur.execute(query)
            rows = [
                {key: _json_ready(value) for key, value in row.items()}
                for row in cur.fetchall()
            ]
            (seed_dir / f"{table}.json").write_text(
                json.dumps(rows, indent=2, ensure_ascii=False) + "\n",
                encoding="utf-8",
            )
            counts[table] = len(rows)
    return counts
