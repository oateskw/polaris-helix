"""Export the live configuration tables to seed_data/*.json.

Run this after changing prompts, industries, manifests or the schedule, then
commit the files. They are what init_db.py reloads after a database loss.

    python export_seed.py

Lead data (cold_leads), logs and memory are NOT exported.
"""

import sys

import psycopg

from database.seeding import SEED_DIR, export_seed_data
from init_db import get_validated_database_url


def main() -> int:
    try:
        database_url = get_validated_database_url()
    except ValueError as exc:
        print(f"❌ [export_seed] CONFIGURATION ERROR: {exc}", file=sys.stderr)
        return 1

    try:
        with psycopg.connect(database_url) as conn:
            counts = export_seed_data(conn)
    except psycopg.Error as exc:
        print(f"❌ [export_seed] Database error: {exc}", file=sys.stderr)
        return 1

    for table, count in counts.items():
        print(f"{table}: {count} rows")
    print(f"✅ Wrote {len(counts)} files to {SEED_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
