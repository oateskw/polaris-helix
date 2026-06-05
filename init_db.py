"""
init_db.py — One-shot provisioning script that creates the agent_system_logs
table on our Neon Postgres cloud instance if it does not already exist.

Run once before starting the API:
    python init_db.py
"""

import os
import sys
import traceback

from dotenv import load_dotenv
import psycopg

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


PROVISION_SQL = """
CREATE TABLE IF NOT EXISTS agent_system_logs (
    id          BIGSERIAL    PRIMARY KEY,
    agent_name  TEXT         NOT NULL CHECK (agent_name IN ('vanguard', 'beacon')),
    log_level   TEXT         NOT NULL DEFAULT 'INFO'
                             CHECK (log_level IN ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL')),
    message     TEXT         NOT NULL,
    metadata    JSONB,
    timestamp   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_asl_agent_name  ON agent_system_logs (agent_name);
CREATE INDEX IF NOT EXISTS idx_asl_timestamp   ON agent_system_logs (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_asl_log_level   ON agent_system_logs (log_level);
"""


def main() -> None:
    try:
        database_url = get_validated_database_url()
    except ValueError as exc:
        print(f"\n[init_db] CONFIG ERROR: {exc}\n")
        sys.exit(1)

    print("[init_db] Connecting to Neon Postgres (SSL required)…")
    try:
        with psycopg.connect(database_url) as conn:
            with conn.cursor() as cur:
                cur.execute(PROVISION_SQL)
            conn.commit()
        print("[init_db] Schema provisioned on Neon: table + indexes are ready.")
    except (psycopg.OperationalError, OSError) as exc:
        print(
            "\n[init_db] CLOUD CONNECTION FAILED — possible causes: wrong credentials, "
            "DNS resolution failure, or network timeout. Full trace:\n"
        )
        traceback.print_exc()
        print(f"\n[init_db] Error detail: {exc}\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
