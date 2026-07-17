import os
import sys
import traceback
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse
from dotenv import load_dotenv
import psycopg

from database.schemas import PROVISION_SQL, SEED_SQL

load_dotenv()


def get_validated_database_url() -> str:
    """Validates and enforces strict SSL connection parameters for cloud Neon Postgres instances."""
    url = os.getenv("DATABASE_URL", "")
    if not url or not url.startswith("postgresql://"):
        raise ValueError(
            "Missing or invalid cloud DATABASE_URL. Please set your Neon Postgres "
            "connection string in your environment or .env file."
        )

    parsed = urlparse(url)
    query_map = dict(parse_qsl(parsed.query, keep_blank_values=True))

    if query_map.get("sslmode") != "require":
        query_map["sslmode"] = "require"
        url = urlunparse(parsed._replace(query=urlencode(query_map)))

    return url


def run_provisioning():
    try:
        database_url = get_validated_database_url()
    except ValueError as exc:
        print(f"\n❌ [init_db] CONFIGURATION ERROR: {exc}\n")
        sys.exit(1)

    print("📡 Connecting to Neon Cloud Postgres instance (SSL Enforced)...")
    try:
        with psycopg.connect(database_url) as conn:
            with conn.cursor() as cur:
                print("🛠️  Provisioning relational layout matrices...")
                cur.execute(PROVISION_SQL)

                print(
                    "🌱 Seeding critical operational infrastructure ledger defaults..."
                )
                cur.execute(SEED_SQL)

            conn.commit()
        print("✅ [init_db] Success! Schema provisioned and seed rows verified.")

    except (psycopg.OperationalError, OSError) as exc:
        print(
            "\n💥 [init_db] CLOUD CONNECTION FAILED\n"
            "Verify network boundaries, Render/Neon secrets, or connection strings.\n"
        )
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    run_provisioning()
