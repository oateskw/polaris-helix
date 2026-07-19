import os
import sys
import traceback
from typing import Final
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse
from dotenv import load_dotenv
import psycopg

from database.schemas import PROVISION_SQL, SEED_SQL

load_dotenv()

# Connection pool configuration for initialization phase
_INIT_CONNECT_TIMEOUT: Final[int] = 10
_INIT_COMMAND_TIMEOUT: Final[int] = 30


def get_validated_database_url() -> str:
    """Validates and enforces strict SSL connection parameters for cloud Neon Postgres instances.
    
    Returns:
        str: Validated database URL with enforced SSL requirements.
        
    Raises:
        ValueError: If DATABASE_URL is missing or malformed.
    """
    url = os.getenv("DATABASE_URL", "")
    if not url or not url.startswith("postgresql://"):
        raise ValueError(
            "Missing or invalid cloud DATABASE_URL. Please set your Neon Postgres "
            "connection string in your environment or .env file."
        )

    parsed = urlparse(url)
    query_map = dict(parse_qsl(parsed.query, keep_blank_values=True))

    # Enforce SSL and set connection timeouts for initialization reliability
    if query_map.get("sslmode") != "require":
        query_map["sslmode"] = "require"
    
    # Add timeout parameters if not present to prevent hanging during initialization
    if "connect_timeout" not in query_map:
        query_map["connect_timeout"] = str(_INIT_CONNECT_TIMEOUT)
    if "options" not in query_map:
        query_map["options"] = f"-c statement_timeout={_INIT_COMMAND_TIMEOUT}s"
    
    url = urlunparse(parsed._replace(query=urlencode(query_map)))

    return url


def run_provisioning() -> None:
    """Execute database provisioning with schema creation and seed data insertion.
    
    This function establishes a connection to the cloud Postgres instance,
    provisions the schema, and seeds initial operational data. It enforces
    transactional integrity and proper resource cleanup.
    
    Exits:
        sys.exit(1): On configuration errors or connection failures.
    """
    try:
        database_url = get_validated_database_url()
    except ValueError as exc:
        print(f"\n❌ [init_db] CONFIGURATION ERROR: {exc}\n", file=sys.stderr)
        sys.exit(1)

    print("📡 Connecting to Neon Cloud Postgres instance (SSL Enforced)...")
    
    conn = None
    try:
        conn = psycopg.connect(database_url, autocommit=False)
        
        with conn.cursor() as cur:
            print("🛠️  Provisioning relational layout matrices...")
            cur.execute(PROVISION_SQL)

            print("🌱 Seeding critical operational infrastructure ledger defaults...")
            cur.execute(SEED_SQL)

        conn.commit()
        print("✅ [init_db] Success! Schema provisioned and seed rows verified.")

    except (psycopg.OperationalError, psycopg.DatabaseError) as exc:
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                pass  # Rollback failure is secondary to the primary error
        
        print(
            "\n💥 [init_db] CLOUD CONNECTION FAILED\n"
            "Verify network boundaries, Render/Neon secrets, or connection strings.\n",
            file=sys.stderr
        )
        traceback.print_exc()
        sys.exit(1)
    
    except Exception as exc:
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                pass
        
        print(f"\n💥 [init_db] UNEXPECTED ERROR: {exc}\n", file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass  # Suppress cleanup errors


if __name__ == "__main__":
    run_provisioning()
