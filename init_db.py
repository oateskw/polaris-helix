import argparse
import os
import sys
import traceback
from typing import Final
from urllib.parse import urlparse, parse_qsl, quote, urlencode, urlunparse
from dotenv import load_dotenv
import psycopg

from database.schemas import PROVISION_SQL, SEED_SQL
from database.seeding import SeedError, apply_seed_data, load_all_seed_data

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
    # Neon's pooled endpoint ("-pooler" in the host) rejects startup options.
    if "options" not in query_map and "-pooler" not in (parsed.hostname or ""):
        query_map["options"] = f"-c statement_timeout={_INIT_COMMAND_TIMEOUT}s"
    
    url = urlunparse(
        parsed._replace(query=urlencode(query_map, quote_via=quote))
    )

    return url


def run_provisioning(*, seed: bool = True, overwrite: bool = False) -> None:
    """Execute database provisioning with schema creation and seed data insertion.
    
    This function establishes a connection to the cloud Postgres instance,
    provisions the schema, and seeds initial operational data. It enforces
    transactional integrity and proper resource cleanup.
    
    Args:
        seed: Also load the JSON files in seed_data/ (prompts, manifests, scheduler,
            agent schedule). Files are validated before any connection is made.
        overwrite: Replace existing rows with the seed values. By default existing
            rows are left alone, so re-running never undoes runtime changes.

    Exits:
        sys.exit(1): On configuration errors or connection failures.
    """
    seed_rows: dict = {}
    if seed:
        try:
            seed_rows = load_all_seed_data()
        except SeedError as exc:
            print(f"\n❌ [init_db] SEED DATA ERROR: {exc}\n", file=sys.stderr)
            sys.exit(1)

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

            if seed:
                mode = "overwriting existing rows" if overwrite else "keeping existing rows"
                print(f"📦 Loading seed_data/ ({mode})...")
                results = apply_seed_data(cur, seed_rows, overwrite=overwrite)
                for table, (written, kept) in results.items():
                    print(f"   {table}: {written} written, {kept} left alone")
                if not results:
                    print("   No seed files found; nothing to load.")

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


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Provision the Helix database schema and load seed_data/."
    )
    parser.add_argument(
        "--no-seed", action="store_true", help="Create the schema only."
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace existing rows with the seed_data/ values (default: keep them).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate seed_data/ and print row counts without connecting.",
    )
    args = parser.parse_args(argv)

    # Windows consoles default to cp1252, which cannot print the status emoji.
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="replace")

    if args.dry_run:
        try:
            counts = {t: len(r) for t, r in load_all_seed_data().items()}
        except SeedError as exc:
            print(f"❌ [init_db] SEED DATA ERROR: {exc}", file=sys.stderr)
            sys.exit(1)
        for table, count in counts.items():
            print(f"{table}: {count} rows")
        print("✅ Seed files are valid. No database was contacted.")
        return

    run_provisioning(seed=not args.no_seed, overwrite=args.overwrite)


if __name__ == "__main__":
    main()
