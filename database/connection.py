import os
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse
from psycopg_pool import AsyncConnectionPool


def get_validated_database_url() -> str:
    """
    Parses the DATABASE_URL environment variable and enforces 'sslmode=require'.
    This is required for robust communication with secure cloud databases like Neon.
    """
    url = os.getenv("DATABASE_URL", "")
    if not url or not url.startswith("postgresql://"):
        raise ValueError(
            "Missing or invalid DATABASE_URL. Please set your Postgres "
            "connection string in your environment."
        )

    parsed = urlparse(url)
    query_map = dict(parse_qsl(parsed.query, keep_blank_values=True))

    if query_map.get("sslmode") != "require":
        query_map["sslmode"] = "require"
        url = urlunparse(parsed._replace(query=urlencode(query_map)))

    return url


def create_async_db_pool() -> AsyncConnectionPool:
    """
    Configures and returns a high-performance AsyncConnectionPool for Helix.
    Using connection pooling prevents exhausting cloud limits on Render.
    """
    db_url = get_validated_database_url()
    return AsyncConnectionPool(
        conninfo=db_url,
        open=False,  # We call await pool.open() in lifespan startup
        check=AsyncConnectionPool.check_connection,
        max_idle=270.0,
    )
