import os
import traceback
from contextlib import asynccontextmanager
from typing import Any, Literal

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, ConfigDict
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool
import uvicorn

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


class LogIngestSchema(BaseModel):
    agent_name: Literal["vanguard", "beacon"]
    log_level: str = "INFO"
    message: str
    metadata: dict[str, Any] | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    database_url = get_validated_database_url()

    app.state.db_pool = AsyncConnectionPool(conninfo=database_url, open=False)
    try:
        await app.state.db_pool.open(wait=True)
    except Exception as exc:
        print(
            "\n[helix] CLOUD CONNECTION FAILED — possible causes: wrong credentials, "
            "DNS resolution failure, or network timeout. Full trace:\n"
        )
        traceback.print_exc()
        print(f"\n[helix] Error detail: {exc}\n")
        raise
    try:
        yield
    finally:
        await app.state.db_pool.close()


app = FastAPI(title="Helix Log Intake API", lifespan=lifespan)


@app.post("/api/v1/logs")
async def ingest_log(payload: LogIngestSchema):
    try:
        async with app.state.db_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO agent_system_logs (agent_name, log_level, message, metadata)
                    VALUES (%s, %s, %s, %s)
                    RETURNING id;
                    """,
                    (
                        payload.agent_name,
                        payload.log_level,
                        payload.message,
                        (
                            Jsonb(payload.metadata)
                            if payload.metadata is not None
                            else None
                        ),
                    ),
                )
                row = await cur.fetchone()
            await conn.commit()

        return {"status": "success", "logged_id": row[0]}
    except Exception as exc:
        raise HTTPException(
            status_code=500, detail=f"Failed to log event: {exc}"
        ) from exc


@app.get("/api/v1/stream")
async def stream_logs(
    agent_name: Literal["vanguard", "beacon"] | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=1000),
):
    try:
        async with app.state.db_pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                if agent_name is not None:
                    await cur.execute(
                        """
                        SELECT id, agent_name, log_level, message, metadata, timestamp
                        FROM agent_system_logs
                        WHERE agent_name = %s
                        ORDER BY timestamp DESC
                        LIMIT %s;
                        """,
                        (agent_name, limit),
                    )
                else:
                    await cur.execute(
                        """
                        SELECT id, agent_name, log_level, message, metadata, timestamp
                        FROM agent_system_logs
                        ORDER BY timestamp DESC
                        LIMIT %s;
                        """,
                        (limit,),
                    )
                records = await cur.fetchall()

        return {"status": "success", "records": records}
    except Exception as exc:
        raise HTTPException(
            status_code=500, detail=f"Failed to fetch logs: {exc}"
        ) from exc


if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
