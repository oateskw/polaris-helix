import os
import traceback
import asyncio
import json
from contextlib import asynccontextmanager
from typing import Any, Literal, Optional

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
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
    request: Request,
    agent_name: Literal["vanguard", "beacon"] | None = Query(default=None),
):
    async def log_publisher(agent_name: Optional[str] = None):
        last_seen_id = 0

        try:
            while True:
                if await request.is_disconnected():
                    break

                async with app.state.db_pool.connection() as conn:
                    async with conn.cursor(row_factory=dict_row) as cur:
                        if agent_name is not None:
                            await cur.execute(
                                """
                                SELECT id, agent_name, log_level, message, timestamp
                                FROM agent_system_logs
                                WHERE id > %s AND agent_name = %s
                                ORDER BY id ASC;
                                """,
                                (last_seen_id, agent_name),
                            )
                        else:
                            await cur.execute(
                                """
                                SELECT id, agent_name, log_level, message, timestamp
                                FROM agent_system_logs
                                WHERE id > %s
                                ORDER BY id ASC;
                                """,
                                (last_seen_id,),
                            )

                        rows = await cur.fetchall()

                if rows:
                    last_seen_id = max(row["id"] for row in rows)
                    for row in rows:
                        payload = {
                            "id": row["id"],
                            "agent_name": row["agent_name"],
                            "log_level": row["log_level"],
                            "message": row["message"],
                            "timestamp": row["timestamp"].isoformat(),
                        }
                        yield f"data: {json.dumps(payload)}\n\n"
                else:
                    await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            return

    return StreamingResponse(
        log_publisher(agent_name=agent_name),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
