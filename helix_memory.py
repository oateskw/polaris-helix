import os
import psycopg
import httpx
from typing import List, Dict, Any, Optional

# Uses standard OpenAI embedding model (1536 dimensions matching vector(1536))
EMBEDDING_API_URL = "https://api.openai.com/v1/embeddings"


async def generate_embedding(text: str) -> List[float]:
    """Generates a 1536-dimensional vector embedding for semantic storage and retrieval."""
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        print(
            "⚠️ [Helix Memory] OPENAI_API_KEY missing. Skipping embedding generation."
        )
        return []

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                EMBEDDING_API_URL,
                headers={"Authorization": f"Bearer {api_key}"},
                json={"input": text, "model": "text-embedding-3-small"},
            )
            if resp.status_code == 200:
                return resp.json()["data"][0]["embedding"]
            else:
                print(
                    f"⚠️ [Helix Memory] Embedding API returned {resp.status_code}: {resp.text}"
                )
    except Exception as e:
        print(f"❌ [Helix Memory] Exception during embedding generation: {e}")

    return []


async def record_experience(
    agent_name: str,
    outcome_type: str,
    narrative_summary: str,
    industry: Optional[str] = None,
    campaign_id: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> bool:
    """
    Serializes operational outcomes into vector memory.

    :param agent_name: 'vanguard', 'pulsar', 'nova', 'beacon', or 'helix'
    :param outcome_type: 'high_conversion', 'high_bounce', 'opt_out', 'successful_harvest', etc.
    :param narrative_summary: Unstructured textual lesson learned.
    :param industry: Target industry or niche context.
    """
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("⚠️ [Helix Memory] DATABASE_URL missing. Cannot record memory.")
        return False

    embedding_input = f"[{agent_name.upper()} | Industry: {industry or 'General'}] {narrative_summary}"
    embedding = await generate_embedding(embedding_input)
    if not embedding:
        return False

    try:
        async with await psycopg.AsyncConnection.connect(db_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO helix_semantic_memory 
                    (agent_name, campaign_id, industry, outcome_type, narrative_summary, embedding, metadata)
                    VALUES (%s, %s, %s, %s, %s, %s::vector, %s);
                    """,
                    (
                        agent_name.lower(),
                        campaign_id,
                        industry,
                        outcome_type,
                        narrative_summary,
                        str(embedding),
                        psycopg.types.json.Jsonb(metadata or {}),
                    ),
                )
            await conn.commit()
            print(
                f"🧠 [Helix Memory] Recorded outcome for '{agent_name}' ({outcome_type})."
            )
            return True
    except Exception as e:
        print(f"❌ [Helix Memory] Failed to write memory row: {e}")
        return False


async def query_learned_guidelines(
    target_industry: str, agent_name: str, top_k: int = 3
) -> List[str]:
    """
    Retrieves historical intuition to build dynamic system rules for downstream agents.

    :return: List of dynamic directives formatted as "PREFER: ..." or "AVOID: ..."
    """
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        return []

    search_prompt = f"Targeting {target_industry} with agent {agent_name}. What strategies worked or failed?"
    query_vector = await generate_embedding(search_prompt)
    if not query_vector:
        return []

    try:
        async with await psycopg.AsyncConnection.connect(db_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT narrative_summary, outcome_type
                    FROM helix_semantic_memory
                    WHERE agent_name = %s OR agent_name = 'global'
                    ORDER BY embedding <=> %s::vector
                    LIMIT %s;
                    """,
                    (agent_name.lower(), str(query_vector), top_k),
                )
                rows = await cur.fetchall()

        guidelines = []
        for summary, outcome in rows:
            if outcome in ["high_bounce", "opt_out", "failure", "unresponsive"]:
                guidelines.append(f"AVOID: {summary}")
            else:
                guidelines.append(f"PREFER: {summary}")

        return guidelines
    except Exception as e:
        print(f"❌ [Helix Memory] Query execution failed: {e}")
        return []
