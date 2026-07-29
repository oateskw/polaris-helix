import os
import psycopg
import httpx
from typing import List, Dict, Any, Optional

# Uses standard OpenAI embedding model (1536 dimensions matching vector(1536))
EMBEDDING_API_URL = "https://api.openai.com/v1/embeddings"
NEGATIVE_OUTCOMES = {"high_bounce", "opt_out", "failure", "unresponsive"}
MAX_GUIDELINE_RESULTS = 10


def _normalize_text(value: Optional[str], fallback: str = "") -> str:
    clean = (value or "").strip()
    return clean if clean else fallback


async def generate_embedding(text: str) -> List[float]:
    """Generates a 1536-dimensional vector embedding for semantic storage and retrieval."""
    clean_text = _normalize_text(text)
    if not clean_text:
        print("⚠️ [Helix Memory] Empty embedding input. Skipping embedding generation.")
        return []

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
                json={"input": clean_text, "model": "text-embedding-3-small"},
            )
            if resp.status_code == 200:
                payload = resp.json()
                rows = payload.get("data") or []
                if not rows or not isinstance(rows[0], dict):
                    print("⚠️ [Helix Memory] Embedding API returned malformed payload.")
                    return []
                embedding = rows[0].get("embedding")
                if not isinstance(embedding, list) or not embedding:
                    print(
                        "⚠️ [Helix Memory] Embedding API returned an empty embedding vector."
                    )
                    return []
                return embedding
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
    clean_agent = _normalize_text(agent_name).lower()
    clean_outcome = _normalize_text(outcome_type).lower()
    clean_summary = _normalize_text(narrative_summary)
    clean_industry = _normalize_text(industry)

    if not clean_agent or not clean_outcome or not clean_summary:
        print(
            "⚠️ [Helix Memory] Missing agent, outcome, or narrative summary. Cannot record memory."
        )
        return False

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("⚠️ [Helix Memory] DATABASE_URL missing. Cannot record memory.")
        return False

    clean_metadata = dict(metadata or {})
    source_event_id = _normalize_text(
        str(
            clean_metadata.get("source_event_id")
            or clean_metadata.get("event_id")
            or ""
        )
    )
    if source_event_id:
        clean_metadata["source_event_id"] = source_event_id

    embedding_input = f"[{clean_agent.upper()} | Industry: {clean_industry or 'General'}] {clean_summary}"
    embedding = await generate_embedding(embedding_input)
    if not embedding:
        return False

    try:
        async with await psycopg.AsyncConnection.connect(db_url) as conn:
            async with conn.cursor() as cur:
                if source_event_id:
                    await cur.execute(
                        """
                        SELECT 1
                        FROM helix_semantic_memory
                        WHERE LOWER(agent_name) = %s
                          AND metadata->>'source_event_id' = %s
                        LIMIT 1;
                        """,
                        (clean_agent, source_event_id),
                    )
                    if await cur.fetchone():
                        return True

                await cur.execute(
                    """
                    INSERT INTO helix_semantic_memory 
                    (agent_name, campaign_id, industry, outcome_type, narrative_summary, embedding, metadata)
                    VALUES (%s, %s, %s, %s, %s, %s::vector, %s);
                    """,
                    (
                        clean_agent,
                        _normalize_text(campaign_id) or None,
                        clean_industry or None,
                        clean_outcome,
                        clean_summary,
                        str(embedding),
                        psycopg.types.json.Jsonb(clean_metadata),
                    ),
                )
            await conn.commit()
            print(
                f"🧠 [Helix Memory] Recorded outcome for '{clean_agent}' ({clean_outcome})."
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
    clean_agent = _normalize_text(agent_name).lower()
    clean_industry = _normalize_text(target_industry, "General Operations")
    safe_top_k = max(1, min(int(top_k or 3), MAX_GUIDELINE_RESULTS))

    if not clean_agent:
        return []

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        return []

    search_prompt = (
        f"Targeting {clean_industry} with agent {clean_agent}. "
        "What strategies worked or failed?"
    )
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
                    ORDER BY
                        CASE
                            WHEN LOWER(COALESCE(industry, '')) = LOWER(%s) THEN 0
                            WHEN LOWER(agent_name) = 'global' THEN 1
                            ELSE 2
                        END,
                        embedding <=> %s::vector,
                        created_at DESC
                    LIMIT %s;
                    """,
                    (clean_agent, clean_industry, str(query_vector), safe_top_k),
                )
                rows = await cur.fetchall()

        guidelines = []
        seen_summaries = set()
        for summary, outcome in rows:
            clean_summary = _normalize_text(summary)
            if not clean_summary:
                continue

            dedupe_key = clean_summary.lower()
            if dedupe_key in seen_summaries:
                continue
            seen_summaries.add(dedupe_key)

            if _normalize_text(outcome).lower() in NEGATIVE_OUTCOMES:
                guidelines.append(f"AVOID: {clean_summary}")
            else:
                guidelines.append(f"PREFER: {clean_summary}")

        return guidelines
    except Exception as e:
        print(f"❌ [Helix Memory] Query execution failed: {e}")
        return []
