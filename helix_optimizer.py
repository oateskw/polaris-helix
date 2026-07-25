import asyncio
import os
import json
import re
from anthropic import AsyncAnthropic


async def is_vendor_circuit_healthy(db_pool, vendor_name: str) -> bool:
    """
    🆕 Checks the infrastructure ledger to see if a vendor's circuit is broken.
    Prevents hitting external APIs when they are flagged out of funds.
    """
    try:
        async with db_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT is_circuit_broken 
                    FROM helix_infrastructure_oracle_ledger 
                    WHERE vendor_name = %s LIMIT 1;
                    """,
                    (vendor_name,),
                )
                row = await cur.fetchone()
                if row:
                    # row[0] is the boolean `is_circuit_broken`
                    return not row[0]
                return True
    except Exception as e:
        print(f"⚠️ Circuit ledger lookup failed for {vendor_name}: {e}")
        return True


async def check_anomaly_threshold(db_pool, threshold=5) -> bool:
    """
    Checks if the error/warning log count over the last 3 hours crosses our threshold.
    """
    async with db_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                SELECT COUNT(*) FROM agent_system_logs 
                WHERE timestamp >= NOW() - INTERVAL '3 hours'
                AND log_level IN ('WARNING', 'ERROR');
            """)
            row = await cur.fetchone()
            count = row[0] if row else 0
            return count >= threshold


async def run_daily_prompt_optimization(db_pool, force=False):
    """
    Intelligent prompt-tuning engine driven by Claude 3.5.
    """
    # 🆕 SAFETY CHECK FIRST: Protect our budget and avoid calling a broken API
    anthropic_healthy = await is_vendor_circuit_healthy(db_pool, "Anthropic")
    if not anthropic_healthy:
        print(
            "🛑 [OPTIMIZER] [CIRCUIT BREAKER ENGAGED] Anthropic is flagged as broken/depleted. Aborting optimization."
        )
        return

    anthropic_key = os.getenv("ANTHROPIC_API_KEY")
    if not anthropic_key:
        print("⚠️ [OPTIMIZER] ANTHROPIC_API_KEY missing. Skipping.")
        return

    # Smart Check: If not forced, only optimize if we actually have issues to fix
    if not force:
        has_anomalies = await check_anomaly_threshold(db_pool)
        if not has_anomalies:
            print(
                "⏭️ [OPTIMIZER] System logs are clean. Skipping optimization to save tokens."
            )
            return

    # Dynamic model downgrade to Haiku to dramatically slash costs if forced or running
    # This also reads your environment configuration dynamically
    target_model = os.getenv("ANTHROPIC_MODEL", "claude-haiku-4-5")

    ai_client = AsyncAnthropic(api_key=anthropic_key)

    async with db_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                CREATE TABLE IF NOT EXISTS helix_agent_prompts (
                    id SERIAL PRIMARY KEY,
                    agent_name VARCHAR(50) NOT NULL,
                    prompt_key VARCHAR(100) NOT NULL,
                    system_prompt TEXT NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    CONSTRAINT unique_agent_prompt_key UNIQUE (agent_name, prompt_key)
                );
            """)
            await conn.commit()

            await cur.execute(
                "SELECT agent_name, prompt_key, system_prompt FROM helix_agent_prompts;"
            )
            raw_prompts = await cur.fetchall()

            if not raw_prompts:
                return

            await cur.execute("""
                SELECT agent_name, log_level, message, metadata 
                FROM agent_system_logs 
                WHERE timestamp >= NOW() - INTERVAL '24 hours'
                AND log_level IN ('WARNING', 'ERROR');
            """)
            logs = await cur.fetchall()

    log_context = "".join(
        f"[{log[0]} - {log[1]}]: {log[2]} (Meta: {json.dumps(log[3])})\n"
        for log in logs
    )

    for agent_name, prompt_key, current_prompt in raw_prompts:
        try:
            response = await ai_client.messages.create(
                model=target_model,
                max_tokens=2000,
                temperature=0.1,
                system=(
                    "You are an expert AI Meta-Orchestrator. Your role is to carefully analyze "
                    "agent system prompts and improve them based on execution feedback. "
                    "Look for any negative tone issues, formatting failures, or off-topic hallucinations. "
                    "Output ONLY the revised prompt wrapped in `<prompt>...</prompt>` tags."
                ),
                messages=[
                    {
                        "role": "user",
                        "content": f"Current Prompt:\n{current_prompt}\n\nLogs:\n{log_context or 'No execution errors.'}\n\nRewrite inside `<prompt>` tags.",
                    }
                ],
            )

            new_prompt = None
            for block in response.content:
                if block.type == "text":
                    match = re.search(r"<prompt>(.*?)</prompt>", block.text, re.DOTALL)
                    if match:
                        new_prompt = match.group(1).strip()

            if new_prompt and new_prompt != current_prompt:
                async with db_pool.connection() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute(
                            """
                            UPDATE helix_agent_prompts 
                            SET system_prompt = %s, updated_at = NOW() 
                            WHERE agent_name = %s AND prompt_key = %s;
                        """,
                            (new_prompt, agent_name, prompt_key),
                        )
                    await conn.commit()
                print(
                    f"✅ [OPTIMIZER] Successfully optimized '{agent_name}' ({prompt_key})."
                )
        except Exception as e:
            print(f"❌ [OPTIMIZER] Exception: {e}")


async def reactive_optimizer_loop(db_pool):
    """Continuous background loop monitoring system metrics natively."""
    print("🧠 [INTELLIGENT OPTIMIZER] Event-driven loop initialized.")
    while True:
        try:
            await run_daily_prompt_optimization(db_pool, force=False)
        except Exception as e:
            print(f"⚠️ [INTELLIGENT OPTIMIZER] Loop iteration error: {e}")
        await asyncio.sleep(3600)  # Check every hour
