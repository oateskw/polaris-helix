# helix_optimizer.py
import os
import json
import re
from anthropic import AsyncAnthropic


async def run_daily_prompt_optimization(db_pool):
    """
    Pre-flight job to fetch the latest agent prompts, analyze error/warning logs,
    and dynamically update prompt-specific keys using Claude 3.5.[cite: 6]
    """
    anthropic_key = os.getenv("ANTHROPIC_API_KEY")
    if not anthropic_key:
        print(
            "⚠️ [OPTIMIZER] ANTHROPIC_API_KEY missing. Skipping dynamic optimization."
        )
        return

    ai_client = AsyncAnthropic(api_key=anthropic_key)

    # 1. Fetch current prompts from the database[cite: 6]
    async with db_pool.connection() as conn:
        async with conn.cursor() as cur:
            # Safely verify the table exists with the unique multi-key constraint[cite: 6]
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

            # Fix: Retrieve the prompt_key so we don't overwrite dictionaries![cite: 6]
            await cur.execute(
                "SELECT agent_name, prompt_key, system_prompt FROM helix_agent_prompts;"
            )
            raw_prompts = await cur.fetchall()

            if not raw_prompts:
                print(
                    "⚠️ [OPTIMIZER] No prompts found in 'helix_agent_prompts'. Nothing to optimize."
                )
                return

            # 2. Fetch warnings or errors from the logs table to guide optimization[cite: 6]
            await cur.execute("""
                SELECT agent_name, log_level, message, metadata 
                FROM agent_system_logs 
                WHERE timestamp >= NOW() - INTERVAL '24 hours'
                AND log_level IN ('WARNING', 'ERROR');
            """)
            logs = await cur.fetchall()

    # Format logs for Claude to ingest as context[cite: 6]
    log_context = ""
    for log in logs:
        log_context += f"[{log[0]} - {log[1]}]: {log[2]} (Meta: {json.dumps(log[3])})\n"

    # Fix: Loop over isolated keys instead of losing them[cite: 6]
    for agent_name, prompt_key, current_prompt in raw_prompts:
        print(
            f"🧠 [OPTIMIZER] Analyzing system rules for '{agent_name}' ({prompt_key})..."
        )

        try:
            response = await ai_client.messages.create(
                model=os.getenv("ANTHROPIC_MODEL", "claude-3-5-sonnet-latest"),
                max_tokens=2000,
                temperature=0.1,  # Low temp for structured, logical modifications[cite: 6]
                system=(
                    "You are an expert AI Meta-Orchestrator. Your role is to carefully analyze "
                    "agent system prompts and improve them based on execution feedback. "
                    "Look for any negative tone issues, formatting failures (like unwanted hyphens or em-dashes), "
                    "or off-topic hallucinations mentioned in error/warning logs. Gently refine the prompt to patch those holes. "
                    "Output ONLY the revised prompt wrapped in `<prompt>...</prompt>` tags. Do not "
                    "include any other chat or introductory explanation."
                ),
                messages=[
                    {
                        "role": "user",
                        "content": (
                            f"Current Prompt for agent '{agent_name}' (Task/Key: {prompt_key}):\n"
                            f'"""\n{current_prompt}\n"""\n\n'
                            f"System Execution Logs from the past 24 hours:\n"
                            f"\"\"\"\n{log_context if log_context else 'No execution errors reported.'}\n\"\"\"\n\n"
                            f"Rewrite and improve the prompt to prevent failures, refine professional tone, "
                            f"and solidify output safety. Deliver only the finalized prompt inside `<prompt>` tags."
                        ),
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
                # Fix: Target the UPDATE using both agent_name AND prompt_key![cite: 6]
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
            else:
                print(
                    f"⏭️ [OPTIMIZER] No modification needed for '{agent_name}' ({prompt_key}). Prompt is optimal."
                )

        except Exception as e:
            print(
                f"❌ [OPTIMIZER] Exception occurred while optimizing '{agent_name}' ({prompt_key}): {e}"
            )
