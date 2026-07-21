import os
import asyncio
import httpx
from typing import List, Dict, Any
from psycopg.types.json import Jsonb

# Absolute imports based on your project layout
# Assumes app or db_pool is accessible, or pass db_pool directly into functions.

VENDOR_ANTHROPIC = "Anthropic"
VENDOR_MOLTSET = "Moltset"
VENDOR_PHANTOMBUSTERS = "PhantomBusters"
VENDOR_MYEMAILVERIFIER = "MyEmailVerifier"

# 🧠 STEP 3: Import memory retrieval service
from helix_memory import query_learned_guidelines


def get_phantombuster_keyword_cap(app_state, industry_name: str | None = None) -> int:
    PHANTOMBUSTER_KEYWORD_STAIRCASE = [25, 12, 8, 5]
    default_cap = PHANTOMBUSTER_KEYWORD_STAIRCASE[0]
    if not industry_name:
        return default_cap
    remediation_state = getattr(app_state, "phantombuster_remediation", {})
    industry_state = remediation_state.get(industry_name.lower(), {})
    keyword_cap = industry_state.get("keyword_cap", default_cap)
    return max(PHANTOMBUSTER_KEYWORD_STAIRCASE[-1], int(keyword_cap))


async def async_http_post_proxy(
    url: str, headers: dict, json_data: dict, max_retries: int = 3
) -> bool:
    """🚀 HIGH-PERFORMANCE PROXY PROBE WITH LIVE EXCEPTION VERIFICATION"""
    # Localized import to prevent circular dependency
    from services.telemetry import (
        send_telegram_broadcast,
        ExecutiveNotificationEvent,
        emit_executive_notification,
    )

    backoff = 2.0
    async with httpx.AsyncClient(http2=False, timeout=30.0) as client:
        for attempt in range(1, max_retries + 1):
            try:
                response = await client.post(url, headers=headers, json=json_data)
                status_code = response.status_code

                if 200 <= status_code < 300:
                    return True

                if status_code == 422:
                    try:
                        error_details = response.json()
                    except Exception:
                        error_details = response.text

                    emit_executive_notification(
                        ExecutiveNotificationEvent(
                            source_agent="helix",
                            event_type="critical_failure",
                            severity="critical",
                            headline="Downstream Schema Rejection",
                            outcome="A downstream service rejected payload validation.",
                            impact=f"Endpoint: {url}; details: {error_details}",
                            next_action="Review payload contract between Helix and target service.",
                        )
                    )
                    return False

                if status_code in (408, 429):
                    raise httpx.HTTPStatusError(
                        f"Retryable Client Status ({status_code})",
                        request=response.request,
                        response=response,
                    )

                if 400 <= status_code < 500:
                    response_excerpt = (response.text or "")[:300]
                    emit_executive_notification(
                        ExecutiveNotificationEvent(
                            source_agent="helix",
                            event_type="critical_failure",
                            severity="critical",
                            headline="Downstream Client Rejection",
                            outcome=f"Target service returned HTTP {status_code}.",
                            impact=f"Endpoint: {url}; excerpt: {response_excerpt}",
                            next_action="Validate credentials and request payload.",
                        )
                    )
                    return False

                if status_code in [502, 503, 504] or 500 <= status_code < 600:
                    raise httpx.HTTPStatusError(
                        f"Edge Server Error ({status_code})",
                        request=response.request,
                        response=response,
                    )

                return False
            except Exception as e:
                emit_executive_notification(
                    ExecutiveNotificationEvent(
                        source_agent="helix",
                        event_type="pipeline_milestone",
                        severity="warning",
                        headline="Proxy Retry Triggered",
                        outcome=f"Attempt {attempt} failed for downstream request.",
                        impact=f"Endpoint: {url}; error: {e}",
                        next_action="Automatic retry in progress.",
                    )
                )
                if attempt == max_retries:
                    emit_executive_notification(
                        ExecutiveNotificationEvent(
                            source_agent="helix",
                            event_type="critical_failure",
                            severity="critical",
                            headline="Mesh Node Unreachable",
                            outcome="All retry attempts to downstream endpoint failed.",
                            impact=f"Endpoint: {url}; error: {e}",
                            next_action="Inspect target service uptime and network path.",
                        )
                    )
                else:
                    await asyncio.sleep(backoff**attempt)
        return False


async def trigger_downstream_mesh_with_payload(
    db_pool, app_state, strategy: dict, agent_list: list, isolation_matrix: dict
):
    from services.telemetry import (
        send_telegram_broadcast,
        write_helix_command_center_log,
        ExecutiveNotificationEvent,
        emit_executive_notification,
    )

    if not agent_list:
        await write_helix_command_center_log(
            db_pool,
            "INFO",
            f"Mesh dispatch skipped for strategy '{strategy.get('industry_name', 'UNKNOWN')}': agent_list is empty (unassigned day).",
            "SKIP_MESH_DISPATCH_EMPTY_AGENT_LIST",
            strategy.get("industry_name", "UNKNOWN"),
        )
        return

    master_token = os.getenv("HELIX_AUTH_TOKEN", "")
    target_industry = strategy.get("industry_name", "General Operations")

    # 1. NEXUS OMNI-STRATEGIST (Enforced Dependency Barrier)
    if "nexus" in agent_list:
        if isolation_matrix.get(VENDOR_ANTHROPIC, False):
            emit_executive_notification(
                ExecutiveNotificationEvent(
                    source_agent="nexus",
                    event_type="vendor_circuit_breaker",
                    severity="critical",
                    headline="Nexus Dispatch Blocked",
                    outcome="Anthropic circuit breaker is active.",
                    next_action="Restore Anthropic credits before rerun.",
                )
            )
        else:
            url = os.getenv(
                "NEXUS_NERVOUS_IMPULSE_URL", "https://polaris-nexus.onrender.com"
            ).rstrip("/")

            # 🧠 Dynamic Memory Query for Nexus
            nexus_guidelines = await query_learned_guidelines(
                target_industry=target_industry, agent_name="nexus", top_k=3
            )

            await write_helix_command_center_log(
                db_pool,
                "INFO",
                f"Dispatching tactical orchestration impulse to Nexus node with {len(nexus_guidelines)} learned guidelines.",
                "DISPATCH_NEXUS_REQUEST",
                target_industry,
            )

            nexus_success = await async_http_post_proxy(
                f"{url}/api/v1/nexus/awaken",
                {
                    "X-Nexus-Auth-Token": master_token,
                    "Content-Type": "application/json",
                },
                {
                    "trigger_reason": "perpetual_awareness_heartbeat",
                    "batch_size": 25,
                    "rescue_mode": False,
                    "industry_id": strategy["industry_id"],
                    "industry_name": strategy["industry_name"],
                    "target_niche": strategy["industry_name"],
                    "target_region": strategy["target_region"],
                    "bottleneck_profile": strategy["bottleneck_profile"],
                    "pain_tokens": strategy["pain_tokens"],
                    "learned_guidelines": nexus_guidelines,  # 🚀 Memory Injection
                },
            )

            if not nexus_success:
                emit_executive_notification(
                    ExecutiveNotificationEvent(
                        source_agent="nexus",
                        event_type="critical_failure",
                        severity="critical",
                        headline="Orchestration Halted",
                        outcome=f"Nexus failed to initialize for {target_industry}.",
                        impact="Downstream Nova/Beacon/Vanguard dispatch was suppressed.",
                        next_action="Inspect Nexus availability and payload integrity.",
                    )
                )
                await write_helix_command_center_log(
                    db_pool,
                    "ERROR",
                    "Nexus initialization error caught. Mesh execution sequence terminated.",
                    "ABORT_DOWNSTREAM_MESH",
                    target_industry,
                )
                return

            emit_executive_notification(
                ExecutiveNotificationEvent(
                    source_agent="nexus",
                    event_type="pipeline_milestone",
                    severity="success",
                    headline="Nexus Dispatch Confirmed",
                    outcome="Nexus orchestration impulse deployed successfully.",
                    next_action="Await downstream milestone updates.",
                )
            )

    # 2. NOVA (Lead Harvesting Production Loop)
    if "nova" in agent_list:
        if isolation_matrix.get(VENDOR_MOLTSET, False):
            emit_executive_notification(
                ExecutiveNotificationEvent(
                    source_agent="nova",
                    event_type="vendor_circuit_breaker",
                    severity="critical",
                    headline="Nova Dispatch Blocked",
                    outcome="Moltset circuit breaker is active.",
                    next_action="Restore Moltset credits before rerun.",
                )
            )
            await write_helix_command_center_log(
                db_pool,
                "WARNING",
                "Nova dispatch blocked by Moltset isolation matrix.",
                "SKIP_NOVA_ISOLATION",
                target_industry,
            )
        else:
            url = os.getenv("NOVA_API_URL", "https://polaris-nova.onrender.com").rstrip(
                "/"
            )

            # 🧠 Dynamic Memory Query for Nova
            nova_guidelines = await query_learned_guidelines(
                target_industry=target_industry, agent_name="nova", top_k=3
            )

            await write_helix_command_center_log(
                db_pool,
                "INFO",
                f"Routing lead harvesting instructions to Nova node with {len(nova_guidelines)} learned guidelines.",
                "DISPATCH_NOVA_REQUEST",
                target_industry,
            )

            nova_payload = {
                "target_region": strategy["target_region"],
                "daily_quota_limit": 500,
                "target_company_limit": 35,
                "learned_guidelines": nova_guidelines,  # 🚀 Memory Injection
                "industries": [
                    {
                        "industry_id": strategy["industry_id"],
                        "industry_name": strategy["industry_name"],
                        "moltset_category": strategy["moltset_category"],
                        "bottleneck_profile": strategy["bottleneck_profile"],
                    }
                ],
            }
            asyncio.create_task(
                async_http_post_proxy(
                    f"{url}/api/v1/nova/ignite",
                    {
                        "Authorization": f"Bearer {master_token}",
                        "Content-Type": "application/json",
                    },
                    nova_payload,
                )
            )
            emit_executive_notification(
                ExecutiveNotificationEvent(
                    source_agent="nova",
                    event_type="pipeline_milestone",
                    severity="success",
                    headline="Nova Dispatch Started",
                    outcome=f"Nova execution initiated for {target_industry}.",
                    next_action="Monitor lead-harvest completion signal.",
                )
            )

    # 3. BEACON TASK DISPATCH
    if "beacon" in agent_list:
        url = os.getenv(
            "BEACON_API_URL", "https://polaris-beacon-agent.onrender.com"
        ).rstrip("/")

        # 🧠 Dynamic Memory Query for Beacon
        beacon_guidelines = await query_learned_guidelines(
            target_industry=target_industry, agent_name="beacon", top_k=3
        )

        await write_helix_command_center_log(
            db_pool,
            "INFO",
            f"Dispatching structural framework synthesis orders to Beacon with {len(beacon_guidelines)} learned guidelines.",
            "DISPATCH_BEACON_REQUEST",
            target_industry,
        )

        asyncio.create_task(
            async_http_post_proxy(
                f"{url}/api/v1/beacon/execute",
                {
                    "X-Beacon-Auth-Token": master_token,
                    "Content-Type": "application/json",
                },
                {
                    "industry_id": strategy["industry_id"],
                    "industry_name": strategy["industry_name"],
                    "target_region": strategy["target_region"],
                    "bottleneck_profile": strategy["bottleneck_profile"],
                    "learned_guidelines": beacon_guidelines,  # 🚀 Memory Injection
                },
            )
        )
        send_telegram_broadcast(
            "Beacon task instructions successfully dispatched down-mesh.", "verbose"
        )

    # 4. RADAR
    if "radar" in agent_list:
        if isolation_matrix.get(VENDOR_PHANTOMBUSTERS, False):
            send_telegram_broadcast(
                "Skipping Radar lead scraping cycle. PhantomBusters circuit breaker is TRIPPED.",
                "critical",
            )
        else:
            url = os.getenv(
                "RADAR_API_URL", "https://polaris-radar.onrender.com"
            ).rstrip("/")
            keyword_cap = get_phantombuster_keyword_cap(
                app_state, strategy.get("industry_name")
            )

            radar_guidelines = await query_learned_guidelines(
                target_industry=target_industry, agent_name="radar", top_k=3
            )

            await write_helix_command_center_log(
                db_pool,
                "INFO",
                f"Deploying Radar scraping profiles with bounded keyword threshold {keyword_cap}.",
                "DISPATCH_RADAR_REQUEST",
                target_industry,
            )

            asyncio.create_task(
                async_http_post_proxy(
                    f"{url}/api/v1/radar/execute",
                    {
                        "X-Radar-Auth-Token": master_token,
                        "X-Helix-Keyword-Cap": str(keyword_cap),
                        "Content-Type": "application/json",
                    },
                    {
                        "industry_id": strategy["industry_id"],
                        "industry_name": strategy["industry_name"],
                        "target_region": strategy["target_region"],
                        "bottleneck_profile": strategy["bottleneck_profile"],
                        "learned_guidelines": radar_guidelines,  # 🚀 Memory Injection
                    },
                )
            )
            send_telegram_broadcast(
                "Radar lead scraping cycle successfully deployed down-mesh.", "verbose"
            )

    # 5. PULSAR
    if "pulsar" in agent_list:
        url = os.getenv("PULSAR_API_URL", "https://polaris-pulsar.onrender.com").rstrip(
            "/"
        )

        pulsar_guidelines = await query_learned_guidelines(
            target_industry=target_industry, agent_name="pulsar", top_k=3
        )

        await write_helix_command_center_log(
            db_pool,
            "INFO",
            f"Triggering inbound comment monitoring sweep on Pulsar node with {len(pulsar_guidelines)} learned guidelines.",
            "DISPATCH_PULSAR_REQUEST",
            target_industry,
        )
        asyncio.create_task(
            async_http_post_proxy(
                f"{url}/api/v1/pulsar/sweep",
                {
                    "X-Pulsar-Auth-Token": master_token,
                    "Content-Type": "application/json",
                },
                {"learned_guidelines": pulsar_guidelines},  # 🚀 Memory Injection
            )
        )
        send_telegram_broadcast(
            "Pulsar sweep trigger successfully deployed down-mesh.", "verbose"
        )

    # 6. VANGUARD OUTBOUND SWEEP
    if "vanguard" in agent_list:
        if isolation_matrix.get(VENDOR_MYEMAILVERIFIER, False):
            send_telegram_broadcast(
                "Skipping Vanguard outbound campaign sweep. Validation credit ledger reported isolation.",
                "critical",
            )
        else:
            url = os.getenv(
                "VANGUARD_API_URL", "https://polaris-vanguard-core.onrender.com"
            ).rstrip("/")

            # 🧠 Dynamic Memory Query for Vanguard
            vanguard_guidelines = await query_learned_guidelines(
                target_industry=target_industry, agent_name="vanguard", top_k=3
            )

            await write_helix_command_center_log(
                db_pool,
                "INFO",
                f"Initiating outbound email tracking sweeps via Vanguard with {len(vanguard_guidelines)} learned guidelines.",
                "DISPATCH_VANGUARD_REQUEST",
                target_industry,
            )
            asyncio.create_task(
                async_http_post_proxy(
                    f"{url}/api/v1/dispatch/sweep",
                    {
                        "X-Vanguard-Auth-Token": master_token,
                        "Content-Type": "application/json",
                    },
                    {
                        "industry_name": target_industry,
                        "learned_guidelines": vanguard_guidelines,  # 🚀 Memory Injection
                    },
                )
            )
            send_telegram_broadcast(
                "Vanguard outbound campaign sweep initiated successfully.", "verbose"
            )

    # 7. AURA AUTOMATED MEDIA ENGINE
    if "aura" in agent_list:
        url = os.getenv("AURA_API_URL", "https://polaris-aura.onrender.com").rstrip("/")

        aura_guidelines = await query_learned_guidelines(
            target_industry=target_industry, agent_name="aura", top_k=3
        )

        await write_helix_command_center_log(
            db_pool,
            "INFO",
            "Dispatching strategic campaign parameters down-mesh to Aura video engine.",
            "DISPATCH_AURA_REQUEST",
            target_industry,
        )

        aura_payload = {
            "notification_type": strategy.get("notification_type"),
            "nexus_directive": strategy.get("nexus_directive"),
            "learned_guidelines": aura_guidelines,  # 🚀 Memory Injection
        }

        asyncio.create_task(
            async_http_post_proxy(
                f"{url}/api/v1/aura",
                {
                    "x-aura-secret": master_token,
                    "Content-Type": "application/json",
                },
                aura_payload,
            )
        )
        emit_executive_notification(
            ExecutiveNotificationEvent(
                source_agent="aura",
                event_type="pipeline_milestone",
                severity="success",
                headline="Aura Dispatch Started",
                outcome=f"Aura orchestration payload successfully dispatched for {target_industry}.",
                next_action="Awaiting automated compilation and media rendering metrics.",
            )
        )
