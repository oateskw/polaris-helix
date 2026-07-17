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
    from services.telemetry import send_telegram_broadcast

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

                    send_telegram_broadcast(
                        f"❌ *SCHEMA MISMATCH (422)*\nTarget Node: `{url}` rejected payload validation.\nDetails: `{error_details}`",
                        "critical",
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
                    send_telegram_broadcast(
                        f"⚠️ *DOWNSTREAM CLIENT REJECTION*\nTarget Node: `{url}` returned status `{status_code}`.\nExcerpt: `{response_excerpt}`",
                        "critical",
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
                send_telegram_broadcast(
                    f"⚠️ [PROXY EXCEPTION] Attempt {attempt} failed targeting `{url}`. Error Context: `{str(e)}`",
                    "verbose",
                )
                if attempt == max_retries:
                    send_telegram_broadcast(
                        f"💥 *MESH NODE UNREACHABLE*\nEndpoint: `{url}` failed all connection attempts.\nFinal Network Error: `{e}`",
                        "critical",
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
    )

    master_token = os.getenv("HELIX_AUTH_TOKEN", "")

    # 1. NEXUS OMNI-STRATEGIST (Enforced Dependency Barrier)
    if "nexus" in agent_list:
        if isolation_matrix.get(VENDOR_ANTHROPIC, False):
            send_telegram_broadcast(
                "Skipping Nexus Omni-Strategist sync sequence. Anthropic wallet ledger is isolated.",
                "critical",
            )
        else:
            url = os.getenv(
                "NEXUS_NERVOUS_IMPULSE_URL", "https://polaris-nexus.onrender.com"
            ).rstrip("/")
            await write_helix_command_center_log(
                db_pool,
                "INFO",
                "Dispatching tactical orchestration impulse to Nexus Omni-Strategist node.",
                "DISPATCH_NEXUS_REQUEST",
                strategy["industry_name"],
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
                },
            )

            if not nexus_success:
                send_telegram_broadcast(
                    f"🛑 *CRITICAL ORCHESTRATION HALT*\nNexus failed to initialize for strategy: `{strategy['industry_name']}`.\nSuppressing all downstream mesh tasks (Nova, Beacon, Vanguard) to prevent corruption.",
                    "critical",
                )
                await write_helix_command_center_log(
                    db_pool,
                    "ERROR",
                    "Nexus initialization error caught. Mesh execution sequence terminated.",
                    "ABORT_DOWNSTREAM_MESH",
                    strategy["industry_name"],
                )
                return  # Hard execution block protecting Beacon

            send_telegram_broadcast(
                "Nexus execution impulse successfully deployed down-mesh.", "verbose"
            )

    # 2. NOVA (Lead Harvesting Production Loop)
    if "nova" in agent_list:
        if isolation_matrix.get(VENDOR_MOLTSET, False):
            send_telegram_broadcast(
                "Skipping Lead Nova execution loop. Moltset circuit breaker is TRIPPED.",
                "critical",
            )
        else:
            url = os.getenv("NOVA_API_URL", "https://polaris-nova.onrender.com").rstrip(
                "/"
            )
            await write_helix_command_center_log(
                db_pool,
                "INFO",
                "Routing unrestricted lead harvesting job instructions to Nova edge node.",
                "DISPATCH_NOVA_REQUEST",
                strategy["industry_name"],
            )

            nova_payload = {
                "target_region": strategy["target_region"],
                "daily_quota_limit": 500,
                "target_company_limit": 5,
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
            send_telegram_broadcast(
                "Nova orchestration bundle successfully ignited on mesh edge node.",
                "verbose",
            )

    # 3. BEACON TASK DISPATCH
    if "beacon" in agent_list:
        url = os.getenv(
            "BEACON_API_URL", "https://polaris-beacon-agent.onrender.com"
        ).rstrip("/")
        await write_helix_command_center_log(
            db_pool,
            "INFO",
            "Dispatching structural framework synthesis orders to Beacon.",
            "DISPATCH_BEACON_REQUEST",
            strategy["industry_name"],
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
            await write_helix_command_center_log(
                db_pool,
                "INFO",
                f"Deploying Radar scraping profiles with a bounded keyword threshold of {keyword_cap}.",
                "DISPATCH_RADAR_REQUEST",
                strategy["industry_name"],
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
        await write_helix_command_center_log(
            db_pool,
            "INFO",
            "Triggering inbound comment monitoring sweep on Pulsar gateway node.",
            "DISPATCH_PULSAR_REQUEST",
            strategy["industry_name"],
        )
        asyncio.create_task(
            async_http_post_proxy(
                f"{url}/api/v1/pulsar/sweep", {"X-Pulsar-Auth-Token": master_token}, {}
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
            await write_helix_command_center_log(
                db_pool,
                "INFO",
                "Initiating secure outbound verification and email tracking sweeps via Vanguard.",
                "DISPATCH_VANGUARD_REQUEST",
                strategy["industry_name"],
            )
            asyncio.create_task(
                async_http_post_proxy(
                    f"{url}/api/v1/dispatch/sweep",
                    {
                        "X-Vanguard-Auth-Token": master_token,
                        "Content-Type": "application/json",
                    },
                    {},
                )
            )
            send_telegram_broadcast(
                "Vanguard outbound campaign sweep initiated successfully.", "verbose"
            )
