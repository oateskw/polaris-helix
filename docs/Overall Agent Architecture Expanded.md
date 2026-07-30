# Polaris Innovations: Overall Agent Architecture (Code-Aligned)

Version: July 2026  
Scope: This document is aligned to current code in polaris-helix, polaris-beacon, polaris-vanguard, polaris-vector, and polaris-aura.  
Excluded: polaris_website.

## 1) System Overview

Polaris runs a multi-agent Python service mesh focused on converting inbound and social demand into enriched outbound leads and measurable operational ROI.

Execution model is hybrid:
- Event-driven service-to-service API triggers
- Long-running workers and scheduled loops for orchestration, enrichment, telemetry replay, and vendor-credit safeguards
- Circuit-breaker controls for external vendor degradation and quota exhaustion

Current functional roles:
- Helix: orchestration core, telemetry ingestion, scheduler control, vendor-health and circuit governance
- Nexus: strategic directive compiler and campaign strategy router
- Beacon stack (Beacon, Radar, Pulsar): social campaign generation, scraping interception, and response workflows
- Vanguard stack (Vanguard Core, Nova): outbound dispatch plus lead harvesting
- Vector: social-hook ingestion and enrichment worker
- Aura: Python FastAPI marketing engine and Instagram/listener/hunter routes

## 2) Runtime and Deployment Shape

Runtime platform in this code scope:
- Python FastAPI services across Helix, Beacon stack, Vanguard stack, Vector gateway, and Aura
- Python worker loops for Helix orchestration loops and Vector enrichment sweeps

Important code-truth note:
- In this repository scope, Aura is implemented as Python FastAPI routes, not Next.js route handlers.

## 3) Data Plane and DB Ownership

Primary databases:
- DATABASE_URL:
  - Helix core state, scheduler, logs, oracle ledger, and related control-plane data
  - Beacon/Nexus history and strategy persistence paths in current wiring
- VANGUARD_DATABASE_URL:
  - Outbound lead lifecycle and enrichment pipeline tables
  - Nova staging/telemetry outbox and Vanguard dispatch lifecycle tables

Representative tables in active code paths:
- Helix-side: agent_system_logs, helix_brain_scheduler, helix_inbound_blueprints, helix_infrastructure_oracle_ledger
- Beacon/Nexus-side: beacon_history, nexus_industry_scheduler
- Vanguard/Vector/Nova-side: cold_leads, vanguard_processed_leads, vanguard_sent_history, nova_company_staging_queue, nova_telemetry_outbox

## 4) Service Architecture

### 4.1 Helix (polaris-helix/main.py)

Service type:
- FastAPI orchestration and telemetry API with startup-managed background workers

Key routes currently implemented:
- GET / and HEAD /
- POST /api/v1/ingest
- POST /api/v1/inbound-blueprints
- POST /api/v1/leads/status-update
- POST /api/v1/logs
- GET /api/v1/logs/latest
- GET /api/v1/telemetry/errors-count
- GET /api/v1/scheduler/current
- POST /api/v1/scheduler/rotate
- GET /api/v1/oracle/vendor-health
- GET /api/v1/oracle/vendor-health/{vendor_name}
- POST /api/v1/orchestrator/optimize-now
- POST /api/v1/orchestrator/run-cycle
- GET /api/v1/orchestrator/dashboard-diagnostics
- POST /api/v1/audit/evaluate-agent
- GET /api/v1/health

Core loops and workers started in lifespan:
- perpetual_awareness_loop
- credit_sync_scheduler_loop
- run_agent_performance_audit_loop
- reactive_optimizer_loop
- executive_digest_loop
- observability_watchdog_loop

Operational behavior:
- Enforces observability schema checks at startup
- Runs vendor-credit synchronization before loop fan-out
- Captures worker failure/stoppage into command-center logs and executive notifications

### 4.2 Nexus (polaris-beacon/nexus_agent.py)

Service type:
- FastAPI strategy receptor

Implemented routes:
- GET / and HEAD /
- POST /api/v1/nexus/awaken
- POST /api/v1/nexus/validate

Current responsibilities:
- Strategy family selection (LIVE_AUDIT, OBJECTION_KILLER, BUILD_IN_PUBLIC, BEFORE_AFTER)
- Buying-signal query generation
- Directive and hashtag shaping
- PhantomBuster launch orchestration
- Strategy/history persistence and Helix telemetry output

### 4.3 Beacon Stack (polaris-beacon)

#### A) Beacon Agent (beacon_agent.py)

Implemented routes:
- GET /
- POST /api/v1/beacon/execute

Role:
- LinkedIn campaign generation
- Prompt-layer guideline injection
- Publer scheduling and media flow
- Helix telemetry streaming

#### B) Radar (radar_agent.py)

Implemented routes:
- GET / and HEAD /
- POST /api/v1/radar/execute
- POST /api/v1/radar/interception-hook
- POST /api/v1/radar/instantly-hook

Role:
- Scraper/interception ingestion and normalization
- Dedupe and forwarding behavior for downstream enrichment paths
- Telemetry batching and operational safety logging

#### C) Pulsar (pulsar_agent.py)

Implemented routes:
- GET /
- POST /api/v1/pulsar/inbox-hook
- POST /api/v1/pulsar/interception-hook
- POST /api/v1/pulsar/sweep
- POST /api/v1/pulsar/telegram-callback

Role:
- Inbound comment/reply handling
- Sweep jobs and queue bridge behavior
- Dedupe and guardrailed reaction flows

### 4.4 Vanguard Stack (polaris-vanguard)

#### A) Vanguard Core (vanguard_core.py)

Implemented routes:
- POST /api/v1/dispatch/sweep
- POST /api/v1/admin/sync-replies
- POST /api/v1/webhooks/reply

Role:
- Pulls ready_to_send leads
- Quota checks and dispatch pacing
- Claude-generated outbound copy with fallback
- Instantly dispatch and lead-status mutation
- Aggregate telemetry to Helix

#### B) Nova (nova_agent.py)

Implemented routes:
- POST /api/v1/nova/ignite
- GET / and HEAD /

Role:
- MoltSet-powered company and people harvesting
- Advisory-lock protected run orchestration
- Weekly/short-window search-capacity guardrails
- Staging queue processing and cold_leads insert logic
- Telemetry outbox durability and replay to Helix
- Learned-guidelines normalization and conditional persistence mode (jsonb/text_array/fallback)

### 4.5 Vector (polaris-vector/src/api)

#### A) Social Hook Gateway (social_hook.py)

Implemented routes:
- POST /api/v1/vector/social-hook
- POST /api/v1/enrich-social-lead

Role:
- Inbound social lead capture
- Dedup checks and staged lead insert paths
- Structured comment generation for social enrichment handoffs
- Helix telemetry packets

#### B) Enrichment Worker (enrichment_worker.py)

Service type:
- Continuous worker daemon

Role:
- Sweeps staged leads
- MoltSet enrichment and MyEmailVerifier validation
- LLM typo-rescue fallback for invalid emails
- Promotion to ready_to_send when quality gates pass
- Failure-state mutation and telemetry dispatch

### 4.6 Aura (polaris-aura)

Service type:
- Python FastAPI app (aura_agent.py) with modular routers under routes/

Mounted router surfaces currently in app:
- /api/v1/aura
  - POST /execute
  - POST /generate-reel
- /api/v1/aura/hunter
  - POST /
- /api/v1/aura/instagram-listener
  - GET /
  - POST /
- /api/v1/aura/sheet
  - POST /
  - GET /
- /api/v1/helix/log
  - POST /

Role:
- Brand generation and campaign payloading
- Hunter and listener trigger paths
- CSV bridge utility route
- Helix proxy forwarding route

## 5) Orchestration and Event Flow

Canonical control-plane sequence:
1. Helix loop evaluates scheduler state and vendor/circuit health.
2. Helix dispatches secured downstream impulses by routing policy.
3. Downstream services execute asynchronously and emit telemetry to Helix.
4. Helix logs, diagnostics, and adaptive loops update command-center state.
5. Follow-up sweeps (Nova/Vector/Vanguard) mutate lead lifecycle state.

Manual override:
- POST /api/v1/orchestrator/run-cycle on Helix can force a run cycle with auth.

## 6) Security and Guardrails

Auth patterns in code:
- Shared service token checks via headers and/or bearer dependencies
- Mix of direct token comparison and hmac.compare_digest constant-time comparison

Key safety controls:
- Circuit gating on vendor health/quotas
- Retry paths around external APIs
- Dedupe checks in social and lead pipelines
- Background task separation for responsiveness
- Structured telemetry and alerting for failures and stalls

## 7) Financial and ROI Impact Analysis

The implemented pipeline is designed to convert operational friction into attributable revenue and cost-recovery signals.

Core ROI equations:
- Annual Waste Hours = Weekly Hours Wasted x 52
- Annual Financial Bleed = Annual Waste Hours x Loaded Hourly Cost
- Recovery Uplift = Annual Financial Bleed x Automation Recovery Rate

Example:
- Weekly waste: 18 hours
- Loaded hourly cost: 85 USD
- Annual waste hours: 18 x 52 = 936
- Annual financial bleed: 936 x 85 = 79,560 USD
- At 45 percent recovery: 35,802 USD recovered annually

## 8) Current Known Drift and Integration Gaps

1. Aura Helix proxy route mismatch remains:
- Aura proxy route forwards to /api/v1/helix/log.
- Helix canonical log ingestion route is /api/v1/logs.

2. Historical document drift has been removed:
- Previous references to Next.js Aura route files, plus Echo/Shadow/Prism runtime claims, were not aligned to this repository scope and are intentionally excluded from this code-aligned version.

## 9) Master Summary

Polaris currently operates as a resilient Python-first multi-agent mesh with:
- Helix as orchestration and observability core
- Nexus as strategy compiler
- Beacon/Radar/Pulsar as social execution and interception stack
- Nova and Vanguard Core as harvest-plus-dispatch outbound engine
- Vector as social-to-outbound enrichment bridge
- Aura as FastAPI-based campaign and listener surface

The strongest current architectural properties are:
- Circuit-aware execution
- Route-driven decoupled services
- Durable telemetry and outbox patterns
- Explicit fail-safe behaviors under vendor/API degradation
