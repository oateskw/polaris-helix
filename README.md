# Polaris Helix

Helix is the orchestration and observability core of the Polaris multi-agent service mesh. It is a FastAPI service that decides **what** the other agents work on each day, dispatches them, ingests their telemetry, guards the vendor budgets they spend, audits their performance, tunes their prompts, and reports to operators over Telegram.

> Part of the Polaris workspace. Sibling repos: [polaris-aura](https://github.com/oateskw/polaris-aura), [polaris-beacon](https://github.com/oateskw/polaris-beacon) (Nexus, Beacon, Radar, Pulsar), [polaris-chiron](https://github.com/oateskw/polaris-chiron), [polaris-vanguard](https://github.com/oateskw/polaris-vanguard) (Vanguard Core, Nova), [polaris-vector](https://github.com/oateskw/polaris-vector), [polaris-meridian](https://github.com/oateskw/polaris-meridian), [polaris_website](https://github.com/oateskw/polaris_website). For the cross-service picture, see [docs/Overall Agent Architecture Expanded.md](docs/Overall%20Agent%20Architecture%20Expanded.md).

## Contents

- [What Helix does](#what-helix-does)
- [Architecture](#architecture)
- [Repository layout](#repository-layout)
- [Getting started](#getting-started)
- [Configuration](#configuration)
- [Database](#database)
- [HTTP API](#http-api)
- [Telegram control surface](#telegram-control-surface)
- [Telegram security](#telegram-security)
- [Outcome ledger and strategy review](#outcome-ledger-and-strategy-review)
- [Running on Render free tier](#running-on-render-free-tier)
- [Background workers](#background-workers)
- [Vendor circuit breakers](#vendor-circuit-breakers)
- [Scheduling rules](#scheduling-rules)
- [Memory and prompt optimization](#memory-and-prompt-optimization)
- [Testing](#testing)
- [Deployment notes](#deployment-notes)
- [Known gaps](#known-gaps)

## What Helix does

| Responsibility | Where it lives |
| --- | --- |
| Pick the next industry/region target from the scheduler table and dispatch it to downstream agents | `core/engine.py`, `services/mesh_client.py` |
| Ingest logs and telemetry from every agent, and react to specific failure patterns | `main.py` (`/api/v1/logs`), `services/telemetry.py` |
| Track vendor credits and open circuit breakers when a vendor is depleted or unhealthy | `services/credit_sync.py`, `core/engine.py` |
| Audit agent KPIs and trigger Chiron (the self-healing/optimization agent) on degradation | `core/auditor.py` |
| Rewrite agent system prompts with Claude based on recent warnings/errors | `helix_optimizer.py` |
| Store and retrieve "learned guidelines" via pgvector semantic memory | `helix_memory.py` |
| Send throttled, de-duplicated executive notifications and twice-daily summaries | `services/telemetry.py` |
| Accept operator commands from Telegram | `telegram_orchestrator.py` |

## Architecture

```
                         Telegram operator
                               |  (webhook, secret-token auth)
                               v
 +-------------------------------------------------------------+
 |                          HELIX (FastAPI)                     |
 |                                                             |
 |  REST API  ----  Telegram router  ----  Executive notifier  |
 |                                                             |
 |  Background workers (started in lifespan)                   |
 |   - awareness-dispatcher   (core/engine.py)                 |
 |   - performance-auditor    (core/auditor.py)                |
 |   - vendor-credit-sync     (services/credit_sync.py)        |
 |   - prompt-optimizer       (helix_optimizer.py)             |
 |   - executive-summary-scheduler                             |
 |   - observability-watchdog                                  |
 +-----------+-----------------------------+-------------------+
             | secured HTTP impulses       | psycopg async pool
             v                             v
  Nexus, Nova, Beacon, Radar,       Postgres (Neon) + pgvector
  Pulsar, Vanguard, Aura, Chiron    logs, scheduler, ledger, memory
             |
             +--- telemetry posted back to POST /api/v1/logs
```

Control-plane loop:

1. A worker evaluates the scheduler table and vendor circuit state.
2. Helix dispatches authenticated requests to downstream agents, with learned guidelines injected per agent and industry.
3. Agents run asynchronously and post telemetry back to Helix.
4. Helix logs it, escalates anomalies, updates diagnostics, and notifies operators.

### Downstream agents

Helix dispatches to these services, each authenticated with a per-agent header carrying `HELIX_AUTH_TOKEN`:

| Agent | Role | Auth header sent by Helix | URL env var |
| --- | --- | --- | --- |
| Nexus | Strategy compiler | `X-Nexus-Auth-Token` | `NEXUS_NERVOUS_IMPULSE_URL` |
| Nova | MoltSet company/people harvesting | `Authorization: Bearer` | `NOVA_API_URL` |
| Beacon | LinkedIn campaign generation | `X-Beacon-Auth-Token` | `BEACON_API_URL` |
| Radar | Scraper interception | `X-Radar-Auth-Token` (+ `X-Helix-Keyword-Cap`) | `RADAR_API_URL` |
| Pulsar | Inbound reply/comment handling | `X-Pulsar-Auth-Token` | `PULSAR_API_URL` |
| Vanguard | Outbound dispatch | `X-Vanguard-Auth-Token` | `VANGUARD_API_URL` |
| Aura | Marketing/Instagram engine | `Authorization: Bearer` | `AURA_API_URL` |
| Chiron | Self-healing / optimization | via `core/auditor.py`, `services/telemetry.py` | `CHIRON_API_URL` |

Campaign strategies recognized by the mesh: `LIVE_AUDIT`, `OBJECTION_KILLER`, `BUILD_IN_PUBLIC`, `BEFORE_AFTER`.

## Repository layout

```
main.py                     FastAPI app, lifespan, REST endpoints, summary + watchdog loops
telegram_orchestrator.py    /api/v1/telegram/webhook and command handlers
register_telegram_webhook.py  CLI to register the Telegram webhook
init_db.py                  Provisions schema, seeds the vendor ledger, loads seed_data/
export_seed.py              Writes the live prompts, manifests, industries and schedule to seed_data/
seed_data/                  JSON backups of the irreplaceable configuration rows
helix_optimizer.py          Claude-driven prompt optimization (daily/reactive)
helix_memory.py             Embedding + pgvector semantic memory
config/settings.py          Constants, vendor URLs, schedule helpers, MoltSet categories
core/
  engine.py                 Perpetual awareness loop, Nova budget leveling, MoltSet circuit healing
  auditor.py                Agent KPI audits, Chiron dispatch, log retention
services/
  mesh_client.py            Downstream dispatch with isolation matrix and guideline injection
  telemetry.py              Log writing, notification policy, executive summaries, ROI boost, remediation
  credit_sync.py            PhantomBuster and MyEmailVerifier credit sync
database/
  connection.py             Async pool factory, DATABASE_URL validation
  schemas.py                PROVISION_SQL and SEED_SQL
tests/                      pytest suite
docs/                       Cross-service architecture notes
```

## Getting started

### Prerequisites

- Python 3.11+ (uses `X | None` type syntax and `zoneinfo`)
- A Postgres database with the `pgvector` extension (the project targets Neon)
- Credentials for the vendors you intend to use (see [Configuration](#configuration))

### Install

```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt        # runtime
pip install -r requirements-dev.txt    # runtime + pytest
```

### Configure

Copy [.env.example](.env.example) to `.env` (git-ignored) and fill it in. Minimum for local boot:

```env
DATABASE_URL=postgresql://user:pass@host/dbname
HELIX_AUTH_TOKEN=choose-a-long-random-token
ANTHROPIC_API_KEY=sk-ant-...
```

### Provision the database

```bash
python init_db.py
```

This runs `PROVISION_SQL`, `SEED_SQL` and the JSON files in [seed_data/](seed_data/README.md) in one transaction. It creates every table Helix reads or writes, including `helix_agent_schedule`, `helix_agent_manifests`, `helix_semantic_memory` and `helix_agent_prompts`. All statements are `IF NOT EXISTS`, so it is safe against an existing database. It enforces `sslmode=require` and sets a 10s connect timeout and 30s statement timeout. It is idempotent.

### Rebuild after losing the database

The rows that cannot be re-harvested (agent prompts, agent manifests, industries, the schedule) live in `seed_data/` as JSON, so a lost database is a short job:

```bash
python init_db.py --dry-run     # validate the seed files first, contacts nothing
python init_db.py               # schema + seed data; existing rows are kept
python export_seed.py           # after any change, write the live config back to seed_data/ and commit it
```

Existing rows are never overwritten unless you pass `--overwrite`, because the prompt optimizer and Telegram commands change them at runtime. Lead data, logs and memory are not backed up this way; use Neon's restore window or a scheduled `pg_dump` for those. See [seed_data/README.md](seed_data/README.md) for the file formats.

### Run

```bash
uvicorn main:app --host 0.0.0.0 --port 8000
```

On startup Helix opens the pool, ensures the observability schema, syncs vendor credits, and starts the six background workers. A startup notification, "Helix Orchestrator Online", is sent to Telegram if configured.

### Register the Telegram webhook

```bash
python register_telegram_webhook.py https://<your-host>/api/v1/telegram/webhook
```

`HELIX_AUTH_TOKEN` is used as Telegram's `secret_token`, so it may contain only letters, digits, `_` and `-` (max 256 chars).

## Configuration

All settings come from environment variables (loaded from `.env` via python-dotenv).

### Required

| Variable | Purpose |
| --- | --- |
| `DATABASE_URL` | Postgres connection string. Must start with `postgresql://`. `sslmode=require` is enforced. |
| `HELIX_AUTH_TOKEN` | Shared secret for inbound API auth and outbound calls to the mesh. Also the Telegram webhook secret unless `TELEGRAM_WEBHOOK_SECRET` is set. |

### Service integrations

| Variable | Default | Purpose |
| --- | --- | --- |
| `ANTHROPIC_API_KEY` | none | Executive summaries and prompt optimization. AI features are disabled if unset. |
| `ANTHROPIC_MODEL` | `claude-haiku-4-5` (optimizer), `claude-3-5-sonnet-latest` (settings.py) | Model for the optimizer. |
| `OPENAI_API_KEY` | none | Embeddings (`text-embedding-3-small`) for semantic memory. A warning is logged if missing. |
| `VANGUARD_API_KEY` | none | Auth key for `/api/v1/leads/status-update` (falls back to `HELIX_AUTH_TOKEN`). |
| `VANGUARD_DATABASE_URL` | none | Optional second DB, read by dashboard diagnostics. |
| `MOLTSET_API_KEY` | none | MoltSet capacity probes. |
| `PHANTOMBUSTER_API_KEY` | none | PhantomBuster credit sync. Missing key fails the vendor closed. |
| `MYEMAILVERIFIER_API_KEY` | none | MyEmailVerifier credit sync. |
| `TELEGRAM_BOT_TOKEN` | none | Operator bot. |
| `TELEGRAM_CHAT_ID` | none | Admin chat. Commands from other chats are not served. |
| `TELEGRAM_WEBHOOK_SECRET` | `HELIX_AUTH_TOKEN` | Dedicated webhook secret (recommended). Letters, digits, `_`, `-`; max 256. Re-run `register_telegram_webhook.py` after setting it. |
| `TELEGRAM_MAX_COMMAND_AGE_SECONDS` | `900` | Telegram updates older than this are dropped. |
| `TELEGRAM_SUMMARY_CHAT_ID` | `TELEGRAM_CHAT_ID` | Destination for summaries. |
| `TELEGRAM_CRITICAL_BOT_TOKEN`, `TELEGRAM_CRITICAL_CHAT_ID` | fall back to the above | Separate channel for critical alerts. |

### Agent URLs

Defaults point at the Render deployments (`config/settings.py`, `services/mesh_client.py`).

`CHIRON_API_URL`, `NEXUS_NERVOUS_IMPULSE_URL`, `NOVA_API_URL`, `BEACON_API_URL`, `RADAR_API_URL`, `PULSAR_API_URL`, `VANGUARD_API_URL`, `AURA_API_URL`

### Tuning

| Variable | Default | Purpose |
| --- | --- | --- |
| `HELIX_WATCHDOG_INTERVAL_SECONDS` | `300` | Watchdog tick. |
| `NOVA_STALE_RUN_MINUTES` | `120` | A Nova run with no terminal event after this long is flagged stale. |
| `HELIX_SUMMARY_CATCHUP_ON_STARTUP` | `false` | Send the most recent missed summary at boot. |
| `HELIX_NOTIFICATION_DEDUPE_SECONDS` | `300` | Duplicate notification suppression window. |
| `HELIX_CRITICAL_REPEAT_COOLDOWN_SECONDS` | `900` | Re-alert cooldown for repeated critical events. |
| `HELIX_NOTIFICATION_POLICY_MODE` | `strict` | Notification throttling mode. |
| `NEXUS_MORNING_HOUR` | `6` | Hour (ET) of the Nexus morning run. |
| `NOVA_DAILY_RESERVE_RATIO` | `0.10` | Fraction of weekly MoltSet capacity held back from daily budgets. |
| `NOVA_EXPECTED_RECORDS_PER_DOMAIN` | `25` | Used when estimating Nova consumption. |
| `NOVA_DEFAULT_RECORDS_PER_PEOPLE_QUERY` | `150` | Fallback estimate per people query. |
| `NOVA_THRESHOLD_ALERT_DEBOUNCE_HOURS` | `6` | Debounce for budget-threshold alerts. |
| `NOVA_BUDGET_DEFERRAL_ALERT_COOLDOWN_MINUTES` | `720` | Cooldown for "Nova deferred for budget" alerts. |
| `CHIRON_OPTIMIZATION_COOLDOWN_HOURS` | `168` (7 days) | Minimum gap between Chiron optimizations per agent. |
| `HELIX_STRATEGY_REVIEW_ENABLED` | `true` | Set `false` to stop the weekly strategy review. |
| `HELIX_STRATEGY_MIN_LEADS` | `30` | Minimum leads harvested in the window before an industry can get a proposal. |
| `HELIX_STRATEGY_MAX_NEGATIVE_RATE` | `0.05` | Bounce plus opt-out rate above which a priority cut is proposed. |
| `HELIX_BRIEFS_ENABLED` | `false` | Let the weekly review propose briefs for Chiron. |
| `HELIX_BRIEFS_MIN_HARVEST` | `100` | Leads per window below which Nova gets a brief. |
| `HELIX_BRIEFS_MIN_REPLY_RATE` | `0.02` | Replies per lead below which Vanguard gets a brief. |
| `HELIX_BRIEFS_MIN_BLUEPRINTS` | `1` | Blueprint submissions per window below which Beacon gets a brief. |
| `GITHUB_TOKEN` | unset | Read-only token for checking Chiron's pull requests. Optional. |
| `HELIX_PRIORITY_FIRST_ORDERING` | `false` | When `true`, scheduler selection sorts by `priority_score` before rotation order. Changes Nova rotation. |

Hard-coded constants in `config/settings.py`: PhantomBuster keyword staircase `[25, 12, 8, 5]`, zero-result threshold `2`, tuning cooldown `60` min, credit sync every 8h with ±10 min jitter.

## Database

`database/schemas.py` provisions the following; other tables are created lazily by the code that uses them.

| Table | Purpose |
| --- | --- |
| `agent_system_logs` | Central log store. Constrained agent names and levels, unique `metadata.event_id`, and a check that `ERROR` rows carry a Python traceback in `metadata.traceback`. |
| `helix_brain_scheduler` | Industry targets: pain tokens, region, priority, cooldown hours, `assigned_days` (0-6). |
| `helix_infrastructure_oracle_ledger` | Vendor credits and the `is_circuit_broken` flag. Seeded for MyEmailVerifier, PhantomBusters, Moltset, Anthropic. |
| `helix_inbound_blueprints` | Inbound ROI-calculator submissions. |
| `cold_leads` | Harvested leads and lifecycle status. |
| `chiron_healing_sessions` | One row per Chiron heal trigger (unique per triggering log). |
| `telegram_processed_updates` | Idempotency for Telegram updates (30 day retention). |
| `helix_semantic_memory` | pgvector-backed learned guidelines (checked at startup, indexed if present). |
| `helix_agent_manifests` | Per-agent state, including `optimization_status` and `last_optimized_at` (used by the auditor). |
| `helix_agent_prompts` | Editable agent system prompts (created by the optimizer). |
| `helix_outcome_events` | Append-only funnel ledger: reply, meeting, deal (with amount), bounce, opt-out, blueprint. Idempotent on `(lead_email, stage, source_event_id)`. |
| `helix_strategy_proposals` | Proposed priority/pause/resume changes awaiting your approval. |
| `helix_agent_schedule` | Per-agent weekday/time schedule read by the awareness loop (`weekday_int`, `scheduled_time`, `is_enabled`). |

Log retention: the auditor deletes `agent_system_logs` older than 30 days on every pass.

## HTTP API

Interactive docs are served at `/docs` when the app is running.

If `HELIX_AUTH_TOKEN` is unset, every authenticated endpoint returns 401 (fail closed).

Authentication is a shared-secret header compared in constant time (`hmac.compare_digest`). Header names differ by endpoint, listed below.

| Method | Path | Header | Description |
| --- | --- | --- | --- |
| GET, HEAD | `/` | none | Heartbeat. |
| GET | `/api/v1/health` | none | Health check. |
| POST | `/api/v1/ingest` | `x-helix-auth-token` | Ingest a list of leads. |
| POST | `/api/v1/inbound-blueprints` | `x-helix-auth-token` | Store an inbound ROI blueprint. |
| POST | `/api/v1/outcomes` | `x-helix-auth-token` | Record a funnel outcome (`reply`, `positive_reply`, `meeting_booked`, `deal_won`, `bounced`, `opt_out`) from a calendar or CRM webhook. Body: `email`, `stage`, optional `amount`, `source_event_id`. |
| POST | `/api/v1/leads/status-update?email=` | `x-vanguard-key` | Update lead status, triggers ROI attribution boost. |
| POST | `/api/v1/logs` | `x-helix-auth-token` | Ingest an agent log or telemetry event. Drives the reactive remediation in `analyze_and_orchestrate_log`. |
| GET | `/api/v1/logs/latest?agent=` | `x-inbound-key` | Latest log for an agent. |
| GET | `/api/v1/telemetry/errors-count?agent=` | `x-helix-auth-token` | Count of error/critical logs for one agent in the last 24h. |
| GET | `/api/v1/scheduler/current` | `x-inbound-key` | Active scheduler target. |
| POST | `/api/v1/scheduler/rotate` | `x-inbound-key` | Rotate to a new industry/region (`SchedulerTargetUpdateModel`). |
| GET | `/api/v1/oracle/vendor-health` | `x-inbound-key` | All vendor ledger rows. |
| GET | `/api/v1/oracle/vendor-health/{vendor_name}` | `x-inbound-key` | One vendor. |
| POST | `/api/v1/orchestrator/optimize-now` | `x-helix-auth-token` | Force prompt optimization, ignoring the anomaly threshold. |
| POST | `/api/v1/orchestrator/run-cycle` | `x-helix-auth-token` | Select the next eligible target and dispatch `nexus, beacon, nova, vanguard, pulsar` in the background. |
| GET | `/api/v1/orchestrator/dashboard-diagnostics` | `x-helix-auth-token` | Aggregated diagnostics for dashboards. |
| POST | `/api/v1/audit/evaluate-agent?agent_name=` | `x-helix-auth-token` | Run a KPI audit and trigger Chiron if underperforming. |
| POST | `/api/v1/telegram/webhook` | `X-Telegram-Bot-Api-Secret-Token` | Telegram updates. |

`run-cycle` target selection: the highest `priority_score` active industry whose `last_targeted_at` is older than its `minimum_cooldown_hours`. It stamps `last_targeted_at` when picked. It returns `idle` if nothing is eligible.

Industry names submitted to the scheduler are mapped deterministically onto the allowed MoltSet categories in `config/settings.py` (21 categories, defaulting to "Professional and Business Services").

## Telegram control surface

Commands are accepted only from `TELEGRAM_CHAT_ID`, and each update is claimed in `telegram_processed_updates` so retries do not run twice.

| Command | Action |
| --- | --- |
| `/status` | Worker and vendor-circuit status. |
| `/today` | Per-agent run counts for the current ET day. |
| `/history` | Recent activity. |
| `/summary [now\|daytime\|overnight]` | On-demand executive summary for a window. |
| `/optimize` | Force prompt optimization. |
| `/execute [agent]` | Run the full mesh, or one of `nexus, nova, beacon, radar, pulsar, vanguard, aura`. |
| `/enable <agent>`, `/disable <agent>` | Toggle an agent. `helix` can be enabled but not disabled. |
| `/reply <email>`, `/booked <email>` | Log a reply or a booked meeting for a lead. |
| `/won <email> <amount>` | Log a closed deal and its value in USD. |
| `/scorecard [7\|28]` | Per-industry funnel summary for the last 7 or 28 days. |
| `/proposals` | List pending strategy proposals. |
| `/approve <id>`, `/reject <id>` | Apply or discard a proposal. Nothing changes until you approve. |
| `/help` | Command reference. |

### Telegram security

The webhook is the only way to command Helix from outside, so it is layered:

- **Secret token.** Every request must carry `X-Telegram-Bot-Api-Secret-Token`, compared in constant time on UTF-8 bytes (non-ASCII input returns 401, not 500). Set a dedicated `TELEGRAM_WEBHOOK_SECRET` so a leak of `HELIX_AUTH_TOKEN` from any other agent does not also give control of the bot. Without it, the webhook falls back to `HELIX_AUTH_TOKEN`.
- **Sender check.** Only a non-bot user whose ID and chat ID both equal `TELEGRAM_CHAT_ID` is served, which limits it to your private chat.
- **Replay protection.** Updates must carry a `date` within `TELEGRAM_MAX_COMMAND_AGE_SECONDS` (default 15 min), and each `update_id` is processed once.
- **Size limits.** Bodies over 64 KB and command text over 512 characters are rejected.
- **Rate limits.** 20 commands per minute overall, and `/execute` and `/optimize` once per minute each, since they dispatch work and spend vendor credits.
- **Output hygiene.** Messages sent to Telegram are HTML-escaped, truncated, and scrubbed of configured secret values and bot-token URLs.
- **Input validation.** Emails, amounts and proposal IDs are strictly parsed before any write.

Not covered: Telegram's source IPs are not allow-listed, because behind Render's proxy they would have to be read from a spoofable header. The secret token is the control.

### Notifications

`ExecutiveNotificationEvent`s are routed by severity to a summary or critical channel. Delivery is rate-limited per minute, de-duplicated by fingerprint, and critical events have a repeat cooldown. Summaries are generated from logs in two ET windows:

- **Overnight Summary**: 19:00 to 06:00 (sent at 06:00)
- **Daytime Summary**: 06:00 to 19:00 (sent at 19:00)

## Outcome ledger and strategy review

Helix learns from results instead of only rotating through industries.

**Ledger.** Every reply, booked meeting, closed deal (with amount), bounce, opt-out and inbound blueprint is written to `helix_outcome_events`. Sources: Vanguard status updates (automatic), Telegram commands, `POST /api/v1/outcomes`, and the blueprint endpoint. Region and campaign strategy are copied from the industry's scheduler row at the time of the event, so attribution is approximate if those change later.

**Weekly review.** From Monday 07:00 ET, one review runs per week, and it catches up if the host was asleep at that time. It builds a scorecard over the last 28 days and sends it to Telegram with numbered proposals:

| Proposal | Trigger (needs at least `HELIX_STRATEGY_MIN_LEADS` leads) |
| --- | --- |
| Priority +10 | Active, has results, and yield per 100 leads is above the median of eligible industries. |
| Priority -10 | Bounce plus opt-out rate above `HELIX_STRATEGY_MAX_NEGATIVE_RATE`. |
| Priority -5 | No replies, meetings or deals, priority above 1. |
| Pause | No results from 3 times the minimum lead sample. |
| Resume | Paused, but produced results in the window. |

Points weight a reply 15, a meeting 40 and a deal 100, matching the live priority bumps. Rates use leads harvested as the denominator, because send counts live in other services. Proposals are rule-based, never applied automatically, and expire after 14 days.

**Briefs for Chiron (off by default).** With `HELIX_BRIEFS_ENABLED=true`, the same review also looks at the whole funnel and may propose a *brief*: a written goal sent to Chiron, which changes the agent's code and opens a pull request. Nothing is sent until you `/approve` it, and Chiron never merges.

| Weak result (28 days) | Brief goes to | Rule |
| --- | --- | --- |
| Fewer than `HELIX_BRIEFS_MIN_HARVEST` leads (100) | `nova` | Raise leads harvested. |
| Bounce plus opt-out rate above the negative limit | `nova` | Improve lead quality. |
| Replies per lead below `HELIX_BRIEFS_MIN_REPLY_RATE` (2%) | `vanguard` | Improve email copy. |
| At least 5 replies and no meetings | `vanguard` | Improve the call to action. |
| Fewer than `HELIX_BRIEFS_MIN_BLUEPRINTS` blueprints (1) | `beacon` | Improve inbound conversion. |

Each brief states the goal, the evidence, where to look, the constraints (prompts and logic only; no auth, secrets, sending limits or unsubscribe handling), and tells Chiron to make no change if nothing is likely to help. A brief is skipped when the agent is inactive, inside the Chiron cooldown (`CHIRON_OPTIMIZATION_COOLDOWN_HOURS`), already queued, or its last pull request is still open. The open-PR check and the "since merge" counts in the weekly message need `GITHUB_TOKEN` (read-only is enough); without it the state shows as unknown. Attribution is coarse: the ledger does not record which template produced a reply, so briefs ask for experiments, not certainties, and results after a merge are noisy at low volume.

If Chiron cannot be reached (it may be waking), `/approve` reports the failure and the proposal stays pending, so you can retry.

Approved priority changes only steer work when scheduler selection puts priority first. Set `HELIX_PRIORITY_FIRST_ORDERING=true` for that, knowing it changes how Nova rotates between industries.

## Background workers

**Which workers start.** By default only the weekly strategy review starts. The others exist for when the downstream agents are running again, and are opt-in through `HELIX_ENABLED_WORKERS`, a comma list of: `awareness` (Nova dispatch and the 04:30 preflight), `auditor` (agent KPIs and Chiron dispatch), `credit-sync` (vendor credits, which also runs once at boot), `optimizer` (hourly prompt optimizer), `summary` (6:00 and 19:00 digests), `watchdog` (5-minute heartbeat and Nova health probe), and `strategy`. Use `all` to start everything. `GET /api/v1/health` reports a disabled worker as `disabled`, and stays `healthy` if every enabled worker is running. Helix sends no Telegram message at startup, and a weekly review on an empty database sends nothing (it still records that it ran).


Started in `lifespan` in `main.py`. If any exits, Helix writes a `CRITICAL` command-center log and an executive notification. All are cancelled on shutdown.

| Task name | Function | Cadence | Purpose |
| --- | --- | --- | --- |
| `awareness-dispatcher` | `perpetual_awareness_loop` | continuous ticks | Heals the MoltSet circuit, dispatches Nova every 30 min within its daily record budget, runs the 04:30 ET preflight audit, and fires time-locked agents from `helix_agent_schedule` (per weekday and `scheduled_time`). |
| `performance-auditor` | `run_agent_performance_audit_loop` | 30 min | Log retention, then KPI audit and Chiron dispatch. |
| `vendor-credit-sync` | `credit_sync_scheduler_loop` | 8h ± 10 min | Refresh PhantomBuster and MyEmailVerifier credits. |
| `prompt-optimizer` | `reactive_optimizer_loop` | hourly | Optimize prompts if 5 or more warnings/errors landed in the last 3 hours. |
| `executive-summary-scheduler` | `scheduled_summary_loop` | 06:00 and 19:00 ET | Executive summaries. |
| `observability-watchdog` | `observability_watchdog_loop` | 5 min | Detect stale Nova runs and pipeline stalls. |
| `strategy-review` | `strategy_review_loop` | checks every 30 min | Weekly scorecard and proposals, with catch-up after a sleep. |

### Agent KPI thresholds

`core/auditor.py` evaluates these over each agent's lookback window; a miss triggers Chiron, subject to the cooldown.

| Agent | Thresholds | Lookback |
| --- | --- | --- |
| beacon | avg impressions ≥ 150, link clicks ≥ 5, posts ≥ 1 | 7 days |
| vanguard | open rate ≥ 35%, bounce rate ≤ 3% | 7 days |
| nova | harvest yield per run ≥ 10 | 3 days |
| radar | processed rate ≥ 0.10 | 3 days |
| pulsar | DMs per batch ≥ 1 | 3 days |
| nexus | records per run ≥ 1 | 3 days |

Agents with missing KPI data are marked `INSUFFICIENT_DATA_WITH_GAPS` rather than failed.

## Vendor circuit breakers

The ledger (`helix_infrastructure_oracle_ledger`) holds credits per vendor and an `is_circuit_broken` flag. `load_vendor_isolation_matrix` turns it into a dict that `trigger_downstream_mesh_with_payload` consults before each dispatch:

- **Anthropic** broken: Nexus dispatch and the prompt optimizer are blocked.
- **MoltSet**: Nova harvesting is paused. A probe checks weekly capacity and heals the circuit, with a 30 minute recovery cooldown.
- **PhantomBusters** and **MyEmailVerifier**: synced from vendor APIs. A missing API key or failed sync **fails closed** (circuit broken).

Nova's daily record budget is derived from MoltSet's weekly record allowance minus the reserve ratio, less what was consumed today; when a run would exceed it, it is deferred and a (cooled-down) alert is sent.

PhantomBuster zero-result handling steps the keyword cap down the staircase (25, 12, 8, 5) after repeated empty results, with a 60 minute tuning cooldown.

## Scheduling rules

- All schedule logic uses `America/New_York`.
- Workers operate Monday to Friday, 06:00 to 22:00 ET (`should_helix_workers_operate`); Saturday and Sunday are standby.
- Each industry in `helix_brain_scheduler` can be limited to specific `assigned_days`; with no eligible target, dispatch is skipped and logged as a standdown.

## Memory and prompt optimization

- **Semantic memory** (`helix_memory.py`): `record_experience` embeds an outcome with OpenAI and stores it in `helix_semantic_memory`; `query_learned_guidelines` retrieves up to 10 guidelines for the agent (or `global`), ranking exact industry matches first, then by embedding distance (`<=>`). Negative outcomes: `high_bounce`, `opt_out`, `failure`, `unresponsive`. Mesh dispatch injects the top results into each agent's payload.
- **Prompt optimizer** (`helix_optimizer.py`): reads `helix_agent_prompts` and the last 24h of warnings/errors, asks Claude for a revision wrapped in `<prompt>` tags, and updates the row. It aborts if the Anthropic circuit is broken, and re-checks between prompts.

## Testing

```bash
pytest
```

The suite uses stub DB pools and mocked HTTP, so it needs no database or network. Coverage includes API endpoints, auditor logic, config, DB connection validation, core engine, mesh client, MoltSet recovery, Nova dispatch, optimizer, Telegram orchestrator, and telemetry.

## Running on Render free tier

Helix is designed to run as one free Render web service with Neon Postgres. As I understand the free tier (check Render's current docs): the service spins down after about 15 minutes without inbound requests, takes about a minute to wake, has no persistent disk, and cron jobs are paid.

- All state is in Postgres. Nothing writes local files.
- The weekly review checks the database for a missing run marker, so it still runs after a sleep.
- Telegram commands wake the service. Telegram retries a delivery that times out during the wake-up, and duplicates are ignored.
- **Keep it awake with cron-job.org, but only 04:00 to 22:00 ET.** Create a job that sends `HEAD https://<your-host>/` every 5 minutes, with an execution window of 04:00 to 22:00 America/New_York. That is about 560 hours a month, which covers the 04:30 preflight audit, Nova's day and the Monday 07:00 review, and leaves room in the shared monthly allowance for Chiron, which has no pinger and is woken by Helix on demand. The first ping after a sleep can exceed cron-job.org's timeout while the service wakes; that failed run still wakes it, so ignore it. If it sleeps, the other scheduled work stops too: Nova dispatch every 30 minutes, the 04:30 ET preflight audit, and the performance auditor. Only the weekly review catches up.
- Helix awake 24/7 would use nearly the whole monthly allowance (about 744 of 750 hours, to be checked in the Render dashboard), so a second always-on free service would not fit. That is why the pinger has a window.
- Chiron sleeps when idle. Helix gives it up to 90 seconds, with one retry, to wake when dispatching.

## Deployment notes

- Deployed on Render as a web service (the other agents are separate Render services; default URLs are `*.onrender.com`), with Neon Postgres.
- The pool is sized `min 2 / max 20`, with a 30s checkout timeout, 30 min max lifetime, and 270s max idle, to stay inside cloud connection limits.
- Run one instance only: the background workers keep in-process state (notification dedupe, PhantomBuster policy, preflight date) and would duplicate dispatches if scaled out.
- Set the same `HELIX_AUTH_TOKEN` on Helix and every downstream agent.

## Known gaps

1. **Inconsistent auth headers.** Some endpoints use `x-helix-auth-token`, others `x-inbound-key`, and lead status uses `x-vanguard-key`.
2. **Duplicated `get_validated_database_url`** in `database/connection.py` and `init_db.py`, with slightly different behavior (the latter adds timeouts).
3. **Default model mismatch.** `config/settings.py` defaults `ANTHROPIC_MODEL` to `claude-3-5-sonnet-latest`, while the optimizer defaults to `claude-haiku-4-5`.
4. **Aura proxy drift.** Aura forwards logs to `/api/v1/helix/log`, but Helix ingests at `/api/v1/logs` (noted in the architecture doc).
5. **Reconstructed schema.** The DDL for `helix_agent_schedule`, `helix_agent_manifests`, `helix_semantic_memory`, `helix_agent_prompts` and the Chiron columns of `chiron_healing_sessions` was written from how the code uses them, not from the original database. Other services may use more columns.
6. **Nova budget-deferral alerts are not sent to Telegram by default.** They are `warning` severity, and the default `HELIX_NOTIFICATION_POLICY_MODE=strict` suppresses non-critical, non-command events, so they only reach the summaries. Set the mode to `legacy` if you want them delivered immediately.
