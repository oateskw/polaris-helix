# Helix growth plan: 3 paying venue customers in 90 days

## Context
Helix is the "boss" agent whose one job is to build Polaris Innovations into a business with many customers. So far it can score results, propose changes and brief Chiron, but it has no plan of its own and nothing is feeding it. This plan answers three questions: what is the plan, how does inbound marketing fit, and which tools are needed.

Decisions from the owner: 90-day goal is **3 paying venue customers**; tool budget is **flexible if it works** (start small, scale on results); outbound may use a **disclosed test-inquiry audit hook**. Wedding businesses are the focus, venues first. Today is 2026-10-25.

## What exists today (found by reading the site and repos)
The website (`polaris_website/polaris-web`, Next.js on Vercel, live at polarisinnovationsai.com) is further along than the agents:
- **A sharp offer.** `/venues`: "Reply faster. Follow up with every couple worth following up with." An inquiry and follow-up system (replies, follow-ups at 2, 5 and 15 days, tour reminders, staff app, per-email Off/Review/Automatic control). Pricing is a one-time setup plus a monthly fee, first month free after go-live.
- **One real proof point.** The Manor at Airmont (Northern Virginia). Baseline: median first reply 7 hours, 43% within 4 hours. Live since **2026-10-16**; before-and-after results are promised after 60 to 90 days (**about 2026-12-15 to 2027-01-14**).
- **Lead magnets and capture.** Prism "Waste Scorecard" at `/scoping`, blueprint generator (gpt-4o-mini), savings calculator, Echo chat widget, free inquiry audit by email, cal.com booking.
- **Follow-up machinery.** A cron route (`/api/shadow/cron`) takes abandoned scorecards, writes recovery email copy, pushes the lead to Instantly, and can escalate to LinkedIn through Beacon.
- **Admin dashboards** for stats and Instantly stats.

## What is broken or missing (this is the real critical path)
1. **The site is probably capturing nothing right now.** Its routes insert into `cold_leads`, `lead_suppression`, `vanguard_sent_history`, `inbound_blueprints` and `segmented_telemetry` through `VANGUARD_DATABASE_URL` / `DATABASE_URL`. The old Neon project was dropped. The rebuilt database only has Helix's reconstructed schema: its `cold_leads` lacks `processed_at`, `personalized_subject`, `personalized_copy`, `shadow_suppression_active`, `utm_*` and more, and the other four tables do not exist. The routes catch errors and only log them, so failures are silent. The Vercel environment variables almost certainly still point at the old database.
2. **The site never tells Helix anything useful.** Nothing calls Helix's `/api/v1/inbound-blueprints`, so the `blueprint_submitted` metric is empty. cal.com bookings, audit-request emails and chat leads never reach `/api/v1/outcomes`, so the scorecard cannot see meetings or deals.
3. **No analytics.** Without visitor data there is no top of the funnel to optimize.
4. **Conversion gaps on `/venues`.** Mixed CTAs ("free inquiry audit", "Start Free Blueprint", "Get Custom Blueprint"), no on-page form (email or leave), no pricing range, no sample emails or demo, no testimonial.
5. **Generic home page.** "Custom AI Automation for Any Business" competes with everyone. The venue page is the strong asset.
6. **No content engine.** No blog or resources, so nothing for search to find.
7. **Sending is not ready.** Cold email needs a separate warmed domain (takes weeks), a real unsubscribe path and a physical address (CAN-SPAM). Not verified in Vanguard.

## The strategy in one paragraph
Be the specialist: **"the inquiry follow-up system for wedding venues"**, proven by The Manor, starting in Northern Virginia, DC and Maryland so proof is local and calls can be in person. Use two channels that feed one booking funnel: **outbound** (targeted, small, personalized) for speed, and **inbound** (audit tool, benchmark content, the case study) for compounding. Helix runs a weekly loop and moves effort toward what produces booked calls. Planners, photographers, caterers and florists wait until venues convert.

## The loop Helix runs
1. **Find** venues (Nova, MoltSet). 2. **Reach** them (Vanguard through Instantly; Beacon on LinkedIn). 3. **Convert** (site: audit, scorecard, call). 4. **Prove** (Manor results, then each new customer). 5. **Learn**: weekly scorecard against the goal, proposals the owner approves, briefs to Chiron.

## Funnel math (rough benchmarks, not verified; Helix replaces them with real data after two weeks)
Assume cold email gets about 3% replies, a third of those positive, a third of positives book a call, and about 25% of calls close. Then 3 customers needs about 12 calls, about 36 positive replies, about 100 replies, and roughly **3,000 contacts** over 8 to 10 weeks: **about 300 new venues contacted per week** from week 3, plus whatever inbound adds. If real rates are better, volume drops.

## Timeline
| Weeks | Focus | Outcome |
| --- | --- | --- |
| 1-2 | **Phase 0: repair the foundation.** Start domain warmup the same day. | Site captures leads; Helix sees every stage. |
| 2-4 | **Phase 1: outbound ready.** Venue list, audit hook, compliance, first 50 test sends. | First replies; real rates. |
| 3-10 | **Phase 2: outbound at ~300 a week**, inbound content weekly. | Booked calls. |
| 6-12 | **Phase 3: learning loop and proof.** Update the site with Manor results when they land (mid-Dec to mid-Jan). | Closing, case study v2. |

## Phase 0: repair the foundation (first, blocks everything)
Owner actions (outside the repo):
- In Vercel, set the site's `DATABASE_URL` and `VANGUARD_DATABASE_URL` to the new `polaris` database, and redeploy.
- Start a sending domain and Instantly warmup now; it is the longest lead time.

Code actions:
- **Helix `database/schemas.py`**: add the website's tables and columns as `IF NOT EXISTS` / `ADD COLUMN IF NOT EXISTS`: the extra `cold_leads` columns the site and Vanguard use, `lead_suppression`, `vanguard_sent_history`, `inbound_blueprints`, `segmented_telemetry`. First read each repo's actual queries (`polaris-web/app/api/**`, `polaris-vanguard/*.py`) to get exact columns and types rather than guessing. Update `export_seed`/README accordingly.
- **Site to Helix wiring** (`polaris-web`): after a scorecard or blueprint submission call Helix `POST /api/v1/inbound-blueprints`; add a cal.com webhook route that posts `meeting_booked` to `POST /api/v1/outcomes`; log audit requests as a new ledger stage (or reuse `reply`) through the same endpoint. Use `x-helix-auth-token`, which is what Helix checks. Note the site currently sends `Authorization: Bearer` to a logs URL; confirm which header `/api/v1/logs` accepts.
- **Analytics**: add privacy-friendly page analytics (Vercel Analytics is already on the platform; Plausible or PostHog are alternatives) plus UTM capture.

## Phase 1: outbound readiness
- **Targeting.** Change the venue industry rows to Virginia, DC and Maryland first (`target_region` in `seed_data/helix_brain_scheduler.json`, currently "United States"), then re-run `init_db.py`. Keep the other five wedding industries loaded but paused until venues convert.
- **Hook.** Disclosed test-inquiry audit: send one inquiry, time the reply, and say so in the first email ("I sent your venue a test inquiry on Tuesday at 10:12; the reply came Thursday at 4:40. Here is what that costs you."). This needs a small, supervised process and a tracking sheet or table, not automation at first. Decide the inquiry text and the sending address; keep volume low.
- **Compliance check** of Vanguard: unsubscribe in every email, physical address, suppression list honored (`lead_suppression`), no purchased lists of personal addresses outside business context. Fix gaps before sending.
- **Reply handling.** Vanguard's reply webhook already classifies replies and posts status to Helix; confirm it writes `reply`/`positive_reply`/`meeting_booked` to the ledger.
- **First batch**: 50 venues, one email plus two follow-ups, measure replies before scaling.

## Phase 2: inbound engine (compounding, runs beside outbound)
- **Fix `/venues`** (owner copy, then Chiron/owner edits to the Next.js site): one primary CTA; an on-page "get my free inquiry audit" form that writes to the database and Helix; a price range or "from" figure; a sample reply email and a screenshot or short demo of the staff app; Manor numbers as they arrive.
- **Content** (weekly, drafted by Beacon, approved by the owner): "How fast should a venue reply to a wedding inquiry?", reply-time benchmarks, a follow-up cadence template, The Knot/WeddingWire inquiry handling. Add a `/resources` section to the site. Distribute on LinkedIn through Beacon/Publer.
- **Lead magnet**: turn the audit into a self-serve "inquiry reply-time check" the venue can request, which feeds the same funnel.
- **Partner channel**: planners and photographers who see many venues; one referral conversation per week.

## Phase 3: Helix learns and reports (the "boss" part)
- **Goal tracking.** Add `HELIX_GOAL_CUSTOMERS` / target date to Helix config; the weekly message shows plan vs actual per stage: contacts, replies, audit requests, calls, proposals, customers.
- **Funnel by channel.** Extend `compute_funnel` (`services/briefs.py`) and the scorecard to split by source (`outbound`, `linkedin`, `site`, `referral`) using UTM and `helix_outcome_events.source`.
- **Weekly experiments.** New proposal kind: one experiment per week (a subject line, an opening line, a CTA) with a hypothesis and a measurement window; the owner approves in Telegram, as today. Briefs to Chiron stay for code changes inside the Python agents (Chiron edits `.py` files only, so site edits are manual or a separate step).
- **Proof loop.** When a customer goes live, Helix prompts the owner for the baseline measurement and later the result, so each customer becomes the next case study.

## Tools: have, missing
| Need | Tool | Status |
| --- | --- | --- |
| Lead data | MoltSet (Nova) | Exists, paid. Region filter needed. |
| Email verification | MyEmailVerifier | Exists. |
| Sending and warmup | Instantly + a separate sending domain | Instantly exists; domain and warmup missing. |
| Reply classification | Vanguard (Anthropic) | Exists; confirm ledger writes. |
| Booking | cal.com | Exists; webhook to Helix missing. |
| Lead magnets | Prism scorecard, blueprint, calculator, audit | Exist; not wired to Helix. |
| Site analytics | Vercel Analytics, Plausible or PostHog | Missing. |
| Content distribution | Beacon + Publer (LinkedIn) | Exists; content plan missing. |
| CRM and pipeline | Helix ledger plus the Neon tables | Light; enough for 3 customers. Add a proposals/deals stage later. |
| Reporting | Telegram scorecard | Exists; goal tracking missing. |
| Social proof | Manor case study, testimonials | One case; results due mid-Dec to mid-Jan. |

Cost note (unverified, check current pricing): a sending domain, Instantly and lead credits are the main spend. Budget is "flexible if it works"; start with the smallest Instantly plan and one domain.

## Free-tier and Neon fit
Everything new here is event-driven (webhooks and the existing weekly review); no new polling workers. Nova and Vanguard cannot run as extra always-on services within the shared free hours, so they must be woken by Helix or run on demand. The first outbound volume (about 300 a week) is easily handled by on-demand runs.

## Files to change in this repo (Phase 0 and 3 code)
- Modify: `database/schemas.py` (shared website tables and columns), `main.py` (accept new ledger sources/stages if needed), `services/briefs.py`, `services/strategy.py` (goal tracking, channel split), `seed_data/helix_brain_scheduler.json` (regions), `README.md`, `.env.example`.
- Create: `tests/` for each new function; `docs/growth-plan.md` holding this plan in the repo.
- In `polaris_website/polaris-web` (separate repo): new webhook route for cal.com, calls to Helix from the scorecard and blueprint routes, analytics, `/venues` and `/resources` changes. Planned as separate PRs.

## Verification
- Phase 0: submit a test scorecard on the live site with an obviously fake address; confirm a `cold_leads` row appears in the new database, and a `blueprint_submitted` row appears in `helix_outcome_events`; create a cal.com test booking and confirm `meeting_booked`. Delete the test rows afterwards.
- `.venv/Scripts/python -m pytest -q` for all Helix changes; `python init_db.py` twice must change nothing on the second run.
- Phase 1: first 50 sends: deliverability (bounce under 3%), unsubscribe works, replies land in the ledger.
- Weekly: `/scorecard` shows plan vs actual; the owner can tell, at each Monday review, which stage is the bottleneck.

## Risks and decisions for the owner
- **Cold email deliverability and legality.** A cold domain, honest copy, unsubscribe, and low volume first. A disclosed test inquiry should be short and polite; keep it to venues you will email anyway.
- **Test-inquiry ethics.** It consumes a venue's time. Keep the first batch small and disclose it in the first email, as chosen.
- **One proof case.** The Manor is the only evidence, and results are not in yet. If the 60 to 90 day numbers are weak, the pitch needs to change; Helix will say so rather than spin it.
- **The site may be losing leads today.** Fixing the Vercel database variables is the single most urgent action, and it does not depend on any code here.
- **Attribution is approximate** until UTM and source tracking are in place.
