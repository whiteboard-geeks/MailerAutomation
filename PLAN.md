# USPS-Direct Labels — Migration Plan

**Goal:** Replace Pirate Ship with a 100% API-driven mailer workflow: buy USPS labels
programmatically under WBG's own Mailer ID, receive delivery scans via USPS's free
Tracking Webhook, and retire the EasyPost tracking layer (whose silent polling
failures caused the wave 53 and waves 60/61 incidents).

**Why USPS-direct** (decided 2026-07-20): owning the MID in the package barcode is
what makes USPS tracking free (API + webhook). EasyPost labels ($0.08/label + 3%
postage surcharge from June 2026) and Shippo ($0.05/label) ship under *their* MID,
keeping tracking third-party-polled — the exact failure class we're escaping. USPS
webhook access for labels bought through a third party (Pirate Ship) is a ~$599/mo
"service provider" tier since USPS's April 2026 access-control change.

## Phase 0 — Onboarding (no code; Lance + USPS bureaucracy)

- [ ] USPS Business Account for WBG → Customer Onboarding Portal (COP)
      ⏸ 2026-07-20: PAUSED at account-type screen (reg.usps.com, appId=GSS,
      business) — waiting on correct WBG mailing address before registering,
      since business name + address become the CRID. Lance creates the account
      himself (credentials); Claude resumes for post-login COP steps.
- [ ] CRID + Mailer ID issued (MID approval: same day–2 business days)
- [ ] Enterprise Payment Account (EPA) linked to bank; verified via two micro-debits
- [ ] Enroll in USPS Ship (required for Labels API)
- [ ] API app created in COP → consumer key/secret (OAuth client credentials)
- [ ] Store credentials in Infisical (WBG instance), not .env
- [ ] Smoke test: OAuth token → `GET /tracking/v3/tracking/<code>` on a live wave-62+
      tracking number (also settles whether polling works for Pirate Ship-MID labels)
- [ ] Rate check: price our actual mailer spec (Ground Advantage, real weight/dims)
      via Prices API; compare against what Pirate Ship charges today

## Phase 1 — Label purchase workflow

- [ ] `CreateWaveLabelsWorkflow` (Temporal): input = wave identifier
      - Query Close for the wave's leads + addresses
      - Payments API token (valid 8h; fetch per-run)
      - `POST /labels/v3/label` per lead → tracking number + label PDF
      - Write tracking number to Close lead (existing custom field)
      - Merge label PDFs into one batch file for printing
- [ ] Address validation via USPS Addresses API (replaces Pirate Ship's check;
      feeds "Mailer Address Check" field)
- [ ] Refund path: `DELETE /labels/v3/label/<trackingNumber>` for misprints/cancels
- [ ] Decide label output format with Barbara (4×6 thermal vs sheet PDF) — OPEN
- [ ] Decide trigger UX: how a wave kicks off label creation (endpoint? script?) — OPEN

## Phase 2 — Tracking webhook

- [ ] Flask route `/usps/tracking_webhook` (listener URL per Subscriptions –
      Tracking 3.2 spec), auth-verified
- [ ] Subscription by MID (covers every package automatically; no per-tracker step)
- [ ] Delivered events → same two writes as today (reuse
      `update_delivery_information_for_lead` +
      `create_package_delivered_custom_activity_in_close` via Temporal workflow)
- [ ] Nightly watchdog: free Tracking API poll for leads undelivered after N days;
      digest email on anything it heals (belt & suspenders, $0)

## Phase 3 — Parallel wave + cutover

- [ ] Run one full wave through the new path alongside Pirate Ship expectations:
      compare postage cost, print quality, delivery detection latency/completeness
- [ ] Cut over label buying; stop creating EasyPost trackers for new waves
- [ ] After last EasyPost-tracked wave resolves: remove `/easypost/*` routes,
      tracker-creation workflow, and EasyPost keys; update CLAUDE.md ops docs

## Open questions

- International mailers ever? (Different API + customs; assumed domestic-only)
- Sequencing vs the Onspring fork — build here first, fork inherits
- What does Barbara's printing hardware expect?

## Status log

- 2026-07-20: Plan created. Waves 60/61 EasyPost freeze diagnosed (62 trackers
  dropped from polling mid-transit); support ticket sent to EasyPost from
  lance@whiteboardgeeks.com. Awaiting Pirate Ship xlsx for backfill of the
  frozen leads (`python -m scripts.backfill_wave_delivered <xlsx>`).
