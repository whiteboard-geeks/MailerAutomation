# MailerAutomation — Operations & Troubleshooting

## Where things run

- **Production:** Whiteboard Geeks Hetzner box `wbg-apps` (moved off Heroku 2026-11-16). Webhooks hit `https://app.whiteboardgeeks.com/mailer/<route>` (Caddy strips `/mailer`, proxies to gunicorn on 127.0.0.1:8781). systemd units `mailer-automation-web` and `mailer-automation-worker`; code in `/opt/mailer-automation/current` (a link into `releases/<sha>`); settings in `/etc/mailer-automation/env` (root-only); Redis is the box's local `redis-server`, db 7.
- **Staging:** same box, `https://app.whiteboardgeeks.com/mailer-staging/`, port 8782, units `mailer-automation-staging-web|worker`, `/opt/mailer-automation-staging`, `/etc/mailer-automation/staging.env`, Redis db 8.
- **Deploys:** push to `main` → GitHub Actions → `ssh ma-deploy@app.whiteboardgeeks.com "<staging|production> <sha> [test]"`, which runs `/usr/local/sbin/mailer-automation-deploy` (build, restart, health check, automatic rollback). Manual deploy or rollback: `ssh wbg-apps sudo mailer-automation-deploy production <sha>`.
- **Health check:** `mailer-automation-health.timer` every 5 min; emails Lance when it goes down and when it recovers.
- **Temporal (self-hosted):** Whiteboard Geeks Hetzner `wbg-apps`, namespace `mailerautomation-prod`, mTLS gRPC `app.whiteboardgeeks.com:7233`, SSO UI https://app.whiteboardgeeks.com/temporal/namespaces/mailerautomation-prod/workflows
- The Flask web dynos enqueue new workflows on self-hosted Temporal; the `worker_temporal` dyno runs their activities. Temporal Cloud was retired on August 3, 2026.

## Logs & where to look first

| Symptom | First place to look |
|---|---|
| Webhook not triggering anything downstream | `ssh wbg-apps sudo journalctl -u mailer-automation-web --since -2h \| grep <route>` — confirms the request hit Flask and was/wasn't enqueued |
| Workflow ran but produced wrong output | Self-hosted Temporal UI → search by workflow id and view event history. Pre-cutover Cloud history is available only in the read-only archive documented in `infra/temporal/README.md`. |
| "Did the worker actually pick this up?" | Temporal UI status. `RUNNING` for >1 min usually means an activity is failing or waiting on `_data_issue_fixed` signal. |
| Worker / activity errors | `ssh wbg-apps sudo journalctl -u mailer-automation-worker --since -2h` |
| Redis-related | `ssh wbg-apps sudo redis-cli -n 7` (staging: `-n 8`) |

journald on wbg-apps keeps up to 1 GB of logs (weeks). Logs from before 2026-11-16 were in Heroku's Papertrail addon.

## Temporal CLI

The local `prod` and `staging` environments at `~/.config/temporalio/temporal.yaml` use the self-hosted mTLS endpoint. Server operations, backups, and the retired Cloud archive are documented in `infra/temporal/README.md`. Useful one-liners:

```sh
temporal --env prod workflow list --limit 20 --query 'WorkflowType="WebhookDeliveryStatusWorkflow"'
temporal --env prod workflow show -w <workflow-id> --output json
temporal --env prod workflow count --query 'WorkflowType="WebhookDeliveryStatusWorkflow"'
```

Workflow IDs are random UUIDs (g_run_id), not tracking codes — to find a workflow for a specific tracking number, you usually have to inspect the workflow input via `workflow show` or grep the journald logs for the request_id.

## EasyPost gotchas

- The `/easypost/delivery_status` webhook fires for **every** tracker state change. The Flask handler short-circuits with `200 OK` and "Tracking status is not 'delivered'" unless `result.status == "delivered"`. Lots of "not delivered" lines in logs is normal.
- **EasyPost stops getting USPS updates if a tracker sits in `pre_transit` too long without movement** (last seen: wave 53, 4/27 → 4/29 then frozen forever). Symptom: `tracker.updated_at` frozen, `status` stuck at `pre_transit`, USPS later actually delivers but EasyPost never sees it. Neither `GET /v2/trackers/<id>` nor `POST /v2/trackers` with the same tracking_code refreshes; only EasyPost support can re-engage polling for those trackers (or recreate trackers under a different code, which isn't an option since USPS owns the tracking number).
- To check directly: hit `https://api.easypost.com/v2/trackers?tracking_code=<code>` with the prod API key — look at `updated_at` vs the last `tracking_details` datetime.

## One-time backfill from Pirate Ship

When EasyPost has stopped polling but Pirate Ship has the delivery scans, use:

```sh
# dry-run (default)
python -m scripts.backfill_wave_delivered <pirateship.xlsx>
# write
python -m scripts.backfill_wave_delivered <pirateship.xlsx> --apply
```

Idempotent: re-running won't create duplicate "Mailer Delivered" custom activities (the Close create call checks for an existing one first). The script lives at `scripts/backfill_wave_delivered.py`.

## Sender / recipient automation flow (high level)

1. Close lead created with tracking number → external system POSTs to `/easypost/create_tracker` → `WebhookCreateTrackerWorkflow` runs → EasyPost tracker created, tracker id stored on Close lead
2. EasyPost polls USPS, fires `tracker.updated` webhooks to `/easypost/delivery_status`
3. On `status=delivered`: `WebhookDeliveryStatusWorkflow` runs two activities:
   a. `update_delivery_info_for_lead_activity` — searches Close lead by tracking number, writes 7 delivery custom fields
   b. `create_package_delivered_custom_activity_in_close_activity` — creates a "Mailer Delivered" custom activity (Close auto-enrollment workflows trigger off this)

## Email error reporting

Activity failures send HTML emails on the **last** Temporal retry attempt only (see `temporal/shared.py::is_last_attempt`). Recipients live in `utils/email.py`.
