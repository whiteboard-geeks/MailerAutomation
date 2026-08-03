# Self-hosted Temporal for MailerAutomation

This stack runs the production and staging Temporal namespaces on the Whiteboard
Geeks Hetzner application server.

## Topology

- Temporal Server `1.31.0`, pinned through `.env`
- PostgreSQL 16 with separate `temporal` and `temporal_visibility` databases
- PostgreSQL advanced Visibility (no Elasticsearch at this workload)
- HAProxy on public TCP `7233`, requiring a client certificate signed by the
  private Temporal CA
- Temporal UI on host loopback `127.0.0.1:8088`, published by Caddy at
  `https://app.whiteboardgeeks.com/temporal/` behind Google SSO
- Daily PostgreSQL logical backups in `/var/backups/temporal`, retained 14 days

The Temporal Server itself is not exposed directly. Heroku and GitHub Actions
connect through the HAProxy mTLS boundary.

## Server installation

The deployed directory is `/opt/temporal`. Secrets are only in:

- `/opt/temporal/.env` (`0600`)
- `/opt/temporal/pki/ca/` (`0700`; CA private key)
- `/opt/temporal/pki/proxy/` (`0700`; HAProxy server material)
- Heroku/GitHub secret stores (client CA, certificate, and key as base64)

Start or update the pinned stack:

```sh
cd /opt/temporal
docker compose pull
docker compose up -d
```

Check it:

```sh
docker compose ps
docker compose logs --since=15m temporal mtls-proxy ui
```

## Schema upgrades

Do not change `TEMPORAL_VERSION` and blindly recreate the server. Read the
Temporal upgrade notes, back up first, then run both schema updates with the
matching `temporalio/admin-tools` image before starting the new server image.
The initial `schema` service is deliberately one-shot and retained after a
successful first run.

## Backups

Run and verify a backup manually:

```sh
systemctl start temporal-backup.service
systemctl status temporal-backup.service
ls -lh /var/backups/temporal
sha256sum -c /var/backups/temporal/sha256-*.txt
```

These are same-host backups. A Hetzner snapshot or off-host copy is still
required for host-level disaster recovery.

## Cloud drain

Temporal does not support moving open Workflow histories between clusters.
During cutover, each Heroku worker polls both clusters:

- `TEMPORAL_*`: self-hosted primary; all new Workflows start here
- `TEMPORAL_LEGACY_*`: old Temporal Cloud namespace; only existing open
  Workflows drain here

Do not remove the legacy variables or delete the Cloud namespaces until their
open Workflow count reaches zero (or the parked Workflows are explicitly
terminated after review).
