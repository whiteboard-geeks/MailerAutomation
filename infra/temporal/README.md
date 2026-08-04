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
- Hetzner automated backups enabled, plus manual cutover snapshots

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

Hetzner automated backups are enabled for host-level recovery. Manual cutover
snapshots are tagged `purpose=temporal-cutover`. Keep an off-host copy of any
archive that must survive loss of the Hetzner account.

## Retired Temporal Cloud archive

Temporal Cloud was fully retired on August 3, 2026. Temporal cannot import an
existing Workflow event history as a live execution on another cluster, so the
pre-cutover histories are preserved as a read-only audit and SDK replay archive.

The verified archive is stored in two places:

- Server: `/opt/temporal/legacy-cloud-archive/temporal-cloud-archive-20260803`
- Off-host workstation copy:
  `~/Backups/Temporal/temporal-cloud-archive-20260803.tar.gz`

Archive coverage:

- Production visibility metadata: 7,076 Workflows, including all 96 that were
  still open at retirement
- Staging visibility metadata: 102 Workflows, including all 44 that were still
  open and both failed executions
- Raw event histories: 448 selected executions (all open, all retained failed,
  250 recent completed production, and all retained completed staging)
- `SHA256SUMS` verifies every archive file

All Temporal Cloud namespaces and Cloud API credentials were deleted. Heroku
has no `TEMPORAL_LEGACY_*` configuration, and the local `prod` and `staging`
Temporal CLI environments now point to the self-hosted mTLS endpoint.
