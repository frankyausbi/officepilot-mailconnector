# OfficePilot Mail Collector

Central multi-tenant IMAP collector for KI OfficePilot.

## Environment
Copy `.env.example` to `.env` and set the exact same `OFFICEPILOT_COLLECTOR_SECRET` configured in OfficePilot. Never commit `.env`.

## Run
`docker compose up -d --build`

## Logs
`docker compose logs -f --tail=100`

## Notes
- Polls active OfficePilot mailboxes.
- Uses IMAP UID + UIDVALIDITY checkpoints.
- Sends attachment metadata only, never attachment binary data.
- Credentials and message bodies are not logged.
- 401 from OfficePilot stops the collector to avoid repeated requests with a bad secret.


## v1.1.0
Adds OfficePilot outbound SMTP queue processing with claim/sent/failed callbacks, threading headers, retry classification and best-effort duplicate recovery via the IMAP Sent folder. SMTP credentials are fetched only for a claimed job and are never logged.


## Version 1.1.1 – OfficePilot monitoring heartbeat

Every 60 seconds, the collector independently POSTs to `/api/public/collector/heartbeat`
using the existing `OFFICEPILOT_COLLECTOR_SECRET`. The collector ID defaults to
`hostinger-vps-1` and may be overridden with `COLLECTOR_ID` in `.env`.
Metrics are process-local and reset on restart; `active_mailboxes` is the most
recent number returned by the mailbox listing, `smtp_pending` is the most recent
outbox listing length, and cycle counters count completed/failed top-level cycles
(not individual mailbox errors). Monitoring failures do not interrupt mail processing.

Deploy: `git pull origin main && docker compose up -d --build`.
Check: `docker compose logs --tail=40` and OfficePilot Superadmin > Systemüberwachung.
