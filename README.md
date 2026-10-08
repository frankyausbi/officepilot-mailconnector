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
