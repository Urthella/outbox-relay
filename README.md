# outbox-relay

[![ci](https://github.com/Urthella/outbox-relay/actions/workflows/ci.yml/badge.svg)](https://github.com/Urthella/outbox-relay/actions/workflows/ci.yml)

Writing to your database and publishing an event are two separate steps, so a crash between them loses
the event or sends one for a change that never happened. With the outbox pattern, you write the event
to a table in the same transaction as the change. `outbox-relay` reads that table from Postgres's
replication stream and publishes each committed row to NATS JetStream, with no polling.

```mermaid
flowchart LR
  A[your app] -- "one transaction" --> P[(orders + outbox)]
  P -- "logical replication slot" --> R[outbox-relay]
  R -- "publish, wait for ack" --> N[JetStream]
  N -. "ack moves the slot" .-> R
```

## Run it

The server needs `wal_level=logical`, and the relay's role needs `REPLICATION`.

```bash
go install github.com/Urthella/outbox-relay@latest
psql "$DATABASE_URL" -f schema.sql
DATABASE_URL=postgres://relay:secret@localhost:5432/app outbox-relay
```

In your application, insert into `outbox` in the same transaction as the change:

```sql
BEGIN;
UPDATE orders SET status = 'paid' WHERE id = 42;
INSERT INTO outbox (topic, key, payload)
VALUES ('orders.paid', '42', '{"order_id": 42}');
COMMIT;
```

That event reaches the `OUTBOX` stream on the subject `outbox.orders.paid`, with the headers
`Outbox-Id`, `Outbox-Key` and `Outbox-Topic`. `outbox-relay -help` lists the options.

## What it guarantees

- **Only committed rows go out.** A rolled-back insert is never published, and an open transaction
  publishes nothing until it commits.
- **No event is lost by a crash.** The slot moves only after JetStream has acknowledged everything up to
  that commit. If the relay or the broker dies, the stream resumes from the last confirmed position.
- **Commit order.** Events are published one at a time, in the order the transactions committed.
- **A second relay takes over.** A slot serves one connection at a time, so a standby keeps retrying
  and gets the slot when the first relay goes away.

## What it doesn't guarantee

- **Exactly-once delivery.** A restart sends the events it hadn't confirmed again. JetStream drops a
  repeat within its duplicate window (`-dedup-window`, 10 minutes by default). After a longer outage
  repeats can get through, so consumers should still deduplicate on `Outbox-Id`.
- **That a transaction arrives all at once.** A crash partway through a transaction can leave its first
  events visible to consumers until the relay restarts.

## Before you run this in production

- **A stopped relay keeps WAL.** Postgres holds WAL for a slot until the relay confirms it, so a relay
  that stays down fills the disk. Set `max_slot_wal_keep_size` and alert on slot lag. Past that limit,
  Postgres invalidates the slot and events are lost.
- **The outbox table grows.** The relay doesn't delete rows, so remove old ones on a schedule. Deletes
  aren't published, because the publication only covers inserts.
- **An oversized event stops the relay.** A payload over NATS's `max_payload` (1 MB by default) keeps
  failing, and the relay keeps retrying rather than skip it. Keep events small.
- **Invalid topics.** A topic that can't be part of a NATS subject goes to `outbox._invalid` so it can't
  block the rest. The check constraint in `schema.sql` stops these at insert.

## Tests

The tests use a real Postgres and an embedded NATS server. They cover rollbacks, uncommitted rows,
catching up after a restart, a crash between the broker's ack and confirming the slot, a refused
publish not moving the slot, activity on other tables, invalid topics and a standby taking over.

```bash
nix develop
TEST_DATABASE_URL=postgres://postgres@localhost/postgres go test -race ./...
```

This hasn't been run under production load, and there's no benchmark yet. Kafka isn't supported, though
the `Publisher` interface is small enough to add it.

## License

MIT
