---
title: Upgrade Procedure
description: Coordinate the permanent-idempotency migration and atomic store SPI upgrade across every Ratchet node.
---

# Upgrade Procedure

The permanent-idempotency ledger and atomic completion/recurring store contracts require a
**coordinated upgrade**. Stop all older writers and retention jobs before migrating; do not run
mixed versions against the migrated database. Older binaries can bypass the ledger and the new
atomic bookkeeping contracts even when their SQL remains syntactically compatible.

## Upgrade order

1. Back up the database and rehearse restore and migration against a copy.
2. Stop submissions from every producer. Drain running jobs, then stop all scheduler nodes,
   retention workers, and administrative processes that can write Ratchet data.
3. For SQL stores, apply `V007__permanent_idempotency_keys.sql` through the configured migration
   mechanism. Use the updated canonical schema for new installations. Do not modify or rerun
   previously recorded migrations.
4. Deploy matching API, engine, and store artifacts everywhere. Custom stores must implement
   `JobTerminalStore.commitCompletion`, `RecurringJobStore.commitRecurringExecutions`, recurring
   query/count operations, and `JobCrudStore.findOriginalJobIdByIdempotencyKey` before starting.
   Lease-based recurring stores must also preserve opaque claim ownership through commit/release.
5. For MongoDB, start the first upgraded node while all older writers remain stopped. Initialization
   backfills permanent key reservations from surviving jobs. MongoDB must support transactions
   through a replica set or sharded deployment. Verify initialization succeeds before starting
   additional nodes and producers.
6. Verify normal submission, duplicate submission, execution, recurring scheduling, and batch/chain
   completion. Resume retention and producer traffic after these checks pass.

## Retention and historical limits

The `scheduler_idempotency_key` table or collection retains a minimal key, original job UUID, and
reservation timestamp. It is deliberately independent of job-history retention and has no cascading
foreign key. Do not purge its rows during normal archive cleanup. Its size grows with distinct keys,
including automatically generated keys.

A repeated key returns the original job UUID after job history is deleted; detailed history may no
longer exist. Concurrent submissions may still produce a duplicate-key exception for a losing
transaction. Retry in a fresh transaction to resolve the original UUID. Idempotency prevents a
second job from being created; execution retains Ratchet's at-least-once semantics.

Migration preserves keys on surviving job rows and all subsequent submissions. Keys already lost
through archival or deletion before this upgrade cannot be recovered from the database. Reconcile
external history separately if that historical guarantee is required.

## Version and database boundaries

Earlier MySQL/PostgreSQL compatibility tests with 0.1.1 establish common row-format readability,
not permission to use a mixed fleet with the new permanent-key or atomic-bookkeeping guarantees.
There is no mixed-version guarantee for 0.1.0; its identifier/schema conversion remains a separate,
stopped migration.

## Rollback boundary

Keep old binaries stopped after enabling the new contracts. Rolling back only the binaries would
allow writes that bypass permanent reservations. Use the rehearsed database-and-binary restore
procedure if rollback is necessary, accounting for any work performed since the backup.

## Related guides

- [Database Setup](/deployment/database-setup)
- [Clustering](/deployment/clustering)
