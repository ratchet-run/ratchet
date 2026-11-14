---
title: Upgrading
description: Upgrade steps between Ratchet releases, including the coordinated 0.3.1 to 0.4.0 storage-contract upgrade.
---

# Upgrading

## Upgrade from 0.5.0 to 0.5.1

SQL stores gain `V009__drop_node_info.sql`, which drops the unused `scheduler_node.node_info`
column. Ratchet never wrote to it, so no data is lost. Apply it through the configured migration
mechanism, or use the updated canonical schema for new installations. MongoDB needs no change;
existing node documents may still carry an empty `node_info` field, which Ratchet ignores.

MySQL also gains `V010__widen_text_columns.sql`, which changes ten `TEXT` columns to `MEDIUMTEXT`.
Values over 64 KB, such as large signal payloads, extension state, encrypted values and long error
messages, then fit on MySQL as they already do on the other stores. To change a column type, MySQL
copies the whole table, and writes to that table wait until the copy finishes. On a large
installation, run this migration in a quiet window.

MySQL also gains `V013__priority_int.sql`, which changes `priority` from `TINYINT UNSIGNED` to
`INT` on `scheduler_recurring_job`, `scheduler_job`, `scheduler_job_queue` and
`scheduler_job_archive`, as the other stores already declare it. Hibernate schema validation then
passes on MySQL without a mapping file. Values, defaults and check constraints are unchanged. Like
`V010`, it copies each table and blocks writes to it until the copy finishes, so run it in a quiet
window on a large installation.

PostgreSQL and Oracle also gain `V013__batch_reducer.sql`. Besides adding two `scheduler_batch`
columns, it rebuilds `idx_job_depends_on` on `scheduler_job` as `(depends_on, job_id)` so batch
results can be read in pages. The migrator runs the PostgreSQL script in one transaction, so reads
and writes on `scheduler_job` wait until the new index is built and the transaction commits. On
Oracle, the index build blocks writes to `scheduler_job` while it runs, and the drop and the create
each fail with ORA-00054 if another transaction is writing to the table at that moment. Oracle DDL
commits as it goes, so if the script stops between the drop and the create, the index stays missing
until the script runs again. Running it again is safe. On a large installation, plan a maintenance
window for this migration, and on Oracle stop the nodes that write to `scheduler_job` first. MySQL
and SQL Server only add the columns. MongoDB replaces the old single-field index of the same name
when the store starts.

Every SQL store also gains the partition set tables, in `V014__partition_sets.sql` on PostgreSQL,
Oracle and SQL Server and in `V016__partition_sets.sql` on MySQL. The new tables start empty, so
their indexes build at once. The script also adds two nullable columns to `scheduler_batch` and
builds `idx_batch_partition_set` over its existing rows. The migrator runs the PostgreSQL script in
one transaction, and adding the columns locks `scheduler_batch` until it commits, so reads and
writes on that table wait while the index is built. On Oracle and SQL Server the index build blocks
writes to `scheduler_batch` while it runs, and on Oracle the DDL fails with ORA-00054 if another
transaction is writing to the table at that moment. Running the script again is safe. MySQL adds
the columns and builds the index online, so writes continue. `scheduler_batch` has one row per
batch, so this is usually quick, but on an installation that keeps many batches, run it in a quiet
window. MongoDB creates the new collections and indexes when the store starts.

SQL stores also gain a migration that adds the `scheduler_resource_waiter` table
(`V017__resource_waiter.sql` on MySQL, `V015__resource_waiter.sql` on PostgreSQL, Oracle and SQL
Server). The table holds the jobs waiting for a resource permit, so they are served in order and a
released permit wakes them on any node. MongoDB creates the collection and its index when the store
starts. With `ratchet.schema.auto-migrate=false` on Spring Boot or Micronaut, a MongoDB node only
validates the schema, so create the `scheduler_resource_waiter` collection and its
`idx_resource_waiter_order` index before you start it. The store JAR's `ddl/mongodb-init.js` creates
both. During a rolling upgrade, nodes still on the older version ignore the waiting line, so the
order holds only once every node runs the new version.

Extension state is now limited to 1 MiB of UTF-8 per namespace on every store. A store rejects
larger state with `IllegalArgumentException`.

## Upgrade from 0.4.0 to 0.5.0

0.5.0 adds no schema migrations. SQL stores stay at `V008`, and MongoDB needs no new
initialization. Deploy matching API, engine, and store artifacts on every node.

Custom stores need no new contract methods, but 0.5.0 moves the schema metadata API and changes
some store-core members. See the [store TCK guide](/conformance/adopting-the-tck#store-core-upgrade-changes)
before rebuilding a custom store.

## Upgrade from 0.3.1 to 0.4.0

Moving from 0.3.1 to 0.4.0 introduces a permanent-idempotency ledger and atomic
completion/recurring store contracts. This transition requires a
**coordinated upgrade**. Stop all older writers and retention jobs before migrating; do not run
mixed versions against the migrated database. Older binaries can bypass the ledger and the new
atomic bookkeeping contracts even when their SQL remains syntactically compatible.

This coordinated shutdown applies to this storage-contract transition. Future releases can
support rolling upgrades when their schema and behavior remain compatible, with mixed-version
tests covering the supported source and target versions.

### Upgrade order

1. Back up the database and rehearse restore and migration against a copy.
2. Stop submissions from every producer. Drain running jobs, then stop all scheduler nodes,
   retention workers, and administrative processes that can write Ratchet data.
3. For MySQL, PostgreSQL, Oracle, and SQL Server, apply
   `V007__permanent_idempotency_keys.sql`, then `V008__priority_ordered_claim_index.sql`, through
   the configured migration mechanism before starting 0.4.0 workers. V007 backfills permanent
   reservations from surviving job rows; V008 adds the priority-ordered claim index. Use the
   updated canonical schema for new installations. Preserve the migration history; do not
   modify or rerun previously recorded migrations.
4. Deploy matching API, engine, and store artifacts everywhere. Custom stores must implement
   `JobTerminalStore.commitCompletion`, `RecurringJobStore.commitRecurringExecutions`,
   `RecurringJobStore.searchRecurring`, `RecurringJobStore.countRecurring`, and
   `JobCrudStore.findOriginalJobIdByIdempotencyKey` before starting.
   Lease-based recurring stores must also preserve opaque claim ownership through commit/release.
5. For MongoDB, start the first upgraded node while all older writers remain stopped. Initialization
   backfills permanent key reservations from surviving jobs. MongoDB must support transactions
   through a replica set or sharded deployment. Verify initialization succeeds before starting
   additional nodes and producers.
6. Verify normal submission, duplicate submission, execution, recurring scheduling, and batch/chain
   completion. Resume retention and producer traffic after these checks pass.

### Retention and historical limits

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

### Version and database boundaries

Earlier MySQL/PostgreSQL compatibility tests with 0.1.1 establish common row-format readability,
not permission to use a mixed fleet with the new permanent-key or atomic-bookkeeping guarantees.
There is no mixed-version guarantee for 0.1.0; its identifier/schema conversion remains a separate,
stopped migration.

### Rollback boundary

Keep old binaries stopped after enabling the new contracts. Rolling back only the binaries would
allow writes that bypass permanent reservations. Use the rehearsed database-and-binary restore
procedure if rollback is necessary, accounting for any work performed since the backup.

## Related guides

- [Database Setup](/deployment/database-setup)
- [Clustering](/deployment/clustering)
