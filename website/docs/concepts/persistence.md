---
sidebar_position: 10
title: Persistence
description: Entity/document model, JobStore SPI composition, UUIDv7 identifiers, and constraint detection
---

# Persistence

Ratchet persists all job state in the selected store backend. SQL stores use JPA entities and DDL-backed tables; the MongoDB store maps the same model to documents and collections. The shared persistence layer is built around a composable SPI interface, UUIDv7 identifiers, and dialect-specific constraint detection where the backend needs it.

## Entity Model

The core logical model is `JobEntity`. In SQL stores it is split across a cold
metadata table (`scheduler_job`) and a hot executable queue table
(`scheduler_job_queue`). The cold table owns immutable job shape and terminal
history; the hot table exists only while a job is live and owns claim/poll
state. MongoDB maps the same logical model to collections. Supporting entities
handle batches, executions, workflow conditions, locks, nodes, structured logs,
resource limits, DLQ alerts, and archived jobs.

<div class="docs-diagram persistence-model-diagram" role="img" aria-label="Ratchet persistence model with scheduler_job as cold metadata, scheduler_job_queue as hot live state, and supporting tables for tags, executions, batches, workflow conditions, locks, nodes, logs, resources, alerts, and archive rows.">
  <div class="docs-diagram-table docs-diagram-card--primary">
    <div class="docs-diagram-table-header">
      <strong>scheduler_job</strong>
      <small>Cold metadata, immutable job shape, and terminal survivor fields.</small>
    </div>
    <div class="docs-diagram-fields">
      <span>job_id UUIDv7 PK</span>
      <span>priority + job_type</span>
      <span>payload + params</span>
      <span>max_retries + backoff</span>
      <span>cron_expr + zone_id</span>
      <span>idempotency_key</span>
      <span>business_key</span>
      <span>depends_on</span>
      <span>superseded_by</span>
      <span>caller_principal</span>
      <span>resource_name</span>
      <span>terminal status/error/result</span>
    </div>
  </div>

  <div class="docs-diagram-connector">
    <span>1:0/1 while live</span>
  </div>

  <div class="docs-diagram-table docs-diagram-card--active">
    <div class="docs-diagram-table-header">
      <strong>scheduler_job_queue</strong>
      <small>Hot claim/poll state. SQL claim, pickup, retry, pause, resume, and signal operations target this table.</small>
    </div>
    <div class="docs-diagram-fields">
      <span>job_id PK/FK</span>
      <span>status: PENDING, RUNNING, PAUSED, WAITING</span>
      <span>scheduled_time</span>
      <span>attempts</span>
      <span>picked_by + picked_at</span>
      <span>paused_from_status</span>
      <span>last_error</span>
      <span>version + updated_at</span>
      <span>signal_key</span>
      <span>signal_timeout</span>
      <span>signal_payload</span>
      <span>signal_delivery_id</span>
    </div>
  </div>

  <div class="docs-diagram-connector">
    <span>Supporting tables</span>
  </div>

  <div class="docs-diagram-row docs-diagram-row--tight">
    <div class="docs-diagram-card">
      <strong>scheduler_job_tag</strong>
      <small>Tags per job for search and affinity filters.</small>
    </div>
    <div class="docs-diagram-card">
      <strong>scheduler_job_execution</strong>
      <small>Execution attempts and history.</small>
    </div>
    <div class="docs-diagram-card">
      <strong>scheduler_batch</strong>
      <small>Batch parent metadata.</small>
    </div>
    <div class="docs-diagram-card">
      <strong>scheduler_batch_metrics</strong>
      <small>Batch progress counters.</small>
    </div>
    <div class="docs-diagram-card">
      <strong>scheduler_workflow_condition</strong>
      <small>Workflow branch predicates and conditions.</small>
    </div>
    <div class="docs-diagram-card">
      <strong>scheduler_lock</strong>
      <small>Store-backed leases for singleton operations.</small>
    </div>
    <div class="docs-diagram-card">
      <strong>scheduler_node</strong>
      <small>Node registration and heartbeats.</small>
    </div>
    <div class="docs-diagram-card">
      <strong>scheduler_job_log</strong>
      <small>Structured job logs.</small>
    </div>
    <div class="docs-diagram-card">
      <strong>scheduler_resource_limit / permit</strong>
      <small>Resource capacity and active permits.</small>
    </div>
    <div class="docs-diagram-card">
      <strong>scheduler_dlq_alerts</strong>
      <small>DLQ alert audit and deduplication.</small>
    </div>
    <div class="docs-diagram-card">
      <strong>scheduler_job_archive</strong>
      <small>Archived terminal jobs.</small>
    </div>
    <div class="docs-diagram-card">
      <strong>scheduler_business_key_reservation</strong>
      <small>Active business-key uniqueness.</small>
    </div>
  </div>
</div>

### SQL Job Tables

The SQL stores denormalize a few immutable fields into `scheduler_job_queue`
(`job_type`, `priority`, `business_key`, `timeout_sec`, `max_retries`) so the
claim path can populate lightweight claim DTOs from one hot table.

| Column | Type | Purpose |
|--------|------|---------|
| `job_id` | `BINARY(16)`/`uuid` (UUIDv7) | Primary key, time-ordered |
| `scheduler_job.job_type` | `ENUM`/`TEXT` | Internal execution type (SINGLE, BATCH_CHILD, etc.) |
| `scheduler_job.priority` | `INT` | Priority ordinal (0=LOWEST to 4=CRITICAL) |
| `scheduler_job.payload` | JSON | Serialized job definition (target, method, args) |
| `scheduler_job.params` | JSON | Key-value parameters accessible via `JobContext` |
| `idempotency_key` | `VARCHAR(36)` UNIQUE | Globally unique deduplication key |
| `business_key` | `VARCHAR` | Active-unique key for concurrent execution prevention |
| `depends_on` | `BINARY(16)`/`uuid` | FK to parent job for chains |
| `superseded_by` | `BINARY(16)`/`uuid` | FK to replacement job |
| `caller_principal` | `VARCHAR(255)` | Captured Jakarta Security caller principal, if available |
| `resource_name` | `VARCHAR(100)` | Resource pool for permit acquisition |
| `terminal_status` / `terminal_error` | `ENUM`/`TEXT` / `TEXT` | Cold survivor fields set at terminal transition |
| `scheduler_job_queue.status` | `ENUM`/`TEXT` | Live lifecycle state (PENDING, RUNNING, PAUSED, WAITING) |
| `scheduler_job_queue.scheduled_time` | `TIMESTAMP` | When the job becomes eligible for polling |
| `scheduler_job_queue.attempts` | `INT` | Current attempt count while live |
| `scheduler_job_queue.picked_by` / `picked_at` | `VARCHAR(64)` / `TIMESTAMP` | Node claim ownership and claim time |
| `scheduler_job_queue.version` | `INT` | Optimistic locking version for live queue mutations |
| `scheduler_job_queue.signal_key` / `signal_timeout` | `VARCHAR` / `TIMESTAMP` | Signal-wait key and timeout for WAITING jobs |
| `scheduler_job_queue.signal_payload` / metadata | `TEXT` / `VARCHAR` | Delivered signal payload, decision metadata, and delivery id |

The `scheduler_business_key_reservation` table owns active business-key
uniqueness. Terminal rows keep their `business_key` for audit/search, but they
do not block a future active job from using the same key.

### Indexes

SQL stores define hot queue indexes for the Poller and supporting cold-table
indexes for traversal/search. MongoDB defines analogous collection indexes in
`ratchet-store-mongodb`:

| Index | Columns | Purpose |
|-------|---------|---------|
| `idx_claim_executable` | `scheduler_job_queue(job_type, scheduled_time, priority, job_id)` for pending rows | Executable claim filter |
| `idx_queue_orphan` | `scheduler_job_queue(status, picked_at, picked_by)` | Orphan recovery by node |
| `pk_scheduler_business_key_reservation` | `scheduler_business_key_reservation(business_key)` | Active business-key uniqueness and lookup |
| `idx_job_depends_on` | `scheduler_job(depends_on)` | Chain/workflow traversal |
| `idx_job_superseded_by` | `scheduler_job(superseded_by)` | Replacement lookup |
| `idx_job_created_at` | `scheduler_job(created_at)` | Operational search and retention |
| `idx_rec_claim` | `scheduler_recurring_job(next_fire) WHERE is_paused = FALSE` (PostgreSQL) / `scheduler_recurring_job(is_paused, next_fire)` (MySQL) | Recurring-master claim scheduling |
| `idx_signal_key_status` | `scheduler_job_queue(signal_key, status)` | Atomic signal delivery by key |
| `idx_signal_timeout_status` | `scheduler_job_queue(status, signal_timeout)` | Signal timeout scans |
| `idx_signal_delivery_id` | `scheduler_job_queue(signal_delivery_id)` | Signal delivery event lookup |

## UUIDv7 Identifiers

Ratchet uses **RFC 9562 §5.7 UUIDv7** for primary keys. UUIDs are 128-bit values that are time-ordered, coordination-free, and globally unique.

### Layout

<div class="docs-diagram" role="img" aria-label="UUIDv7 layout: 48 bits timestamp, 4 bits version, 12 bits rand_a, 2 bits variant, and 62 bits rand_b.">
  <span class="fit-kicker">128-bit UUIDv7 layout</span>
  <div class="uuid-strip">
    <div class="uuid-segment docs-diagram-card--primary">
      <strong>unix_ts_ms</strong>
      <small>48 bits</small>
    </div>
    <div class="uuid-segment">
      <strong>ver</strong>
      <small>4 bits</small>
    </div>
    <div class="uuid-segment docs-diagram-card--active">
      <strong>rand_a</strong>
      <small>12 bits</small>
    </div>
    <div class="uuid-segment">
      <strong>var</strong>
      <small>2 bits</small>
    </div>
    <div class="uuid-segment docs-diagram-card--store">
      <strong>rand_b</strong>
      <small>62 bits</small>
    </div>
  </div>
</div>

| Field | Bits | Purpose |
|-------|------|---------|
| `unix_ts_ms` | 48 | Wall-clock millisecond timestamp |
| `ver` | 4 | Version constant `7` |
| `rand_a` | 12 | Per-millisecond monotonic counter |
| `var` | 2 | RFC 9562 variant constant `10` |
| `rand_b` | 62 | Cryptographic random (SecureRandom) |

### Properties

- **Time-ordered:** The 48-bit timestamp prefix preserves B-tree locality: inserts cluster at the right edge, and range scans by time work directly.
- **Monotonic within a millisecond:** `rand_a` is used as a per-ms counter; on overflow inside a single ms, generation busy-spins via `Thread.onSpinWait` until the wall clock advances (RFC 9562 §6.2 wait-for-tick). The timestamp is never advanced past wall-clock time.
- **Coordination-free:** 62 bits of randomness in `rand_b` make collisions vanishingly unlikely without inter-node coordination.
- **128-bit `java.util.UUID`:** Standard Java type, no special storage adapter on PostgreSQL (native `uuid`). MySQL stores as `BINARY(16)` and uses the MySQL store's `META-INF/orm-mysql.xml` mapping plus `UuidByteArrayConverter` so non-Hibernate JPA providers bind UUID fields as 16 bytes. MongoDB stores BSON UUID subtype 4 (`UuidRepresentation.STANDARD`).

### Utility Methods

```java
// Generate a new UUIDv7
UUID id = UuidV7Factory.create();
```

### Why UUIDv7 Instead of TSID or Auto-Increment

| Concern | Auto-Increment | TSID | UUIDv7 |
|---------|---------------|-------------------|--------|
| Multi-node generation | Requires coordination | Manual node-id slot (10 bits = 1024 nodes) | Coordination-free |
| Concurrent generators before collisions | n/a | ~38 (birthday paradox on 10-bit node + 12-bit seq) | Effectively unbounded (62 random bits) |
| Insert contention | B-tree hotspot | Distributed | Distributed (timestamp prefix only) |
| Temporal ordering | Needs `created_at` | Embedded | Embedded |
| Range scan by time | Needs index | Use ID | Use ID |
| Migration / merge | Conflicts | Risk if node ids reused | Globally unique |

## JobStore SPI

The mandatory `JobStore` interface composes the persistence concerns every store must provide. Optional capabilities are **not** part of it: a store advertises one by also implementing its interface, and callers probe for it with `capability()` rather than assuming it is present. A minimal backend implements only the core through one CDI bean; the shipped MySQL, PostgreSQL, and MongoDB stores advertise every capability.

```java
public interface JobStore
    extends JobCrudStore,
            JobClaimStore,
            JobTerminalStore,
            JobRetryStore,
            JobPauseStore,
            JobBatchStatusStore,
            JobBulkStore,
            NodeStore,
            TagStore {

    // Probe for an optional capability this store may also implement.
    default <T> Optional<T> capability(Class<T> type) {
        return type.isInstance(this) ? Optional.of(type.cast(this)) : Optional.empty();
    }
}
```

### Core Interface Responsibilities (mandatory)

| Interface | Responsibility |
|-----------|---------------|
| `JobCrudStore` | Create, read, update, delete individual jobs |
| `JobClaimStore` | Atomic batch claiming (`SKIP LOCKED` for SQL stores, atomic updates for MongoDB) |
| `JobTerminalStore` | Terminal success, failure, and cancellation transitions |
| `JobRetryStore` | Retry scheduling and attempt-state updates |
| `JobPauseStore` | Pause and resume transitions |
| `JobBatchStatusStore` | Non-terminal status, pickup, and orphan operations |
| `JobBulkStore` | Bulk operations (DLQ purge, batch insert) |
| `NodeStore` | Node registration, heartbeat, and crash recovery |
| `TagStore` | Job tag writes |

### Optional Capability Responsibilities

| Capability | Responsibility |
|-----------|---------------|
| `RecurringJobStore` | Recurring-master persistence (claim, advance, cancel/archive) |
| `BatchStore` | Batch parent/child management, progress tracking, and metrics |
| `WorkflowConditionStore` | Workflow condition persistence and retrieval |
| `SignalStore` | Atomic signal delivery, signal-timeout scans, and signal-event lookup |
| `ResourcePermitStore` | Resource permit acquisition and release |
| `LockStore` | Distributed lock acquisition and release |
| `ArchiveStore` | Job archival to the archive table/collection |
| `JobQueryStore` | Read-only list/detail/queue-health queries and tag lookups |
| `JobAnalyticsStore` | Aggregate counts, rate statistics, and percentile metrics |
| `JobAuditStore` | Execution history recording and structured job log storage |
| `DlqAlertStore` | DLQ alert audit trail and deduplication |

### Why a Core-Plus-Capabilities Split?

The decomposition serves multiple purposes:

1. **No forced surface:** A store implements only what its backend can support. An in-memory test double or a Redis backend can ship the core and skip archiving, analytics, or distributed locks.
2. **Conditional conformance:** The TCK reports a capability contract as `N/A` for a store that does not advertise it, so a core-only store stays conformant rather than failing for absent features.
3. **Cognitive load:** Each interface has a focused, understandable contract.
4. **Dependency injection:** RI services depend only on the interfaces they need (e.g. `Poller` depends on `JobClaimStore`, not the full `JobStore`), and resolve optional capabilities through a nullable `Instance<T>`.

## Constraint Detection

Different databases report constraint violations differently. The `ConstraintDetector` interface abstracts this:

```java
public interface ConstraintDetector {
    @Nullable String constraintName(Exception e);
    boolean isDuplicateKey(Exception e);
    default boolean isDuplicateBusinessKey(Exception e);
    boolean isDeadlock(Exception e);
    boolean isTransientConnectionFailure(Exception e);
}
```

Each SQL store module provides a dialect-specific implementation:

- **MySQL:** Parses for "Duplicate entry" in the error message
- **PostgreSQL:** Checks SQL state codes (23505 for unique violation, plus deadlock/serialization and connection-failure states)

This is used primarily for idempotency key enforcement: when a duplicate key is detected, the submission is silently rejected rather than throwing an error to the caller.

## DDL Schema

SQL store modules ship DDL as plain SQL files in `src/main/resources/ddl/`. The `*-schema.sql` file is the authoritative clean-install schema for that dialect, and it reserves a `ratchet_schema_version` table for ordered upgrades.

Ratchet still does not run migrations automatically by default. Your application remains responsible for applying schema changes, whether through Flyway, Liquibase, or another deployment-time mechanism. When incremental Ratchet migration scripts are added, they live under `ddl/migrations/` and follow the `V###__description.sql` convention. Those ordered `V*` files must compose to the same schema shipped in the clean-install DDL.

For SQL stores, if you do not already use a migration framework, `ratchet-store-core` also exposes `SchemaMigrator`, a small optional utility that discovers ordered `V*` scripts, serializes startup with a database advisory lock, validates checksums in `ratchet_schema_version`, and applies only pending scripts. Call it from a `SchedulerLifecycleHook.beforeStart` hook so migrations finish before the poller starts claiming jobs.

```
stores/ratchet-store-mysql/src/main/resources/ddl/mysql-schema.sql
stores/ratchet-store-postgresql/src/main/resources/ddl/postgresql-schema.sql
```

MongoDB does not ship SQL DDL. The `ratchet-store-mongodb` module creates the required collections and indexes at startup.

### UUID Inspection by Store

- **PostgreSQL:** query UUID columns directly; `psql` renders native `uuid`
  values as hyphenated strings.
- **MySQL:** raw `BINARY(16)` values are not readable in CLI output. Apply the
  optional `ddl/views/vw_jobs.sql` operator views and query those views for
  hyphenated UUID strings. The views use `BIN_TO_UUID(col)` with no swap flag;
  `BIN_TO_UUID(col, 1)` is for MySQL's UUIDv1 time-reorder format and does not
  match Ratchet's Java-standard byte order.
- **MongoDB:** use a `MongoClient` configured with `UuidRepresentation.STANDARD`
  so BSON subtype 4 UUID values round-trip correctly; `mongosh` renders them as
  `UUID("...")`.

## Optimistic Locking

SQL stores use JPA `@Version` on the `version` column, while MongoDB uses atomic filter-and-update operations. Both paths prevent lost updates when two nodes attempt to modify the same job concurrently. The engine uses compare-and-swap patterns for critical transitions:

```java
// Atomic status transition — fails if another thread changed the status
boolean success = jobStore.compareAndSwapStatus(
    jobId, JobStatus.RUNNING, JobStatus.SUCCEEDED, null);
```

Combined with `FOR UPDATE SKIP LOCKED` in SQL stores or atomic document claiming in MongoDB, this ensures a ready job is claimed by only one node at a time.

## Related

- [Architecture Overview](./overview.md) -- Module structure and SPI overview
- [Execution Model](./execution-model.md) -- How job claiming works
- [Clustering](./clustering.md) -- Multi-node persistence considerations
- [Job Lifecycle](./job-lifecycle.md) -- State transitions stored in the database
