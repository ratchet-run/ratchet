---
sidebar_position: 10
title: Persistence
description: Entity/document model, JobStore SPI composition, UUIDv7 identifiers, and constraint detection
---

# Persistence

Ratchet persists all job state in the selected store backend. SQL stores use JPA entities and DDL-backed tables; the MongoDB store maps the same model to documents and collections. The shared persistence layer is built around a composable SPI interface, UUIDv7 identifiers, and dialect-specific constraint detection where the backend needs it.

## Entity Model

The core model is `JobEntity`, which maps to the `scheduler_job` table for SQL stores and the `scheduler_job` collection for MongoDB. Supporting entities handle batches, executions, workflow conditions, locks, nodes, and archived jobs.

```
┌─────────────────────────────────────────┐
│              scheduler_job              │
│ (JobEntity - core job state)            │
├─────────────────────────────────────────┤
│ job_id (UUIDv7 PK)                      │
│ status, priority, job_type              │
│ scheduled_time, picked_by, picked_at    │
│ payload, params, tags                   │
│ max_retries, attempts, backoff_policy   │
│ cron_expr, zone_id, next_fire           │
│ idempotency_key, business_key           │
│ depends_on, superseded_by               │
│ resource_name                           │
│ on_success_payload, on_failure_payload  │
│ execution timing, result, version       │
└─────────────────────────────────────────┘
         │
         │ 1:N
         ▼
┌─────────────────────┐  ┌─────────────────────────┐
│ scheduler_job_tag   │  │ scheduler_job_execution  │
│ (tags per job)      │  │ (JobExecutionEntity)     │
└─────────────────────┘  └─────────────────────────┘

┌─────────────────────┐  ┌─────────────────────────┐
│ scheduler_batch     │  │ scheduler_batch_metrics  │
│ (BatchEntity)       │  │ (BatchMetricsEntity)     │
└─────────────────────┘  └─────────────────────────┘

┌─────────────────────────────┐  ┌─────────────────────┐
│ scheduler_workflow_condition│  │ scheduler_lock       │
│ (WorkflowConditionEntity)  │  │ (LockEntity)         │
└─────────────────────────────┘  └─────────────────────┘

┌─────────────────────┐  ┌─────────────────────────┐
│ scheduler_node      │  │ scheduler_job_archive    │
│ (NodeEntity)        │  │ (ArchivedJobEntity)      │
└─────────────────────┘  └─────────────────────────┘

┌──────────────────────┐  ┌──────────────────────────┐
│ scheduler_resource_  │  │ scheduler_dlq_alert      │
│ limit / permit       │  │ (DlqAlertEntity)         │
└──────────────────────┘  └──────────────────────────┘
```

### JobEntity

The central entity/document has these key fields:

| Column | Type | Purpose |
|--------|------|---------|
| `job_id` | `BINARY(16)`/`uuid` (UUIDv7) | Primary key, time-ordered |
| `status` | `VARCHAR(16)` | Current lifecycle state (PENDING, RUNNING, etc.) |
| `job_type` | `VARCHAR(16)` | Internal execution type (SINGLE, BATCH_CHILD, etc.) |
| `priority` | `INT` | Priority ordinal (0=LOWEST to 4=CRITICAL) |
| `scheduled_time` | `TIMESTAMP` | When the job becomes eligible for polling |
| `picked_by` | `VARCHAR(64)` | Node ID of the worker executing this job |
| `picked_at` | `TIMESTAMP` | When the job was claimed |
| `payload` | `BLOB/TEXT` | Serialized job definition (target, method, args) |
| `params` | `TEXT` (JSON) | Key-value parameters accessible via `JobContext` |
| `idempotency_key` | `VARCHAR(36)` UNIQUE | Globally unique deduplication key |
| `business_key` | `VARCHAR` | Active-unique key for concurrent execution prevention |
| `depends_on` | `BINARY(16)`/`uuid` | FK to parent job for chains |
| `superseded_by` | `BINARY(16)`/`uuid` | FK to replacement job |
| `resource_name` | `VARCHAR(100)` | Resource pool for permit acquisition |
| `max_retries` | `INT` | Maximum retry attempts |
| `attempts` | `INT` | Current attempt count |
| `backoff_policy` | `VARCHAR(16)` | NONE, FIXED, or EXPONENTIAL |
| `backoff_param_ms` | `INT` | Base delay for backoff calculation |
| `cron_expr` | `VARCHAR(64)` | Cron expression for recurring jobs |
| `zone_id` | `VARCHAR(32)` | Timezone for cron evaluation |
| `version` | `INT` | Optimistic locking version |

### Indexes

SQL stores define these `scheduler_job` indexes for the Poller and common queries. MongoDB defines analogous collection indexes in `ratchet-store-mongodb`:

| Index | Columns | Purpose |
|-------|---------|---------|
| `idx_job_claim_cover` | `job_type, scheduled_time, priority, job_id` where `status = 'PENDING'` | PostgreSQL executable claim filter |
| `idx_claim_executable` | `status, job_type, scheduled_time, priority, job_id` | MySQL executable claim filter |
| `idx_job_poll_composite` | `status, priority, scheduled_time` | General polling lookup |
| `idx_job_due` | `status, scheduled_time` | Simple due-job lookup |
| `idx_job_priority_due` | `priority, scheduled_time` | Priority-ordered queries |
| `idx_recurring_due` | `status, next_fire` | Recurring job scheduling |
| `idx_job_recurring_composite` | `next_fire, priority, job_id` where `status = 'PENDING'` and `job_type = 'RECURRING'` | PostgreSQL recurring claim filter |
| `idx_job_depends_on` | `depends_on` | Chain/workflow traversal |
| `idx_job_business_key` | `business_key` | Business key uniqueness check |
| `idx_job_picked_by` | `picked_by` | Orphan recovery by node |

## UUIDv7 Identifiers

Ratchet uses **RFC 9562 §5.7 UUIDv7** for primary keys. UUIDs are 128-bit values that are time-ordered, coordination-free, and globally unique.

### Layout

```
  0                   1                   2                   3
  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 ┌──────────────────────────────┬─────┬──────┬──┬───────────────┐
 │  48 bits: unix_ts_ms         │ ver │rand_a│va│   62 bits     │
 │  Unix epoch milliseconds     │  7  │ 12bit│r │   rand_b      │
 └──────────────────────────────┴─────┴──────┴──┴───────────────┘
```

| Field | Bits | Purpose |
|-------|------|---------|
| `unix_ts_ms` | 48 | Wall-clock millisecond timestamp |
| `ver` | 4 | Version constant `7` |
| `rand_a` | 12 | Per-millisecond monotonic counter |
| `var` | 2 | RFC 9562 variant constant `10` |
| `rand_b` | 62 | Cryptographic random (SecureRandom) |

### Properties

- **Time-ordered:** The 48-bit timestamp prefix preserves B-tree locality — inserts cluster at the right edge, range scans by time work directly.
- **Monotonic within a millisecond:** `rand_a` is used as a per-ms counter; on overflow inside a single ms, generation busy-spins via `Thread.onSpinWait` until the wall clock advances (RFC 9562 §6.2 wait-for-tick). The timestamp is never advanced past wall-clock time.
- **Coordination-free:** 62 bits of randomness in `rand_b` make collisions vanishingly unlikely without inter-node coordination.
- **128-bit `java.util.UUID`:** Standard Java type, no special storage adapter on PostgreSQL (native `uuid`). MySQL stores as `BINARY(16)` via `UuidByteArrayConverter`.

### Utility Methods

```java
// Generate a new UUIDv7
UUID id = UuidV7Factory.create();

// Extract creation timestamp (high 48 bits)
Instant created = UuidV7Factory.timestampOf(id);
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

The `JobStore` interface is a marker that composes the store SPIs used by the RI. Store implementations (MySQL, PostgreSQL, MongoDB, or your own backend) implement that full surface through one CDI bean.

```java
public interface JobStore
    extends JobCrudStore,
            JobClaimStore,
            JobTerminalStore,
            JobRetryStore,
            JobPauseStore,
            JobBatchStatusStore,
            JobStatusStore, // Deprecated compatibility marker for one alpha release
            JobBulkStore,
            BatchStore,
            LockStore,
            NodeStore,
            ArchiveStore,
            ExecutionStore,
            JobLogStore,
            TagStore,
            WorkflowConditionStore,
            BatchMetricsStore,
            DlqAlertStore,
            ResourcePermitStore {
    // Marker interface — all methods inherited from sub-interfaces
}
```

### Sub-Interface Responsibilities

| Interface | Responsibility |
|-----------|---------------|
| `JobCrudStore` | Create, read, update, delete individual jobs |
| `JobClaimStore` | Atomic batch claiming (`SKIP LOCKED` for SQL stores, atomic updates for MongoDB) |
| `JobTerminalStore` | Terminal success, failure, and cancellation transitions |
| `JobRetryStore` | Retry scheduling and attempt-state updates |
| `JobPauseStore` | Pause and resume transitions |
| `JobBatchStatusStore` | Non-terminal status, pickup, orphan, and recurring-cancel operations |
| `JobStatusStore` | Deprecated compatibility marker that composes the four status-focused SPIs above |
| `JobBulkStore` | Bulk operations (DLQ purge, batch insert) |
| `BatchStore` | Batch parent/child management, progress tracking |
| `LockStore` | Distributed lock acquisition and release |
| `NodeStore` | Node registration and heartbeat |
| `ArchiveStore` | Job archival to the archive table/collection |
| `ExecutionStore` | Execution history recording |
| `JobLogStore` | Structured job log storage |
| `TagStore` | Tag-based job queries |
| `WorkflowConditionStore` | Workflow condition persistence and retrieval |
| `BatchMetricsStore` | Batch-level metrics and progress |
| `DlqAlertStore` | DLQ alert audit trail and deduplication |
| `ResourcePermitStore` | Resource permit acquisition and release |

### Why Sub-Interfaces?

The decomposition serves multiple purposes:

1. **TCK modularity:** Future TCK versions can test sub-interfaces independently
2. **Cognitive load:** Each interface has a focused, understandable contract
3. **Dependency injection:** RI services depend only on the sub-interfaces they need (e.g., `Poller` depends on `JobClaimStore`, not the full `JobStore`)
4. **Alternative implementations:** A NoSQL store might implement `JobCrudStore` and `JobClaimStore` differently while reusing other sub-interface implementations

## Constraint Detection

Different databases report constraint violations differently. The `ConstraintDetector` interface abstracts this:

```java
public interface ConstraintDetector {
    boolean isUniqueConstraintViolation(Exception e);
    boolean isForeignKeyViolation(Exception e);
}
```

Each SQL store module provides a dialect-specific implementation:

- **MySQL:** Parses for "Duplicate entry" in the error message
- **PostgreSQL:** Checks SQL state codes (23505 for unique violation, 23503 for FK violation)

This is used primarily for idempotency key enforcement -- when a duplicate key is detected, the submission is silently rejected rather than throwing an error to the caller.

## DDL Schema

SQL store modules ship DDL as plain SQL files in `src/main/resources/ddl/`. The `*-schema.sql` file is the authoritative clean-install schema for that dialect, and it reserves a `ratchet_schema_version` table for ordered upgrades.

Ratchet still does not run migrations automatically by default. Your application remains responsible for applying schema changes, whether through Flyway, Liquibase, or another deployment-time mechanism. When incremental Ratchet migration scripts are added, they live under `ddl/migrations/` and follow the `V###__description.sql` convention. Those ordered `V*` files must compose to the same schema shipped in the clean-install DDL.

For SQL stores, if you do not already use a migration framework, `ratchet-store-core` also exposes `SchemaMigrator`, a small optional utility that discovers ordered `V*` scripts, serializes startup with a database advisory lock, validates checksums in `ratchet_schema_version`, and applies only pending scripts. Call it from a `SchedulerLifecycleHook.beforeStart` hook so migrations finish before the poller starts claiming jobs.

```
ratchet-store-mysql/src/main/resources/ddl/mysql-schema.sql
ratchet-store-postgresql/src/main/resources/ddl/postgresql-schema.sql
```

MongoDB does not ship SQL DDL. The `ratchet-store-mongodb` module creates the required collections and indexes at startup.

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
