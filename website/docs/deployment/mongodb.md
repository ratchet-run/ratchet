---
sidebar_position: 7
title: MongoDB
description: "Setting up the MongoDB store: collections, indexes, configuration, and migration from SQL stores"
---

# MongoDB Deployment

Ratchet on MongoDB 6.0+.

## Prerequisites

- MongoDB 6.0 or later
- WiredTiger storage engine (default since MongoDB 3.2)
- A replica set or sharded cluster

Ratchet's MongoDB store uses atomic single-document operations for job claiming. State changes that
also update a cross-collection invariant use MongoDB transactions, including active business-key
ownership shared by queue jobs and recurring masters. Standalone `mongod` deployments do not
support those transactions.

Lifecycle/status methods enter that transaction before changing the owner document, including for
jobs without a business key; determining that no reservation exists outside the transaction would
open a race with a concurrent save. Moving to this store version therefore requires a replica set
or sharded cluster even when an application does not currently assign business keys.

## Maven Dependency

```xml
<dependency>
  <groupId>run.ratchet</groupId>
  <artifactId>ratchet-store-mongodb</artifactId>
  <version>0.3.0</version>
</dependency>
```

This pulls in the MongoDB sync driver. No ODM (Morphia, Spring Data) is required; the store uses the driver directly.

## Collection Setup

You normally do not need a separate bootstrap step. The MongoDB store initializes collections and
indexes from its own `@PostConstruct`, so they are created automatically when the store starts. The
operation is idempotent and safe to run on every boot.

## Collections

| Collection | Purpose |
|-----------|---------|
| `scheduler_job` | Main job store: status, payload, scheduling, priority |
| `scheduler_business_key_reservation` | Shared active business-key ownership for queue jobs and recurring masters |
| `scheduler_recurring_job` | Recurring job master records (cron/interval definitions) |
| `scheduler_recurring_job_archive` | Archived recurring job definitions |
| `scheduler_batch` | Batch parent records and progress state |
| `scheduler_batch_metrics` | Batch-level runtime metrics |
| `scheduler_job_execution` | Execution history: start/end times, node, outcome |
| `scheduler_job_log` | Optional per-job log storage if your application persists `JobLogLine` events |
| `scheduler_job_archive` | Archived completed/failed jobs |
| `scheduler_lock` | Distributed advisory locks with TTL |
| `scheduler_node` | Cluster node heartbeats |
| `scheduler_workflow_condition` | Workflow branch conditions |
| `scheduler_resource_limit` | Resource concurrency configuration |
| `scheduler_resource_permit` | Resource permit tracking for rate limiting |

## Key Indexes

The initializer creates these indexes for query performance:

### scheduler_job

| Index | Fields | Notes |
|-------|--------|-------|
| `idx_job_claim_exec` | `status`, `job_type`, `priority` DESC, `scheduled_time`, `_id` | Executable claim candidate filtering |
| `idx_job_poll_composite` | `status`, `priority` DESC, `scheduled_time` | General due-job lookup |
| `idx_job_idempotency_key` | `idempotency_key` | **Unique** (global dedup) |
| `idx_job_active_business_key` | `business_key` | **Unique partial** defense within queue jobs (PENDING/RUNNING/PAUSED/WAITING only) |
| `idx_job_tags` | `tags` | Multikey index for tag-based queries |
| `idx_job_picked_by` | `picked_by` | Find jobs claimed by a specific node |

### scheduler_business_key_reservation

The reservation document's `_id` is the business key, so MongoDB's built-in `_id_` index is the
single unique serialization point across queue jobs and recurring masters. `idx_bk_owner` indexes
`owner_job_id` for terminal and cancel cleanup. Reservation and owner changes commit in the same
transaction.

### scheduler_lock

| Index | Fields | Notes |
|-------|--------|-------|
| `idx_lock_ttl` | `expires_at` | **TTL index** (MongoDB auto-deletes expired locks) |

### scheduler_job_execution

| Index | Fields | Notes |
|-------|--------|-------|
| `idx_execution_job_id` | `job_id` | Execution history lookup |
| `idx_execution_node_started` | `node_id`, `started_at` | Per-node execution queries |

## How It Differs from SQL Stores

MongoDB doesn't have `SELECT ... FOR UPDATE SKIP LOCKED`. Instead, the MongoDB store first ranks due candidates with an aggregation that computes effective priority, then claims each selected ID with an **atomic `findOneAndUpdate`**:

```javascript
// Claim one selected candidate ID (atomic)
db.scheduler_job.findOneAndUpdate(
  { _id: candidateId, status: "PENDING" },
  { $set: { status: "RUNNING", picked_by: nodeId, picked_at: ISODate() } },
  { returnDocument: "after" }
)
```

This gives the same guarantee (no two nodes claim the same job) through MongoDB's document-level write lock rather than row-level `SKIP LOCKED`.

### UUIDv7 Identifiers

Ratchet uses the same RFC 9562 §5.7 UUIDv7 identifiers on MongoDB as it does for SQL stores. Those IDs are generated in the application, stored in `_id` as BSON binary subtype 4 (`UuidRepresentation.STANDARD`), and remain time-ordered without a database counter collection. The `MongoClientFactory` enforces the STANDARD representation; a `@PostConstruct` probe in `MongoJobStoreImpl` fails fast if a caller-supplied client uses any other UUID encoding.

### Tags

In SQL stores, tags use a separate `scheduler_job_tag` join table. In MongoDB, tags are embedded as an array field on the job document with a multikey index, which is more natural for document storage and eliminates the join.

## Configuration

### Shared Options

Use `RatchetOptions.builder().store(s -> s.priorityBoostIntervalMinutes(...))` to configure the shared starvation-prevention priority boost interval. The default is 15 minutes.

### Connection

The MongoDB store injects a `MongoDatabase` CDI bean. Keep the underlying `MongoClient` as an
application-scoped resource and close it at shutdown:

```java
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

@ApplicationScoped
public class MongoProducer {
  private MongoClient client;

  @Produces
  @ApplicationScoped
  public MongoDatabase mongoDatabase() {
    if (client == null) {
      client = MongoClients.create("mongodb://localhost:27017");
    }
    return client.getDatabase("ratchet");
  }

  @PreDestroy
  void close() {
    if (client != null) {
      client.close();
    }
  }
}
```

With MicroProfile Config, read the URI and database name from your own application properties:

```java
@Produces
@ApplicationScoped
public MongoDatabase mongoDatabase(
    @ConfigProperty(name = "mongodb.uri") String uri,
    @ConfigProperty(name = "mongodb.database", defaultValue = "ratchet") String database) {
  client = MongoClients.create(uri);
  return client.getDatabase(database);
}
```

Required unique indexes are created at startup. If Ratchet cannot create the idempotency or
active-business-key indexes, startup fails so duplicate scheduling semantics are not silently
weakened.

When upgrading an existing database, startup backfills reservations for active queue jobs and live
recurring masters. If the old data already contains the same business key in both collections,
startup fails and reports the conflicting owners; resolve that conflict before retrying. Perform a
quiescent, coordinated upgrade: stop scheduler writes from all old nodes, let one upgraded node
complete initialization, and then start the remaining upgraded nodes. Do not run old and new
Ratchet versions together, because old MongoDB store code does not write the shared reservation
collection.

## Monitoring

Use MongoDB's built-in profiler to identify slow queries:

```javascript
db.setProfilingLevel(1, { slowms: 100 })
db.system.profile.find({ ns: /scheduler_/ }).sort({ ts: -1 }).limit(10)
```

Key metrics to watch:
- `scheduler_job` collection size and index hit rates
- Lock acquisition time on `scheduler_lock`
- `findOneAndUpdate` latency on claim operations

## See Also

- [Database Setup](./database-setup.md) -- General database preparation
- [Clustering](./clustering.md) -- Multi-node deployment patterns
- [Performance Tuning](./performance-tuning.md) -- Query optimization and index maintenance
