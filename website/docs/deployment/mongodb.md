---
sidebar_position: 7
title: MongoDB
description: Setting up the MongoDB store — collections, indexes, configuration, and migration from SQL stores
---

# MongoDB Deployment

Ratchet on MongoDB 5.0+.

## Prerequisites

- MongoDB 5.0 or later (for snapshot reads and stable API)
- WiredTiger storage engine (default since MongoDB 3.2)

Ratchet's MongoDB store uses atomic single-document operations for job claiming and state
transitions. A standalone MongoDB server is acceptable for the store. Use a replica set or sharded
cluster when your application has separate requirements for multi-document transactions or high
availability.

## Maven Dependency

```xml
<dependency>
  <groupId>run.ratchet</groupId>
  <artifactId>ratchet-store-mongodb</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

This pulls in the MongoDB sync driver. No ODM (Morphia, Spring Data) is required — the store uses the driver directly.

## Collection Setup

You normally do not need a separate bootstrap step. The MongoDB store initializes collections and
indexes from its own `@PostConstruct`, so they are created automatically when the store starts. The
operation is idempotent and safe to run on every boot.

## Collections

| Collection | Purpose |
|-----------|---------|
| `scheduler_job` | Main job store — status, payload, scheduling, priority |
| `scheduler_job_execution` | Execution history — start/end times, node, outcome |
| `scheduler_job_log` | Optional per-job log storage if your `JobLogger` publishes log lines |
| `scheduler_job_archive` | Archived completed/failed jobs |
| `scheduler_lock` | Distributed advisory locks with TTL |
| `scheduler_node` | Cluster node heartbeats |
| `scheduler_workflow_condition` | Workflow branch conditions |
| `scheduler_dlq_alerts` | Dead-letter queue deduplication |
| `scheduler_resource_permit` | Resource permit tracking for rate limiting |

## Key Indexes

The initializer creates these indexes for query performance:

### scheduler_job

| Index | Fields | Notes |
|-------|--------|-------|
| `idx_job_claim_exec` | `status`, `job_type`, `priority` DESC, `scheduled_time`, `_id` | Executable claim candidate filtering |
| `idx_job_claim_recurring` | `status`, `job_type`, `priority` DESC, `next_fire`, `_id` | Recurring claim candidate filtering |
| `idx_job_poll_composite` | `status`, `priority` DESC, `scheduled_time` | General due-job lookup |
| `idx_job_recurring_composite` | `job_type`, `status`, `next_fire` | Recurring due-time lookup |
| `idx_job_idempotency_key` | `idempotency_key` | **Unique** — global dedup |
| `idx_job_active_business_key` | `business_key` | **Unique partial** — only for PENDING/RUNNING/PAUSED |
| `idx_job_tags` | `tags` | Multikey index for tag-based queries |
| `idx_job_picked_by` | `picked_by` | Find jobs claimed by a specific node |

### scheduler_lock

| Index | Fields | Notes |
|-------|--------|-------|
| `idx_lock_ttl` | `expires_at` | **TTL index** — MongoDB auto-deletes expired locks |

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

This provides the same guarantee — no two nodes claim the same job — through MongoDB's document-level write lock rather than row-level `SKIP LOCKED`.

### Sequential IDs

MongoDB uses ObjectIDs by default, but Ratchet's API uses `long` job IDs. The MongoDB store maintains a `counters` collection with atomic `$inc` to generate sequential numeric IDs:

```javascript
db.counters.findOneAndUpdate(
  { _id: "scheduler_job" },
  { $inc: { seq: 1 } },
  { returnDocument: "after", upsert: true }
)
```

### Tags

In SQL stores, tags use a separate `scheduler_job_tag` join table. In MongoDB, tags are embedded as an array field on the job document with a multikey index — more natural for document storage and eliminates the join.

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

Required unique indexes are created at startup. If Ratchet cannot create the idempotency,
active-business-key, or DLQ deduplication indexes, startup fails so duplicate scheduling semantics
are not silently weakened.

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

- [Database Setup](./database-setup.md) — General database preparation
- [Clustering](./clustering.md) — Multi-node deployment patterns
- [Performance Tuning](./performance-tuning.md) — Query optimization and index maintenance
