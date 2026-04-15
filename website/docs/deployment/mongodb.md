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
- Replica set or sharded cluster (required for transactions)

:::caution Standalone mode
MongoDB standalone instances do not support multi-document transactions. Ratchet requires a replica set, even for single-node deployments. Use `mongod --replSet rs0` and `rs.initiate()` for development.
:::

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

You normally do not need a separate bootstrap step. `MongoJobStore` runs `new MongoCollectionInitializer(database).initialize()` from its own `@PostConstruct`, so collections and indexes are created automatically when the store starts.

If you want to pre-create them explicitly in a standalone bootstrap, use the initializer instance directly. The operation is **idempotent** — safe to run on every boot:

```java
@ApplicationScoped
public class MongoStartup {

    @Inject
    MongoClient mongoClient;

    @PostConstruct
    void init() {
        MongoDatabase db = mongoClient.getDatabase("myapp");
        new MongoCollectionInitializer(db).initialize();
    }
}
```

This creates all collections and indexes if they don't already exist.

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
| `idx_job_poll_composite` | `status`, `priority` DESC, `scheduled_time` | Primary polling query |
| `idx_job_recurring_composite` | `job_type`, `status`, `next_fire` | Recurring job claims |
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

MongoDB doesn't have `SELECT ... FOR UPDATE SKIP LOCKED`. Instead, the MongoDB store uses **atomic `findOneAndUpdate`** operations for all state transitions:

```javascript
// Claim a pending job (atomic)
db.scheduler_job.findOneAndUpdate(
  { status: "PENDING", scheduled_time: { $lte: ISODate() } },
  { $set: { status: "RUNNING", picked_by: nodeId, picked_at: ISODate() } },
  { sort: { priority: -1, scheduled_time: 1 } }
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

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `RATCHET_PRIORITY_BOOST_INTERVAL_MINUTES` | `15` | How often to boost starved job priorities |

### Connection

The MongoDB store expects a `MongoClient` CDI bean. How you produce it depends on your environment:

**WildFly / JBoss with a managed connection:**
```java
@Produces
@ApplicationScoped
public MongoClient mongoClient() {
    return MongoClients.create("mongodb://localhost:27017");
}
```

**With connection string from environment:**
```java
@Produces
@ApplicationScoped
public MongoClient mongoClient(
        @ConfigProperty(name = "mongodb.uri") String uri) {
    return MongoClients.create(uri);
}
```

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
