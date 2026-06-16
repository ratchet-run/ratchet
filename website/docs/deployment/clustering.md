---
sidebar_position: 8
title: Clustering
description: "Running Ratchet across multiple nodes: claim-based execution, node heartbeats, wakeup coordination, and failure detection"
---

# Clustering

Ratchet can run on multiple nodes against the same store. Ordinary job claiming is coordinated through the database, `SKIP LOCKED` ensures no two nodes claim the same job, and store-backed leases serialize destructive startup cleanup. `ClusterCoordinator` remains optional and is only for fast cross-node wakeups.

## Architecture

<div class="docs-diagram" role="img" aria-label="Ratchet multi-node deployment: every node runs a local poller and worker pool, and all nodes coordinate through the shared store using SKIP LOCKED, node heartbeats, and scheduler locks.">
  <div class="docs-diagram-row">
    <div class="docs-diagram-card docs-diagram-card--active">
      <strong>Node A</strong>
      <small>Local poller and worker pool.</small>
    </div>
    <div class="docs-diagram-card docs-diagram-card--active">
      <strong>Node B</strong>
      <small>Claims a different subset of work.</small>
    </div>
    <div class="docs-diagram-card docs-diagram-card--active">
      <strong>Node C</strong>
      <small>No master/coordinator node required.</small>
    </div>
  </div>

  <div class="docs-diagram-connector">
    <span>`SKIP LOCKED` prevents duplicate claims while keeping nodes non-blocking</span>
  </div>

  <div class="docs-diagram-row">
    <div class="docs-diagram-card docs-diagram-card--store">
      <strong>scheduler_job_queue</strong>
      <small>Live claim state for pending, running, paused, and waiting jobs.</small>
    </div>
    <div class="docs-diagram-card docs-diagram-card--store">
      <strong>scheduler_node</strong>
      <small>Heartbeats and failure detection.</small>
    </div>
    <div class="docs-diagram-card docs-diagram-card--store">
      <strong>scheduler_lock</strong>
      <small>Store-backed leases for singleton maintenance paths.</small>
    </div>
  </div>
</div>

Every node runs its own poller and workers. For ordinary job claiming, no node is special: the database is the single source of truth.

## How Job Claiming Works

When a node's poller fires, it runs a `SELECT ... FOR UPDATE SKIP LOCKED` query:

```sql
SELECT job_id FROM scheduler_job_queue
WHERE status = 'PENDING'
  AND scheduled_time <= NOW()
ORDER BY (priority + FLOOR(GREATEST(0, overdue_minutes) / :boostInterval)) DESC,
         scheduled_time ASC,
         job_id ASC
FOR UPDATE SKIP LOCKED
LIMIT :batchSize;
```

- **`FOR UPDATE`** locks the selected rows
- **`SKIP LOCKED`** skips rows already locked by another node's in-flight claim

This means each node gets a disjoint set of jobs with no duplicate execution and no distributed lock manager needed. The database handles it.

### Optimized Claiming

Ratchet provides two claim paths:

| Method | Returns | Use case |
|--------|---------|----------|
| `claimNextBatch(limit, nodeId)` | Full `JobEntity` with payload | When you need the complete job immediately |
| `claimNextBatchOptimized(jobType, limit, nodeId)` | Lightweight `JobClaimDto` | When you want to claim fast and load payloads lazily |

The optimized path skips deserializing large payload blobs during the claim query, reducing lock hold time in high-throughput clusters.

### Recurring Job Claiming

Recurring master jobs have their own claim method:

```java
claimDueRecurring(int limit, String nodeId)
```

This selects recurring jobs whose `next_fire` has arrived, claims them, and the engine creates a child instance for each execution cycle.

## Node Identity and Heartbeats

Each node registers itself in the `scheduler_node` table:

| Column | Description |
|--------|-------------|
| `node_id` | Unique identifier (hostname, UUID, or configured value) |
| `heartbeat_ts` | Last time this node checked in |
| `started_at` | When the node first registered |
| `node_info` | Optional JSON metadata (version, IP, etc.) |

The engine calls `NodeStore.upsertHeartbeat(nodeId, timestamp)` periodically. This creates the record on first call and updates it thereafter.

### Failure Detection

Stale heartbeats indicate a dead or partitioned node:

```java
// Find nodes that haven't heartbeated in 5 minutes
List<NodeEntity> dead = nodeStore.findInactiveNodesSince(
    Instant.now().minus(Duration.ofMinutes(5))
);

// Clean up dead node records
nodeStore.deleteInactiveNodesSince(cutoff);
```

Jobs that were claimed by a dead node (status `RUNNING`, `picked_by` set to the dead node) can be reclaimed after the timeout expires. The poller will pick them up on the next cycle.

### Clock Skew

The `NodeStore.getDatabaseTime()` method returns the database server's current timestamp. Nodes can compare this against their local clock to detect skew. Significant skew (>1 second) should be logged as a warning, since scheduling accuracy depends on clocks being reasonably synchronized.

## Cluster Wakeup Notifications

By default, each node polls on its own interval. For time-sensitive jobs (CRITICAL priority, immediate submissions), Ratchet supports cross-node wakeup via the `ClusterCoordinator` SPI:

```java
public interface ClusterCoordinator extends AutoCloseable {
    void notifyNewWork(JobPriority priority, NodeIdentity source, String executionTarget);
    void registerWakeupListener(Consumer<JobWakeupHint> listener);
    void close();
}
```

### How Wakeup Works

1. A job is submitted on Node A with `CRITICAL` priority (or `.immediate()`)
2. After the transaction commits, `JobWakeupService` publishes a `JobWakeupNotification`
3. The `ClusterCoordinator` broadcasts it across nodes (JGroups, JMS, Redis pub/sub, etc.)
4. All nodes receive the notification and immediately wake their pollers

### Wakeup Triggers

Not every job triggers a wakeup. The `JobWakeupService` only notifies for:

- **CRITICAL** priority jobs
- **Single** jobs submitted with zero delay via user-triggered enqueue
- **Batch parent** jobs (to start child distribution quickly)

Normal and low-priority jobs wait for the next poll cycle.

### Default: No-Op

Out of the box, Ratchet uses `NoOpClusterCoordinator`. That is fine for any deployment that can tolerate poll-interval latency for cross-node wakeups, because correctness still comes from the store.

### First-party coordinator modules

To enable push-based wakeups, add one of the first-party coordinator modules: PostgreSQL `LISTEN`/`NOTIFY`, JMS, Hazelcast, or Infinispan/JGroups. They are opt-in by dependency (no `beans.xml` change) and share a common delivery, self-suppression, failure, and metrics contract. See [Cluster Coordinators](/deployment/cluster-coordinators) for setup, configuration, delivery guarantees, and the polling fallback floor.

To build your own transport instead, implement the `ClusterCoordinator` SPI. See [Clustering concepts](/concepts/clustering).

## Priority Boosting

Long-waiting low-priority jobs get promoted automatically. Each claim orders by raw priority plus `floor(wait_minutes / priorityBoostIntervalMinutes)` (default: 15).

Boosting is part of claim ordering only; persisted priority is not rewritten. Set `RatchetOptions.builder().store(s -> s.priorityBoostIntervalMinutes(0))` to disable the boost.

## Distributed Locks

The `scheduler_lock` table provides advisory locks for operations that must be cluster-wide singletons (e.g., recurring job registration, maintenance tasks):

| Column | Description |
|--------|-------------|
| `lock_name` | Unique lock identifier |
| `owner_node` | Node that holds the lock |
| `locked_at` | When the lock was acquired |
| `expires_at` | TTL; lock auto-expires for crash safety |

For MongoDB, the lock collection uses a TTL index on `expires_at`, so expired locks are garbage-collected automatically.

## Sizing Guidelines

| Cluster size | Poll interval | Batch size | Notes |
|-------------|--------------|-----------|-------|
| 1 node | 1–5s | 10–50 | Single-node, no coordination needed |
| 2–5 nodes | 2–5s | 10–20 | `SKIP LOCKED` handles contention well |
| 5–20 nodes | 5–10s | 5–10 | Reduce batch size to spread work evenly |
| 20+ nodes | 10–30s | 5 | Consider `ClusterCoordinator` for latency-sensitive work |

The key tradeoff: shorter poll intervals mean lower latency but more database load. `ClusterCoordinator` lets you keep a longer default interval while still responding immediately to urgent work.

## See also

- [Cluster Configuration](./cluster-configuration.md) -- Tuning poll intervals, thread pools, and batch sizes
- [Performance Tuning](./performance-tuning.md) -- Database indexing and query optimization
- [Concepts: Clustering](../concepts/clustering.md) -- Architectural deep-dive into the clustering model
