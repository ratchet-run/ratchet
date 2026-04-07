---
sidebar_position: 8
title: Clustering
description: Running Ratchet across multiple nodes — claim-based execution, node heartbeats, wakeup coordination, and failure detection
---

# Clustering

Ratchet runs on multiple nodes without additional coordination infrastructure. The database is the shared state, and `SKIP LOCKED` ensures no two nodes claim the same job. For enhanced responsiveness, the optional `ClusterCoordinator` SPI enables cross-node wakeup notifications.

## Architecture

```
     ┌──────────┐     ┌──────────┐     ┌──────────┐
     │  Node A  │     │  Node B  │     │  Node C  │
     │          │     │          │     │          │
     │ Poller   │     │ Poller   │     │ Poller   │
     │ Workers  │     │ Workers  │     │ Workers  │
     └────┬─────┘     └────┬─────┘     └────┬─────┘
          │                │                │
          │  SKIP LOCKED   │  SKIP LOCKED   │  SKIP LOCKED
          │                │                │
          └────────────────┼────────────────┘
                           │
                    ┌──────┴──────┐
                    │   Database  │
                    │             │
                    │ scheduler_  │
                    │   job       │
                    │ scheduler_  │
                    │   node      │
                    │ scheduler_  │
                    │   lock      │
                    └─────────────┘
```

Every node runs its own poller and workers. No node is special — there is no "master" or "coordinator." The database is the single source of truth.

## How Job Claiming Works

When a node's poller fires, it runs a `SELECT ... FOR UPDATE SKIP LOCKED` query:

```sql
SELECT * FROM scheduler_job
WHERE status = 'PENDING'
  AND scheduled_time <= NOW()
ORDER BY priority DESC, scheduled_time ASC
FOR UPDATE SKIP LOCKED
LIMIT :batchSize;
```

- **`FOR UPDATE`** locks the selected rows
- **`SKIP LOCKED`** skips rows already locked by another node's in-flight claim

This means each node gets a disjoint set of jobs — no duplicate execution, no distributed lock manager, no coordination protocol. The database handles it.

### Optimized Claiming

Ratchet provides two claim paths:

| Method | Returns | Use case |
|--------|---------|----------|
| `claimNextBatch(limit, nodeId)` | Full `JobEntity` with payload | When you need the complete job immediately |
| `claimNextBatchOptimized(limit, nodeId)` | Lightweight `JobClaimDto` | When you want to claim fast and load payloads lazily |

The optimized path skips deserializing large payload blobs during the claim query, reducing lock hold time in high-throughput clusters.

### Recurring Job Claiming

Recurring master jobs have their own claim method:

```java
claimDueRecurring(int limit, String nodeId)
```

This selects recurring jobs whose `next_fire_time` has arrived, claims them, and the engine creates a child instance for each execution cycle.

## Node Identity and Heartbeats

Each node registers itself in the `scheduler_node` table:

| Column | Description |
|--------|-------------|
| `node_id` | Unique identifier (hostname, UUID, or configured value) |
| `last_heartbeat` | Last time this node checked in |
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

Jobs that were claimed by a dead node (status `RUNNING`, `picked_by` set to the dead node) can be reclaimed after the timeout expires — the poller will pick them up on the next cycle.

### Clock Skew

The `NodeStore.getDatabaseTime()` method returns the database server's current timestamp. Nodes can compare this against their local clock to detect skew. Significant skew (>1 second) should be logged as a warning, since scheduling accuracy depends on clocks being reasonably synchronized.

## Cluster Wakeup Notifications

By default, each node polls on its own interval. For time-sensitive jobs (CRITICAL priority, immediate submissions), Ratchet supports cross-node wakeup via the `ClusterCoordinator` SPI:

```java
public interface ClusterCoordinator {
    void notifyNewWork(JobPriority priority);
    void registerWakeupListener(Runnable listener);
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

Out of the box, Ratchet uses `NoOpClusterCoordinator`, which does nothing. This is correct for single-node deployments and acceptable for multi-node deployments where poll-interval latency is tolerable.

### Example: JGroups Implementation

```java
@ApplicationScoped
@Alternative
@Priority(1000)
public class JGroupsClusterCoordinator implements ClusterCoordinator {

    private JChannel channel;
    private final List<Runnable> listeners = new CopyOnWriteArrayList<>();

    @PostConstruct
    void init() throws Exception {
        channel = new JChannel("jgroups-config.xml");
        channel.setReceiver(new ReceiverAdapter() {
            @Override
            public void receive(Message msg) {
                listeners.forEach(Runnable::run);
            }
        });
        channel.connect("ratchet-cluster");
    }

    @Override
    public void notifyNewWork(JobPriority priority) {
        try {
            channel.send(new ObjectMessage(null, "WAKEUP"));
        } catch (Exception e) {
            // Log and continue — wakeup is best-effort
        }
    }

    @Override
    public void registerWakeupListener(Runnable listener) {
        listeners.add(listener);
    }
}
```

## Priority Boosting

Long-waiting low-priority jobs get promoted automatically. The store periodically boosts jobs that have been pending longer than `SCHEDULER_PRIORITY_BOOST_INTERVAL_MINUTES` (default: 15):

- `LOWEST` → `LOW`
- `LOW` → `NORMAL`
- `NORMAL` → `HIGH`
- `HIGH` → `CRITICAL`

This prevents starvation in clusters with sustained high-priority load.

## Distributed Locks

The `scheduler_lock` table provides advisory locks for operations that must be cluster-wide singletons (e.g., recurring job registration, maintenance tasks):

| Column | Description |
|--------|-------------|
| `lock_name` | Unique lock identifier |
| `locked_by` | Node that holds the lock |
| `locked_at` | When the lock was acquired |
| `expires_at` | TTL — lock auto-expires for crash safety |

For MongoDB, the lock collection uses a TTL index on `expires_at`, so expired locks are garbage-collected automatically.

## Sizing Guidelines

| Cluster size | Poll interval | Batch size | Notes |
|-------------|--------------|-----------|-------|
| 1 node | 1–5s | 10–50 | Single-node, no coordination needed |
| 2–5 nodes | 2–5s | 10–20 | `SKIP LOCKED` handles contention well |
| 5–20 nodes | 5–10s | 5–10 | Reduce batch size to spread work evenly |
| 20+ nodes | 10–30s | 5 | Consider `ClusterCoordinator` for latency-sensitive work |

The key tradeoff: shorter poll intervals mean lower latency but more database load. `ClusterCoordinator` lets you keep a longer default interval while still responding immediately to urgent work.

## See Also

- [Cluster Configuration](./cluster-configuration.md) — Tuning poll intervals, thread pools, and batch sizes
- [Performance Tuning](./performance-tuning.md) — Database indexing and query optimization
- [Concepts: Clustering](../concepts/clustering.md) — Architectural deep-dive into the clustering model
