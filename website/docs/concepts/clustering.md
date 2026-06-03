---
sidebar_position: 11
title: Clustering
description: ClusterCoordinator SPI, database-backed coordination, node identity, and multi-node consistency
---

# Clustering

Ratchet is designed to run on multiple nodes without additional coordination infrastructure. The database serves as the shared state, `SKIP LOCKED` ensures safe concurrent job claiming, and `scheduler_lock` provides singleton execution for recurring scans. For enhanced responsiveness, the `ClusterCoordinator` SPI enables cross-node wakeup notifications.

## Multi-Node Architecture

<div className="docs-diagram" role="img" aria-label="Ratchet multi-node architecture: each application node runs its own poller and workers, and all nodes coordinate through the shared store using SKIP LOCKED, node heartbeats, and scheduler locks.">
  <div className="docs-diagram-row">
    <div className="docs-diagram-card docs-diagram-card--active">
      <strong>Node A</strong>
      <small>Poller + workers claim their own jobs.</small>
    </div>
    <div className="docs-diagram-card docs-diagram-card--active">
      <strong>Node B</strong>
      <small>Runs independently; no master node.</small>
    </div>
    <div className="docs-diagram-card docs-diagram-card--active">
      <strong>Node C</strong>
      <small>Uses the same store-backed coordination rules.</small>
    </div>
  </div>

  <div className="docs-diagram-connector">
    <span>`SELECT ... FOR UPDATE SKIP LOCKED` gives each node a disjoint claim set</span>
  </div>

  <div className="docs-diagram-row">
    <div className="docs-diagram-card docs-diagram-card--store">
      <strong>scheduler_job_queue</strong>
      <small>Hot PENDING/RUNNING/PAUSED/WAITING state and claim ownership.</small>
    </div>
    <div className="docs-diagram-card docs-diagram-card--store">
      <strong>scheduler_node</strong>
      <small>Node registration and heartbeat timestamps.</small>
    </div>
    <div className="docs-diagram-card docs-diagram-card--store">
      <strong>scheduler_lock</strong>
      <small>Leases for singleton recurring scans and startup cleanup.</small>
    </div>
  </div>
</div>

Each node runs its own Poller, claims its own subset of jobs, and executes them independently. No node is special -- there is no "master" or "coordinator" node. The database is the single source of truth.

## How SKIP LOCKED Provides Cluster Safety

The core guarantee: **no two nodes will claim the same job**. This is enforced at the database level through `SELECT ... FOR UPDATE SKIP LOCKED`:

```sql
-- Node A runs:
SELECT job_id FROM scheduler_job_queue
WHERE status = 'PENDING' AND scheduled_time <= NOW()
ORDER BY (priority + age_boost) DESC, scheduled_time ASC
FOR UPDATE SKIP LOCKED
LIMIT 50;

-- Node B runs the same query simultaneously:
-- Jobs locked by Node A are silently skipped
-- Node B gets a disjoint set of jobs
```

SQL stores use `SKIP LOCKED`, and the MongoDB store uses atomic document updates. These are the foundation of Ratchet's multi-node execution -- no external coordination service is required for ordinary job claiming.

## StartupCoordinator

Destructive startup tasks are coordinated separately from wakeups. Ratchet's default `StartupCoordinator` uses a store-backed lease so only one node performs recurring-annotation orphan cleanup during startup:

```java
@Incubating
public interface StartupCoordinator {
    boolean tryAcquire(String actionName, Duration leaseTtl);
    void release(String actionName);
}
```

This is distinct from `ClusterCoordinator`: startup cleanup uses store-backed leases by default, while `ClusterCoordinator` remains optional and wakeup-focused.

## ClusterCoordinator SPI

While `SKIP LOCKED` guarantees correctness, it doesn't guarantee responsiveness. Without coordination, a job submitted on Node A won't be noticed by Node B until Node B's next poll cycle (which could be up to 60 seconds in deep idle).

The `ClusterCoordinator` SPI bridges this gap:

```java
@Incubating
public interface ClusterCoordinator extends AutoCloseable {
    void notifyNewWork(JobPriority priority, NodeIdentity source, String executionTarget);
    void registerWakeupListener(Consumer<JobWakeupHint> listener);
    void close();
}
```

### How It Works

1. When a job is submitted with immediate or CRITICAL priority, the engine calls `ClusterCoordinator.notifyNewWork(priority, source, executionTarget)`
2. The coordinator broadcasts this notification to all cluster nodes
3. Each node's Poller has registered a wakeup listener via `registerWakeupListener()`
4. When the notification arrives, the Poller exits deep idle and enters burst mode (500ms polling)

### Default: NoOpClusterCoordinator

The default implementation does nothing -- wakeup notifications are only delivered locally. This is correct for single-node deployments and acceptable for multi-node deployments where sub-second latency isn't critical.

With the no-op coordinator, jobs submitted on Node A will be picked up by Node B at the next natural poll cycle. The adaptive polling algorithm ensures this happens within seconds during active periods, or up to 60 seconds during deep idle.

### First-party coordinator modules

For push-based wakeups you normally do not write any code: add one of the shipped coordinator modules (PostgreSQL `LISTEN`/`NOTIFY`, JMS, Hazelcast, or Infinispan) and it activates by dependency. See the [Cluster Coordinators](/deployment/cluster-coordinators) operator guide for setup, delivery guarantees, and the polling fallback floor.

### Implementing a custom ClusterCoordinator

When none of the shipped modules fit, you can implement `ClusterCoordinator` using any messaging technology available in your environment:

**JMS-based (Jakarta EE native):** publish a best-effort wakeup message to a shared topic and
invoke registered listeners from a message-driven bean or other container-managed consumer.

**Infinispan/JGroups-based:**

```java
@Alternative
@Priority(APPLICATION)
@ApplicationScoped
public class InfinispanClusterCoordinator implements ClusterCoordinator {

    @Inject
    private CacheContainer cacheContainer;

    private Consumer<JobWakeupHint> wakeupListener;

    @Override
    public void notifyNewWork(JobPriority priority, NodeIdentity source, String executionTarget) {
        // Publish to Infinispan cluster-wide topic
        cacheContainer.getCache("ratchet-wakeup")
            .put("wakeup-" + System.currentTimeMillis(), priority.name());
    }

    @Override
    public void registerWakeupListener(Consumer<JobWakeupHint> listener) {
        this.wakeupListener = listener;
        // Register Infinispan listener for cache events
    }

    @Override
    public void close() {
        // Release Infinispan resources
    }
}
```

The SPI is intentionally minimal -- `notifyNewWork`, `registerWakeupListener`, and `close` -- so it can be implemented over any pub/sub mechanism.

## Node Identity

Each node needs a unique identifier for job claiming (`picked_by`), distributed locks, and heartbeat registration. The `NodeIdentityProvider` SPI provides this:

```java
@Incubating
public interface NodeIdentityProvider {
    String getNodeId();
}
```

### Default: DefaultNodeIdentityProvider

The default implementation constructs the node ID from `hostname + PID`, providing uniqueness across machines and across multiple instances on the same machine.

### Custom Implementation

For environments where hostname-based IDs aren't unique or stable (e.g., containers with synthetic hostnames), provide a custom implementation:

```java
@Alternative
@Priority(APPLICATION)
@ApplicationScoped
public class KubernetesNodeIdentityProvider implements NodeIdentityProvider {

    private final String nodeId;

    public KubernetesNodeIdentityProvider() {
        // Use Kubernetes pod name as node ID
        this.nodeId = System.getenv("HOSTNAME");
    }

    @Override
    public String getNodeId() {
        return nodeId;
    }
}
```

The node ID must be:
- **Unique** across all nodes in the cluster
- **Stable** for the lifetime of the application instance
- **Reasonable length** (stored in `VARCHAR(64)` columns)

## Node Registration and Heartbeat

Nodes register themselves in the `scheduler_node` table on startup and update their `heartbeat_ts` timestamp periodically via heartbeats. The `DynamicHeartbeatCalculator` adjusts the heartbeat interval based on system load.

| `node_id` | `heartbeat_ts` | `started_at` |
|-----------|----------------|--------------|
| `node-a` | `2024-03-15 10:00:05` | `2024-03-15 09:58:00` |
| `node-b` | `2024-03-15 10:00:03` | `2024-03-15 09:58:30` |
| `node-c` | `2024-03-15 09:55:00` | `2024-03-15 09:40:00` |

## Orphan Recovery

If a node crashes without gracefully shutting down, its jobs remain in RUNNING status with `picked_by` set to the dead node. The `OrphanRecoveryTimer` detects these orphaned jobs:

1. Scans for RUNNING jobs whose `picked_by` node hasn't heartbeated recently
2. Resets orphaned jobs to PENDING status
3. Clears the `picked_by` and `picked_at` fields
4. The jobs become eligible for polling by any healthy node

This ensures jobs are never permanently lost due to node failures. The recovery interval and stale threshold are configurable.

## Distributed Locking

Certain operations must execute on only one node at a time:
- DLQ purge
- Recurring job scheduling
- Job archiving

Ratchet uses database-backed distributed locks via the `LockStore` SPI:

```java
boolean acquired = lockStore.tryLock(
    "dlqPurger",
    Duration.ofMinutes(10),
    nodeIdentityProvider.getNodeId()
);
```

The lock is stored in the `scheduler_lock` table with:
- Lock name (e.g., "dlqPurger")
- Owning node ID
- Expiration time (auto-releases if the holder crashes)

Locks expire automatically after the specified duration, preventing deadlocks if the holder crashes without releasing.

## Recurring Job Coordination

Recurring jobs require special handling in a cluster. Without coordination, multiple nodes would each create the next recurring execution, resulting in duplicate runs.

Ratchet handles this through the `RecurringScheduler`, which uses:

1. **Business key uniqueness:** Each recurring job's business key (from `@Recurring.id()` or the programmatic `withBusinessKey()`) is active-unique. Only one active (PENDING/RUNNING) job can exist with a given business key.
2. **Distributed locking:** The recurring scheduler acquires a lock before scheduling the next execution, preventing race conditions between nodes.
3. **Orphan annotation maintenance:** The `RecurringAnnotationMaintenanceService` cleans up recurring jobs from removed `@Recurring` annotations on redeployment.

### Redeployment Handling

When a new version is deployed with changed `@Recurring` annotations:

1. New recurring jobs are registered for new/modified annotations
2. Old recurring jobs for removed annotations are canceled via `cancelRecurringJobByBusinessKey()`
3. The business key ensures at-most-one active instance during the transition

```java
// Cancel recurring jobs by tag (e.g., during feature toggle)
int canceled = scheduler.cancelRecurringJobsByTag("deprecated-feature");

// Cancel by business key (e.g., replacing a specific schedule)
int canceled = scheduler.cancelRecurringJobByBusinessKey("hourly-report");
```

## Graceful Shutdown

`RatchetLifecycle` and `DrainController` coordinate graceful shutdown:

1. **Drain mode:** The `DrainController` signals the Poller to stop claiming new jobs
2. **In-flight completion:** Already-running jobs are allowed to finish within a timeout
3. **Cache cleanup:** `JobTask.clearCaches()` releases classloader references
4. **Poller stop:** The Poller's scheduling loop terminates
5. **Heartbeat stop:** Node heartbeat ceases

This prevents the "half-executed job" problem during rolling deployments. Kubernetes readiness probes can check the drain controller's status to remove the node from the load balancer before shutdown begins.

## Consistency Guarantees

| Guarantee | Mechanism |
|-----------|-----------|
| No duplicate execution | `SKIP LOCKED` + `@Version` optimistic locking |
| At-least-once delivery | Orphan recovery resets stale RUNNING jobs |
| Idempotency | `idempotency_key` UNIQUE constraint |
| Active-unique business key | Partial unique index on active statuses |
| Single recurring instance | Business key + distributed lock |
| Crash recovery | Orphan recovery timer + lock expiration |

:::info
Ratchet provides **at-least-once** delivery semantics. In rare cases (node crash after execution but before status update), a job may execute twice. If your job logic requires exactly-once semantics, implement idempotency in your business logic (e.g., using the job's idempotency key).
:::

## Related

- [Architecture Overview](./overview.md) -- Module structure and SPI overview
- [Execution Model](./execution-model.md) -- Store-backed claiming details
- [Persistence](./persistence.md) -- Node table, lock table, UUIDv7 generation
- [Scheduling](./scheduling.md) -- Recurring job scheduling and adaptive polling
