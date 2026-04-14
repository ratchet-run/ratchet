---
sidebar_position: 5
title: Cluster Configuration
description: Running Ratchet across multiple nodes — ClusterCoordinator, NodeIdentityProvider, recurring job deduplication, and distributed locking.
---

# Cluster Configuration

Ratchet supports multi-node deployments where multiple application instances share the same database. This guide covers how to configure clustering for safe job claiming, recurring job deduplication, and node coordination.

## How Clustering Works

In a clustered Ratchet deployment:

1. **Job claiming is safe by default** — The database handles concurrency via row-level locking (`SELECT ... FOR UPDATE SKIP LOCKED` on PostgreSQL, InnoDB row locks on MySQL, atomic updates on MongoDB). No additional configuration is needed for one-shot jobs.

2. **Recurring jobs need coordination** — Without clustering, every node independently fires recurring jobs, leading to duplicates. The `ClusterCoordinator` SPI prevents this by designating one node to schedule each recurring job.

3. **Node identity is tracked** — Each node registers itself in the `scheduler_node` table with a heartbeat. This enables distributed locking and stale-node detection.

## Enabling Cluster Mode

There is no separate `ratchet.cluster.enabled` switch. A deployment becomes clustered when multiple Ratchet nodes share the same store and you provide the coordination pieces you need. In practice that means:

- Node heartbeat registration in `scheduler_node`
- Distributed lock acquisition via `scheduler_lock`
- Recurring job leader election
- Cross-node wakeup notifications (if a `ClusterCoordinator` is provided)

## Node Identity

Each node in the cluster needs a unique, stable identifier. Implement the `NodeIdentityProvider` SPI:

```java
@ApplicationScoped
public class KubernetesNodeProvider implements NodeIdentityProvider {

  @Override
  public String getNodeId() {
    // Kubernetes pod name is stable within a StatefulSet
    String podName = System.getenv("HOSTNAME");
    return podName != null ? podName : InetAddress.getLocalHost().getHostName();
  }
}
```

The default implementation uses the JVM hostname. Override it when:
- Running in Kubernetes (use the pod name from `metadata.name`)
- Running in Docker Compose (use the `HOSTNAME` environment variable)
- Running multiple instances on the same host (use a unique port or instance ID)

There is no built-in `ratchet.cluster.node-id` property. Override node identity by providing your own `NodeIdentityProvider` bean when the hostname-based default is not sufficient.

## ClusterCoordinator SPI

The `ClusterCoordinator` interface coordinates work distribution across nodes:

```java
@Incubating
public interface ClusterCoordinator {

  /**
   * Notifies the cluster that new work is available at the given priority level.
   * Other nodes can use this signal to wake up their polling engines immediately.
   */
  void notifyNewWork(JobPriority priority);

  /**
   * Registers a listener that is called when another node signals new work.
   * The polling engine uses this to wake up and check for available jobs.
   */
  void registerWakeupListener(Runnable listener);
}
```

### Database-Only Coordination

If you do not provide a `ClusterCoordinator`, Ratchet falls back to database-only coordination. Each node polls independently, and recurring job deduplication relies on the `scheduler_lock` table. This works but has higher latency — nodes discover new work only on their next poll cycle.

### Redis-Based Coordinator

A Redis-based implementation provides low-latency cross-node notifications:

```java
@ApplicationScoped
public class RedisClusterCoordinator implements ClusterCoordinator {

  @Inject
  RedisClient redis;

  private final List<Runnable> listeners = new CopyOnWriteArrayList<>();

  @PostConstruct
  void init() {
    // Subscribe to wakeup channel in a background thread
    redis.subscribe("ratchet:wakeup", message -> {
      listeners.forEach(Runnable::run);
    });
  }

  @Override
  public void notifyNewWork(JobPriority priority) {
    redis.publish("ratchet:wakeup", priority.name());
  }

  @Override
  public void registerWakeupListener(Runnable listener) {
    listeners.add(listener);
  }
}
```

### JMS/Messaging-Based Coordinator

For environments that already have a message broker (ActiveMQ, RabbitMQ):

```java
@ApplicationScoped
public class JmsClusterCoordinator implements ClusterCoordinator {

  @Inject
  @JMSConnectionFactory("java:/ConnectionFactory")
  JMSContext jms;

  @Resource(lookup = "java:/jms/topic/ratchet-wakeup")
  Topic wakeupTopic;

  private final List<Runnable> listeners = new CopyOnWriteArrayList<>();

  @PostConstruct
  void init() {
    JMSConsumer consumer = jms.createConsumer(wakeupTopic);
    consumer.setMessageListener(msg -> {
      listeners.forEach(Runnable::run);
    });
  }

  @Override
  public void notifyNewWork(JobPriority priority) {
    jms.createProducer().send(wakeupTopic, priority.name());
  }

  @Override
  public void registerWakeupListener(Runnable listener) {
    listeners.add(listener);
  }
}
```

## Distributed Locking

Ratchet uses the `scheduler_lock` table for distributed coordination:

```sql
-- PostgreSQL schema (created by the DDL)
CREATE TABLE IF NOT EXISTS scheduler_lock (
    lock_name  VARCHAR(128) NOT NULL,
    owner_node VARCHAR(64)  NOT NULL,
    locked_at  TIMESTAMPTZ(6) NOT NULL,
    expires_at TIMESTAMPTZ(6) NOT NULL,
    CONSTRAINT pk_scheduler_lock PRIMARY KEY (lock_name)
);
```

Locks are acquired with an expiration time. If a node crashes, its locks expire and another node can acquire them. This prevents deadlocks from node failures.

### How Recurring Job Deduplication Works

1. When a recurring job's `next_fire` time arrives, every node detects it during polling
2. Each node attempts to acquire a distributed lock for that job's cron expression
3. Only the node that successfully acquires the lock schedules the next instance
4. The lock expires after a configurable duration, allowing another node to take over if the leader fails

## Node Heartbeats

Each node maintains a heartbeat in the `scheduler_node` table:

```sql
-- Registered automatically when cluster mode is enabled
SELECT * FROM scheduler_node;

-- Example output:
-- node_id          | heartbeat_ts             | started_at               | node_info
-- ratchet-node-0   | 2026-03-31 10:00:05.123  | 2026-03-31 08:00:00.000  | WildFly 39.0.1
-- ratchet-node-1   | 2026-03-31 10:00:04.456  | 2026-03-31 08:00:01.000  | WildFly 39.0.1
-- ratchet-node-2   | 2026-03-31 10:00:05.789  | 2026-03-31 08:00:02.000  | WildFly 39.0.1
```

Heartbeats are used to:
- Detect stale nodes that may have crashed
- Determine cluster size for scaling decisions
- Identify which node is executing which jobs (via `picked_by`)

### Stale Node Detection

A node is considered stale when its heartbeat exceeds the configured threshold. Stale nodes' running jobs will eventually time out and become eligible for retry by other nodes.

```sql
-- Find stale nodes (no heartbeat in 60 seconds)
SELECT node_id, heartbeat_ts
FROM scheduler_node
WHERE heartbeat_ts < NOW() - INTERVAL '60 seconds';
```

## Kubernetes-Specific Configuration

### StatefulSet with Cluster Mode

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: ratchet-scheduler
spec:
  serviceName: ratchet-scheduler
  replicas: 3
  selector:
    matchLabels:
      app: ratchet-scheduler
  template:
    metadata:
      labels:
        app: ratchet-scheduler
    spec:
      containers:
      - name: app
        image: myapp:latest
        env:
        - name: HOSTNAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: RATCHET_CLUSTER_ENABLED
          value: "true"
```

Pod names (`ratchet-scheduler-0`, `ratchet-scheduler-1`, `ratchet-scheduler-2`) are used as stable node identifiers.

### Scaling Considerations

- **Scaling up**: New nodes start polling and claiming jobs immediately. No manual intervention needed.
- **Scaling down**: Ensure graceful shutdown to let running jobs complete. Set `terminationGracePeriodSeconds` to allow in-flight jobs to finish.
- **Pod disruption**: Use a `PodDisruptionBudget` to prevent too many nodes from being evicted simultaneously.

```yaml
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: ratchet-pdb
spec:
  minAvailable: 1
  selector:
    matchLabels:
      app: ratchet-scheduler
```

## Failure Modes

### Node Failure

If a node crashes while executing jobs:
1. The node's heartbeat stops updating in `scheduler_node`
2. Jobs it was executing remain in `RUNNING` status
3. When the job timeout expires, jobs are marked `FAILED`
4. If retries are configured, failed jobs become `PENDING` and are claimed by surviving nodes
5. Distributed locks held by the dead node expire, allowing other nodes to acquire them

### Network Partition

In a network split:
- Each partition continues executing one-shot jobs independently (safe, since the database prevents double-claiming)
- Recurring job scheduling may temporarily duplicate if the partition separates a node from the database
- After the partition heals, the system self-corrects — lock expiration and idempotency keys prevent lasting inconsistency

### Split-Brain Prevention

Ratchet relies on the database as the single source of truth. All coordination goes through the database (or through `ClusterCoordinator` for notifications only). This means split-brain is limited to the notification layer — the database always arbitrates who executes what.

## Monitoring Cluster Health

### Active Nodes

```sql
SELECT node_id, heartbeat_ts, started_at
FROM scheduler_node
WHERE heartbeat_ts > NOW() - INTERVAL '30 seconds'
ORDER BY started_at;
```

### Jobs Per Node

```sql
SELECT picked_by AS node, COUNT(*) AS running_jobs
FROM scheduler_job
WHERE status = 'RUNNING'
GROUP BY picked_by;
```

### Lock Status

```sql
SELECT lock_name, owner_node, locked_at, expires_at
FROM scheduler_lock
WHERE expires_at > NOW()
ORDER BY locked_at;
```

## See Also

- [Deployment Overview](/docs/deployment/overview) — General deployment guidance
- [Kubernetes Deployment](/docs/deployment/kubernetes) — StatefulSet and pod configuration
- [Performance Tuning](/docs/deployment/performance-tuning) — Tuning polling and thread pools
- [Clustering & Distributed Execution](/docs/deployment/clustering) — Conceptual overview of clustering
- [Monitoring & Observability](/docs/deployment/monitoring) — Metrics and health checks
