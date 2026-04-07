---
title: Configuration
---

# Configuration

Tuning Ratchet for your deployment.

## Environment Properties

Ratchet respects Jakarta EE configuration properties. Set via:
- Environment variables (`RATCHET_*`)
- System properties (`-D ratchet.*`)
- `microprofile-config.properties`
- Your runtime's configuration mechanism

## Executor Configuration

### Thread Pool Size

```properties
ratchet.executor.threads=16
```

Number of threads for job execution. Defaults to # CPU cores.

For I/O-bound jobs, use more threads:
```properties
ratchet.executor.threads=100
```

### Virtual Threads (Java 21+)

```properties
ratchet.executor.use-virtual-threads=true
```

Enables virtual threads (Project Loom) for better scalability.

## Polling Configuration

### Poll Interval

```properties
ratchet.polling.interval=5000  # milliseconds
```

How often the polling engine checks for new jobs. Lower = more responsive but more DB queries.

Default: 5000ms (5 seconds)

### Batch Poll Size

```properties
ratchet.polling.batch-size=100
```

How many jobs to fetch in a single poll. Higher = fewer queries but more memory.

Default: 100

### Adaptive Polling

```properties
ratchet.polling.adaptive=true
```

Automatically adjust poll interval based on job queue depth. When busy, poll more frequently.

Default: true

## Retention & Cleanup

### Job Retention

```properties
ratchet.retention.completed-days=30
```

Delete completed jobs older than this many days.

Default: 30 days

### Archive DLQ

```properties
ratchet.dlq.archive-after-days=365
```

Move DLQ jobs to archive after this many days.

Default: 365 days

## Serialization

### Custom Serialization Strategy

```properties
ratchet.serialization.strategy=com.myapp.ProtoSerializationStrategy
```

Use a custom `SerializationStrategy` for payload serialization.

Default: Jackson JSON

## Security

### Deserialization Class Policy

```properties
ratchet.security.class-policy=com.myapp.StrictClassPolicy
```

Use a custom `ClassPolicy` to control which classes can be deserialized.

Default: Allow all

### Error Sanitization

```properties
ratchet.security.error-sanitizer=com.myapp.PiiSanitizer
```

Use a custom `ErrorSanitizer` to scrub sensitive data from errors.

Default: Pass-through (no sanitization)

## Clustering

### Cluster Mode

```properties
ratchet.cluster.enabled=true
ratchet.cluster.coordinator=com.myapp.RedisClusterCoordinator
```

Enable clustering for distributed recurring jobs. Provide a `ClusterCoordinator` implementation.

Default: Single-node (no clustering)

### Node Identity

```properties
ratchet.cluster.node-id=pod-1-abc123
```

Override the node identifier (hostname by default).

## Metrics

### Metrics Collector

```properties
ratchet.metrics.collector=io.micrometer.MicrometerMetricsCollector
```

Use a custom `MetricsCollector` for metrics reporting.

Default: No-op (no metrics)

### Metrics Export

If using Micrometer:

```properties
management.endpoints.web.exposure.include=metrics
```

## Database-Specific Settings

### PostgreSQL

```properties
ratchet.store.postgresql.batch-size=500
```

Number of rows to batch insert.

### MySQL

```properties
ratchet.store.mysql.use-dynamic-priority=true
```

Enable MySQL generated columns for dynamic priority boosting.

### MongoDB

```properties
ratchet.store.mongodb.uri=mongodb://localhost:27017/ratchet
```

Connection string for MongoDB.

## Logging

### Job Logger Level

```properties
ratchet.logging.level=INFO
```

Log level for Ratchet job execution (INFO, DEBUG, TRACE, WARN, ERROR).

## Example: Complete Configuration

```properties
# Executor
ratchet.executor.threads=32
ratchet.executor.use-virtual-threads=true

# Polling
ratchet.polling.interval=3000
ratchet.polling.batch-size=200
ratchet.polling.adaptive=true

# Retention
ratchet.retention.completed-days=14
ratchet.dlq.archive-after-days=90

# Security
ratchet.security.class-policy=com.acme.AppClassPolicy
ratchet.security.error-sanitizer=com.acme.AcmeSanitizer

# Clustering
ratchet.cluster.enabled=true
ratchet.cluster.coordinator=com.acme.KubernetesClusterCoordinator

# Metrics
ratchet.metrics.collector=io.micrometer.MicrometerMetricsCollector
```

## See Also

- [Deployment Overview](/docs/deployment/overview)
- [Clustering](/docs/concepts/clustering)
- [Database Setup](/docs/deployment/database-setup)
