---
sidebar_position: 5
title: Configuration
description: CDI producer setup, beans.xml requirements, SPI defaults, and runtime tuning via environment variables
---

# Configuration

Ratchet is designed to work with very little configuration once the required security boundary is in place. The reference implementation ships with sensible defaults for most settings, and CDI bean discovery handles the wiring. The main required override is `ClassPolicy`: startup fails if you leave the default allowlist empty. This page covers what's happening under the hood and how to customize it when the defaults don't fit.

## How Ratchet Bootstraps

When your application starts, two CDI beans drive the initialization:

### RatchetProducer

`RatchetProducer` is an `@ApplicationScoped` CDI bean that creates and wires the internal components that require configuration values mixed with injectable dependencies. It produces beans like `ThreadPoolManager`, `JobTimeoutHandler`, `Poller`, and the default SPI implementations.

You don't need to create or configure `RatchetProducer` -- it's discovered automatically by CDI. But understanding what it produces helps you know what's available to override.

Key beans produced by `RatchetProducer`:

| Bean | What it does |
|------|-------------|
| `ThreadPoolManager` | Manages thread pools per job execution type (single, recurring, batch, chain) |
| `Poller` | Claims pending jobs from the store and dispatches them to worker threads |
| `JobTimeoutHandler` | Enforces job timeouts and soft-timeout warnings |
| `NodeIdentityProvider` | Identifies this node in a cluster with heartbeat registration |
| `ClassPolicy` | Security policy controlling which classes can be deserialized from job payloads |
| `ResilienceStrategy` | Circuit breaker backed by the built-in state machine |
| `ErrorSanitizer` | Strips PII and credentials from error messages before persistence |

### RatchetLifecycle

`RatchetLifecycle` is an `@ApplicationScoped` bean that observes the CDI `@Initialized(ApplicationScoped.class)` event to start all scheduler components at application startup, and uses `@PreDestroy` to shut them down gracefully.

Startup order:

1. **Poller** -- starts claiming and executing pending jobs
2. **RecurringScheduler** -- starts spawning due recurring job children based on cron schedules
3. **OrphanRecoveryTimer** -- periodic scan for stuck jobs from crashed nodes
4. **BatchRecoveryTimer** -- periodic scan for stuck batch completions
5. **DeadLetterService** -- schedules daily DLQ purge (if enabled)
6. **JobArchivingService** -- schedules cron-based archiving of completed jobs (if enabled)
7. **LogPurgeTimer** -- schedules cron-based log purging (if enabled)
8. **PollerWakeupListener** -- registers for cluster wakeup notifications

On shutdown, all components are stopped in reverse order, and static caches are cleared to release classloader references.

## beans.xml Requirements

Ratchet's CDI beans use `@ApplicationScoped` and are discovered through annotated bean discovery. In Jakarta CDI 4.0, the default discovery mode is `annotated`, which works out of the box. If your `beans.xml` explicitly sets a discovery mode, make sure it's either `annotated` or `all`:

```xml
<!-- src/main/webapp/WEB-INF/beans.xml -->
<?xml version="1.0" encoding="UTF-8"?>
<beans xmlns="https://jakarta.ee/xml/ns/jakartaee"
       xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
       xsi:schemaLocation="https://jakarta.ee/xml/ns/jakartaee
           https://jakarta.ee/xml/ns/jakartaee/beans_4_0.xsd"
       bean-discovery-mode="annotated">
</beans>
```

:::info No beans.xml needed in most cases
Jakarta CDI 4.0 uses `annotated` discovery by default. If you don't have a `beans.xml` at all, Ratchet will work. You only need one if your project explicitly configures discovery mode or defines CDI alternatives.
:::

### Enabling the Circuit Breaker Interceptor

If you use the `@CircuitBreakerProtected` annotation, the interceptor must be activated in `beans.xml`:

```xml
<beans xmlns="https://jakarta.ee/xml/ns/jakartaee"
       xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
       xsi:schemaLocation="https://jakarta.ee/xml/ns/jakartaee
           https://jakarta.ee/xml/ns/jakartaee/beans_4_0.xsd"
       bean-discovery-mode="annotated">
  <interceptors>
    <class>run.ratchet.ri.cdi.CircuitBreakerInterceptor</class>
  </interceptors>
</beans>
```

Without this interceptor declaration, `@CircuitBreakerProtected` annotations are silently ignored.

## SPI Defaults and How to Override Them

Ratchet's architecture is built around Service Provider Interfaces (SPIs). The reference implementation provides defaults for all of them, but you can replace any default with a CDI alternative.

### Override Pattern

To override any SPI default, provide your own `@ApplicationScoped` bean with `@Alternative` and `@Priority(APPLICATION)`:

```java
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import static jakarta.interceptor.Interceptor.Priority.APPLICATION;

@Alternative
@Priority(APPLICATION)
@ApplicationScoped
public class MyCustomClassPolicy implements ClassPolicy {

    @Override
    public boolean isAllowed(String className) {
        return className.startsWith("com.example.");
    }
}
```

CDI will select your bean over Ratchet's default. No additional configuration needed.

### SPI Reference

| SPI Interface | Default Implementation | What to override |
|---------------|----------------------|------------------|
| `RetryPolicy` | `DefaultRetryPolicy` (defers to `maxRetries` on the job) | Custom retry/no-retry decisions based on exception type or job state |
| `ResilienceStrategy` | `DefaultResilienceStrategy` (built-in 3-state circuit breaker) | Replace with Resilience4j or MicroProfile Fault Tolerance |
| `ClassPolicy` | `PackagePrefixClassPolicy` (empty allowlist by default; deployment fails fast until you override it) | Lock down which classes can be deserialized from job payloads |
| `ErrorSanitizer` | `DefaultErrorSanitizer` (strips common PII patterns) | Custom redaction rules for your domain |
| `SerializationStrategy` | `JdkSerializationStrategy` (JDK serialization for lambda payloads) | Custom payload serialization (e.g., Kryo, Protobuf) |
| `LambdaAnalyzer` | `AsmLambdaAnalyzer` (ASM bytecode analysis) | Custom method reference extraction |
| `ClusterCoordinator` | `NoOpClusterCoordinator` (no wakeup coordination) | Cross-node wakeups when you supply an implementation |
| `StartupCoordinator` | `StoreBackedStartupCoordinator` (uses `scheduler_lock`) | Gate destructive startup work behind a store-backed lease |
| `MetricsCollector` | `NoOpMetricsCollector` (discards all metrics) | Use `ratchet-micrometer` or implement for your metrics backend |
| `BeanResolver` | `CdiBeanResolver` (CDI `Instance<T>` lookup) | Custom bean instantiation for non-CDI contexts |
| `ExecutorProvider` | `DefaultExecutorProvider` (platform threads) | Virtual threads or custom thread pool configuration |
| `NodeIdentityProvider` | `DefaultNodeIdentityProvider` (hostname-based with heartbeat) | Custom node identification for cloud environments |
| `JobLogger` | No-op job-scoped logger binding | Custom structured logging |

### ClassPolicy: Security Configuration

The default `PackagePrefixClassPolicy` controls which classes can be instantiated when deserializing job payloads. Its allowlist is intentionally empty. By default, `RatchetProducer` treats that as a deployment error so you don't accidentally ship a scheduler that can never run jobs:

```
ERROR: ClassPolicy allowedPackages is empty — refusing to start. Provide an
@Alternative @Priority(APPLICATION) ClassPolicy bean with your application's package
prefixes, or opt out (ONLY for demos/tests) with
-Dratchet.allow-empty-class-policy=true
```

To fix this, create a `ClassPolicy` alternative that allows your application's packages:

```java
@Alternative
@Priority(APPLICATION)
@ApplicationScoped
public class AppClassPolicy implements ClassPolicy {

    private static final Set<String> ALLOWED = Set.of(
        "com.example.billing.",
        "com.example.inventory.",
        "com.example.notifications."
    );

    @Override
    public boolean isAllowed(String className) {
        return ALLOWED.stream().anyMatch(className::startsWith);
    }
}
```

If you explicitly set `-Dratchet.allow-empty-class-policy=true`, startup will continue but the default policy still rejects every target class. That switch is for demos and tests, not production.

This is a security boundary. Jobs execute arbitrary code by design -- the class policy ensures only your own classes can be targeted.

## Runtime Configuration via Environment Variables

`RatchetConfiguration` reads settings from environment variables (checked first) and system properties (checked second). Every setting has a `RATCHET_` prefix and a legacy `SCHEDULER_` prefix for backward compatibility.

### Poller Tuning

| Variable | Default | Description |
|----------|---------|-------------|
| `RATCHET_POLLER_BATCH_SIZE` | `50` | Number of jobs claimed per poll cycle |
| `RATCHET_POLLER_MIN_DELAY_MS` | `2000` | Minimum polling interval in milliseconds |
| `RATCHET_POLLER_MAX_DELAY_MS` | `10000` | Maximum polling interval (adaptive polling backs off to this) |
| `RATCHET_POLLER_BURST_DELAY_MS` | `500` | Polling interval when high job volume is detected |
| `RATCHET_POLLER_IDLE_THRESHOLD` | `3` | Number of empty polls before entering idle mode |
| `RATCHET_POLLER_DEEP_IDLE_DELAY_MS` | `30000` | Polling interval in deep idle mode |
| `RATCHET_POLLER_DEEP_IDLE_THRESHOLD_MS` | `60000` | Time in idle before entering deep idle |

The poller uses adaptive polling: it polls frequently when jobs are available and backs off when the queue is empty. This minimizes database load during quiet periods while maintaining responsiveness when jobs arrive.

### Thread Pool Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `RATCHET_THREAD_POOL_SIZE_SINGLE` | `20` | Worker threads for individual jobs |
| `RATCHET_THREAD_POOL_SIZE_RECURRING` | `5` | Worker threads for recurring jobs |
| `RATCHET_THREAD_POOL_SIZE_BATCH_CHILD` | `30` | Worker threads for batch child items |
| `RATCHET_THREAD_POOL_SIZE_BATCH_PARENT` | `2` | Threads for batch parent coordination |
| `RATCHET_THREAD_POOL_SIZE_CHAIN` | `10` | Worker threads for chain step execution |
| `RATCHET_THREAD_POOL_SIZE_DEFAULT` | `10` | Default thread pool (workflow branches) |
| `RATCHET_THREAD_POOL_QUEUE_SIZE` | `100` | Bounded queue size per thread pool |
| `RATCHET_WORKER_USE_VIRTUAL_THREADS` | `false` | Use Java 21 virtual threads instead of platform threads |
| `RATCHET_WORKER_DEFAULT_SLA` | `1800` | Default job timeout in seconds (30 minutes) |

### Node Health and Cluster Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `RATCHET_NODE_HEARTBEAT_INTERVAL_SECONDS` | `10` | How often this node sends heartbeats |
| `RATCHET_NODE_ORPHAN_GRACE_SECONDS` | `60` | Time before a missing node's jobs are reclaimed |
| `RATCHET_ORPHAN_SCAN_INTERVAL_MINUTES` | `5` | How often to scan for orphaned jobs |
| `RATCHET_DYNAMIC_HEARTBEAT_ENABLED` | `true` | Adjust heartbeat frequency based on load |

### Data Retention

| Variable | Default | Description |
|----------|---------|-------------|
| `RATCHET_DLQ_PURGE_ENABLED` | `true` | Enable automatic dead letter queue purging |
| `RATCHET_DLQ_PURGE_CRON` | `0 0 2 * * ?` | Cron schedule for DLQ purge (daily at 2 AM) |
| `RATCHET_DLQ_PURGE_DAYS` | `90` | Days to retain dead-lettered jobs |
| `RATCHET_JOB_ARCHIVE_ENABLED` | `true` | Enable automatic job archiving |
| `RATCHET_JOB_ARCHIVER_CRON` | `0 0 1 * * ?` | Cron schedule for archiving (daily at 1 AM) |
| `RATCHET_JOB_RETENTION_DAYS` | `90` | Days to retain completed jobs before archiving |
| `RATCHET_JOB_ARCHIVE_BATCH_SIZE` | `1000` | Jobs processed per archiving pass |
| `RATCHET_LOG_PURGE_ENABLED` | `true` | Enable automatic job log purging |
| `RATCHET_LOG_PURGER_CRON` | `0 30 2 * * ?` | Cron schedule for log purge (daily at 2:30 AM) |
| `RATCHET_LOG_RETENTION_DAYS` | `30` | Days to retain job logs |

### Circuit Breaker Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `RATCHET_CIRCUIT_BREAKER_ENABLED` | `true` | Master switch for circuit breaker functionality |
| `RATCHET_CB_DEFAULT_FAILURE_RATE` | `50` | Failure rate threshold (%) for DEFAULT profile |
| `RATCHET_CB_DEFAULT_WAIT_SECONDS` | `30` | Open-to-half-open wait time for DEFAULT profile |
| `RATCHET_CB_DEFAULT_WINDOW_SIZE` | `100` | Sliding window size for DEFAULT profile |
| `RATCHET_CB_EXTERNAL_FAILURE_RATE` | `60` | Failure rate threshold (%) for EXTERNAL_API profile |
| `RATCHET_CB_EXTERNAL_WAIT_SECONDS` | `60` | Open-to-half-open wait time for EXTERNAL_API profile |

### Recurring Job Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `RATCHET_RECURRING_POLL_MS` | `1000` | How often to check for due recurring jobs |
| `RATCHET_RECURRING_MAX_POLL_MS` | `60000` | Maximum polling interval for recurring scheduler |
| `RATCHET_RECURRING_BATCH_LIMIT` | `20` | Maximum recurring jobs spawned per poll cycle |

### Other Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `RATCHET_MAX_PAYLOAD_KB` | `100` | Maximum serialized job payload size in KB |
| `RATCHET_PRIORITY_BOOST_INTERVAL_MINUTES` | `15` | How often low-priority jobs get a priority boost to prevent starvation |
| `RATCHET_SOFT_TIMEOUT_PERCENT` | `80` | Percentage of timeout at which a soft warning is emitted |

## Example: Production Configuration

Here's a typical configuration for a production deployment using environment variables:

```bash
# Thread pools sized for the workload
RATCHET_THREAD_POOL_SIZE_SINGLE=30
RATCHET_THREAD_POOL_SIZE_BATCH_CHILD=50

# Aggressive polling for low-latency job execution
RATCHET_POLLER_MIN_DELAY_MS=500
RATCHET_POLLER_BURST_DELAY_MS=100

# Cluster node configuration
RATCHET_NODE_HEARTBEAT_INTERVAL_SECONDS=5
RATCHET_NODE_ORPHAN_GRACE_SECONDS=30

# Data retention
RATCHET_JOB_RETENTION_DAYS=30
RATCHET_LOG_RETENTION_DAYS=14

# Payload limits
RATCHET_MAX_PAYLOAD_KB=200
```

## Example: Development Configuration

For local development, you might want faster polling and shorter retention:

```bash
RATCHET_POLLER_MIN_DELAY_MS=500
RATCHET_POLLER_MAX_DELAY_MS=2000
RATCHET_JOB_RETENTION_DAYS=7
RATCHET_LOG_RETENTION_DAYS=3
RATCHET_DLQ_PURGE_DAYS=7
```

## What's Next

You now have a complete understanding of how to install, use, and configure Ratchet. For deeper topics:

- **Batch processing** -- Build parallel batch jobs with progress tracking and streaming pipelines
- **Recurring jobs** -- Use `@Recurring` annotations and programmatic cron scheduling
- **Circuit breaker** -- Protect external service calls with `@CircuitBreakerProtected`
- **Custom store implementations** -- Use the TCK to validate your own persistence backend
