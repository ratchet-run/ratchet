---
title: Configuration
---

# Configuration

Tuning Ratchet for your deployment.

Ratchet's Jakarta EE configuration model is CDI-first:

- Produce exactly one `@ApplicationScoped RatchetOptions` bean (**required**). If absent, CDI fails deployment with `UnsatisfiedResolutionException` and the scheduler never starts.
- Produce store resources, such as `EntityManager`, `MongoDatabase`, and managed executors, as normal CDI resources.
- Replace behavioral extension points with CDI `@Alternative` beans.

The producer may either build options programmatically (see [RatchetOptions Producer](#ratchetoptions-producer) below) or read env vars + MicroProfile Config via `RatchetOptionsFactory.fromEnvironment()` (see [Source Chain](#source-chain) below). See the canonical [Configuration](/getting-started/configuration) page for a deeper walkthrough.

## RatchetOptions Producer

```java
import run.ratchet.api.RatchetOptions;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

@ApplicationScoped
public class AppSchedulerConfig {

    @Produces
    @ApplicationScoped
    RatchetOptions ratchetOptions() {
        return RatchetOptions.builder()
            .polling(p -> p.batchSize(100).minDelayMs(500).burstDelayMs(100))
            .execution(e -> e.maxConcurrency("SINGLE", 32).maxConcurrency("BATCH_CHILD", 64))
            .node(n -> n.heartbeatIntervalSeconds(10).orphanGraceSeconds(90))
            .maintenance(m -> m.jobRetentionDays(30).logRetentionDays(14))
            .build();
    }
}
```

This object is immutable and container-scoped. It avoids static runtime configuration, survives redeploys, and lets multiple applications in the same server use different settings. See [Configuration](/getting-started/configuration) for the env-driven alternative.

## Store Resources

Keep store wiring Jakarta EE native. Configure the resources themselves with CDI instead of encoding connection details in scheduler properties.

```java
@Produces
@ApplicationScoped
MongoDatabase ratchetMongoDatabase(MongoClient client) {
    return client.getDatabase("ratchet");
}
```

SQL stores still use `RatchetEntityManagerProvider` when you need to bind Ratchet to a specific persistence unit.

```java
@ApplicationScoped
public class OrdersRatchetEntityManagerProvider implements RatchetEntityManagerProvider {

    @PersistenceContext(unitName = "orders-pu")
    EntityManager entityManager;

    @Override
    public EntityManager getEntityManager() {
        return entityManager;
    }
}
```

## Common Options

| Builder path | Default | Purpose |
|---|---:|---|
| `polling.batchSize(50)` | `50` | Jobs claimed per poll cycle |
| `polling.minDelayMs(2000)` | `2000` | Minimum poll interval |
| `polling.maxDelayMs(10000)` | `10000` | Maximum adaptive poll interval |
| `execution.maxConcurrency("SINGLE", 20)` | `20` | Worker concurrency for one-off jobs |
| `execution.maxConcurrency("BATCH_CHILD", 30)` | `30` | Worker concurrency for batch children |
| `execution.defaultThreadingMode(PLATFORM)` | `PLATFORM` | Pool a job runs on when it sets no target of its own |
| `execution.virtualExecutorJndi(name)` | _(none)_ | Adds a second managed executor as the virtual pool; absent means virtual-targeted jobs fall back to platform |
| `execution.virtualCounterAccounting(false)` | `false` | Opt the virtual pool into counter-based backpressure instead of a bounded semaphore |
| `node.heartbeatIntervalSeconds(10)` | `10` | Node heartbeat interval |
| `node.orphanGraceSeconds(60)` | `60` | Grace period before reclaiming orphaned work |
| `maintenance.jobRetentionDays(90)` | `90` | Completed-job retention before archiving |
| `maintenance.logRetentionDays(30)` | `30` | Per-job log retention |
| `payload.maxPayloadKb(100)` | `100` | UTF-8 serialized job payload cap in 1024-byte units, before encryption/store overhead |
| `payload.maxResultBytes(65536)` | `65536` | Persisted result JSON cap; `0` disables truncation |
| `store.priorityBoostIntervalMinutes(15)` | `15` | Starvation-prevention priority boost interval |

## Per-job execution target

A job picks its pool with `.virtual()` or `.platform()` on the builder; calling neither inherits `default-threading-mode`. The label selects a configured executor, not a guaranteed thread type. Set `ratchet.worker.virtual-executor-jndi` to a second managed executor to enable the virtual pool. When it is unset, a `.virtual()` job runs on platform instead, recorded with a one-time log warning and the `ratchet.execution.target.fallback` metric. The metric increments for each affected job occurrence; the warning is emitted once per distinct requested target.

Whether the virtual pool's threads are actually virtual is the container's decision. Pointing `virtual-executor-jndi` at an `@ManagedExecutorDefinition(virtual = true)` is a request the runtime may ignore: GlassFish 8 honors it, while WildFly 40 binds the executor but still runs jobs on platform threads. Jakarta exposes no API to check this at runtime, so Ratchet can neither warn about it nor guarantee it. Confirm against your container's documentation.

`ratchet.worker.use-virtual-threads` was removed. It only switched backpressure accounting and never selected an executor; a stale value is ignored with a startup warning. Use `default-threading-mode` and `virtual-executor-jndi` instead.

## Source Chain

For container deployments, write a producer that reads `RATCHET_*` environment variables and MicroProfile Config via `RatchetOptionsFactory.fromEnvironment()`:

```java
@ApplicationScoped
public class AppSchedulerConfig {

    @Produces
    @ApplicationScoped
    RatchetOptions ratchetOptions() {
        return RatchetOptionsFactory.fromEnvironment();
    }
}
```

Pass a `RatchetConfigSource` to overlay a platform-specific source ahead of the ambient chain:

```java
@ApplicationScoped
public class AppSchedulerConfig {

    @Inject PlatformRatchetConfigSource platformSource;

    @Produces
    @ApplicationScoped
    RatchetOptions ratchetOptions() {
        return RatchetOptionsFactory.fromEnvironment(platformSource);
    }
}

@ApplicationScoped
public class PlatformRatchetConfigSource implements RatchetConfigSource {

    @Override
    public Optional<String> get(String propertyName, String environmentVariable) {
        return platformConfig.lookup(propertyName)
            .or(() -> platformConfig.lookup(environmentVariable));
    }
}
```

The env lookup recognizes canonical `RATCHET_*` environment variable names and `ratchet.*` property names, such as `ratchet.poller.batch-size`.

## SPI Overrides

Security and behavior extension points are CDI beans, not class names in properties.

| SPI | Default | What to override |
|---|---|---|
| `ClassPolicy` | Empty package allowlist; startup fails fast | Allowed application packages |
| `ErrorSanitizer` | Common PII and credential redaction | Domain-specific redaction |
| `MetricsCollector` | No-op | Micrometer or another metrics backend |
| `ExecutorProvider` | Jakarta Concurrency managed executors | Custom managed executors or standalone tests |
| `ResilienceStrategy` | Built-in circuit breaker | External resilience library |

## See also

- [Getting Started configuration](/getting-started/configuration)
- [Installation](/deployment/installation)
- [Troubleshooting](/troubleshooting/common-issues)
