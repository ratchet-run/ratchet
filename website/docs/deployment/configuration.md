---
title: Configuration
---

# Configuration

Tuning Ratchet for your deployment.

Ratchet's Jakarta EE configuration model is CDI-first:

- Produce a single `@ApplicationScoped RatchetOptions` bean for scheduler tuning.
- Produce store resources, such as `EntityManager`, `MongoDatabase`, and managed executors, as normal CDI resources.
- Replace behavioral extension points with CDI `@Alternative` beans.

If no `RatchetOptions` bean exists, the RI builds one from the fallback source chain: CDI-provided `RatchetConfigSource` beans, optional MicroProfile Config when present, environment variables, system properties, then built-in defaults.

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

This object is immutable and container-scoped. It avoids static runtime configuration, survives redeploys cleanly, and lets multiple applications in the same server use different settings.

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
| `execution.useVirtualThreads(false)` | `false` | Switch to Java virtual-thread execution |
| `node.heartbeatIntervalSeconds(10)` | `10` | Node heartbeat interval |
| `node.orphanGraceSeconds(60)` | `60` | Grace period before reclaiming orphaned work |
| `maintenance.jobRetentionDays(90)` | `90` | Completed-job retention before archiving |
| `maintenance.logRetentionDays(30)` | `30` | Per-job log retention |
| `payload.maxPayloadKb(100)` | `100` | Serialized job payload size cap |
| `payload.maxResultBytes(65536)` | `65536` | Persisted result JSON cap; `0` disables truncation |
| `store.priorityBoostIntervalMinutes(15)` | `15` | Starvation-prevention priority boost interval |

## Source Chain Fallback

`RatchetOptions` is the preferred API. The source chain exists for platforms that already centralize configuration elsewhere.

To plug in a custom source, produce a CDI bean:

```java
@ApplicationScoped
public class PlatformRatchetConfigSource implements RatchetConfigSource {

    @Override
    public Optional<String> get(String propertyName, String environmentVariable) {
        return platformConfig.lookup(propertyName)
            .or(() -> platformConfig.lookup(environmentVariable));
    }
}
```

The built-in env/sysprop fallback understands the `RATCHET_*` names used by older deployments and the typed property names, such as `ratchet.poller.batch-size`.

## SPI Overrides

Security and behavior extension points are CDI beans, not class names in properties.

| SPI | Default | What to override |
|---|---|---|
| `ClassPolicy` | Empty package allowlist; startup fails fast | Allowed application packages |
| `ErrorSanitizer` | Common PII and credential redaction | Domain-specific redaction |
| `MetricsCollector` | No-op | Micrometer or another metrics backend |
| `ExecutorProvider` | Jakarta Concurrency managed executors | Custom managed executors or standalone tests |
| `ResilienceStrategy` | Built-in circuit breaker | External resilience library |

## See Also

- [Getting Started configuration](/docs/getting-started/configuration)
- [Installation](/docs/deployment/installation)
- [Troubleshooting](/docs/troubleshooting/common-issues)
