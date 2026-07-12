---
sidebar_position: 5
title: Configuration
description: CDI producer setup, beans.xml requirements, and the required RatchetOptions bean
---

# Configuration

Ratchet is designed to run in Jakarta EE without static global configuration. CDI owns the runtime objects, Ratchet consumes one immutable `RatchetOptions` bean that your application produces, and store-specific resources remain normal CDI resources.

:::important Required producer
Your application **must** produce exactly one `@ApplicationScoped RatchetOptions` bean. If no producer is found, CDI fails deployment with `UnsatisfiedResolutionException` and the scheduler never starts. This is the intended kill-switch for deployments that pull `ratchet` onto the classpath without wanting it active. There is no automatic fallback chain.
:::

## How Ratchet bootstraps

Two CDI beans drive startup:

| Bean | Role |
|---|---|
| `RatchetProducer` | Produces the internal scheduler components that combine options with injectable dependencies |
| `RatchetLifecycle` | Starts pollers, recurring scheduling, recovery timers, retention tasks, and cluster wakeup listeners |

On shutdown, components stop in reverse order and static caches are cleared to release classloader references.

## beans.xml

Ratchet's CDI beans use annotated discovery. Jakarta CDI 4.0 works without a `beans.xml` in most applications. If you define one, keep discovery mode as `annotated` or `all`:

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

If you use `@CircuitBreakerProtected`, enable its interceptor:

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

## RatchetOptions

You have two idiomatic ways to produce `RatchetOptions`. Pick one per application.

### Option A: Environment-driven producer

For container deployments, the smallest viable producer reads `RATCHET_*` environment variables (and MicroProfile Config, if present) via `RatchetOptionsFactory.fromEnvironment()`:

```java
import run.ratchet.api.RatchetOptions;
import run.ratchet.api.RatchetOptionsFactory;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

@ApplicationScoped
public class SchedulerConfiguration {

    @Produces
    @ApplicationScoped
    RatchetOptions ratchetOptions() {
        return RatchetOptionsFactory.fromEnvironment();
    }
}
```

With zero arguments, `fromEnvironment()` reads exclusively from MicroProfile Config and environment variables, applying compiled-in defaults for keys absent from those sources. See [Source Chain](#source-chain) below to overlay custom `RatchetConfigSource` implementations.

### Option B: Programmatic producer

For applications that want compile-time-checked configuration without env-var round-tripping, use the builder directly:

```java
import run.ratchet.api.RatchetOptions;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

@ApplicationScoped
public class SchedulerConfiguration {

    @Produces
    @ApplicationScoped
    RatchetOptions ratchetOptions() {
        return RatchetOptions.builder()
            .polling(p -> p.batchSize(100).minDelayMs(500).burstDelayMs(100))
            .execution(e -> e
                .maxConcurrency("SINGLE", 30)
                .maxConcurrency("BATCH_CHILD", 50)
                .defaultThreadingMode(RatchetOptions.ThreadingMode.PLATFORM))
            .node(n -> n
                .heartbeatIntervalSeconds(5)
                .orphanGraceSeconds(30))
            .maintenance(m -> m
                .jobRetentionDays(30)
                .logRetentionDays(14))
            .payload(p -> p.maxPayloadKb(200).maxResultBytes(65536))
            .build();
    }
}
```

Only produce one unqualified `RatchetOptions` bean per application. Multiple unqualified beans are treated as a deployment error because Ratchet cannot know which set of options should own the runtime.

Keep the producer method `@ApplicationScoped` so configuration sources are read once at bootstrap rather than on every injection.

## Store resources

Store resources stay container-native. Ratchet does not expect static connection configuration.

For MongoDB:

```java
@Produces
@ApplicationScoped
MongoDatabase ratchetMongoDatabase(MongoClient client) {
    return client.getDatabase("ratchet");
}
```

For SQL stores using a named persistence unit:

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

### Database timezone

For SQL stores, configure the JDBC connection so it agrees with the database server on UTC.
Ratchet stores `scheduled_time`, `next_fire`, and similar instants in zone-less `DATETIME` /
`TIMESTAMP` columns, and selects due work by comparing them against the database's server-side
clock (`NOW(3)` / `CURRENT_TIMESTAMP`). If the JDBC connection writes those columns in a different
zone than the server evaluates the comparison in, due jobs are claimed late (or early) by the
offset between the two zones.

Most providers avoid this automatically. Hibernate, for example, negotiates `connectionTimeZone=UTC`
with MySQL Connector/J. Others (notably EclipseLink) bind `java.sql.Timestamp` values in the JVM's
default zone, so a non-UTC application JVM against a UTC database silently skews claim timing. To
stay correct on any provider:

- **MySQL / MariaDB:** add `connectionTimeZone=UTC` to the JDBC URL (or set it as a datasource
  property).
- **PostgreSQL:** run the application JVM in UTC, or pin the connection's session time zone to UTC.
- **Any database:** running the application JVM in UTC (for example `-Duser.timezone=UTC`) also
  resolves the mismatch.

This requirement applies to the application's own datasource configuration; Ratchet ships only the
DDL, not a datasource.

## Option reference

### Polling

| Builder method | Default | Description |
|---|---:|---|
| `batchSize(int)` | `50` | Number of jobs claimed per poll cycle |
| `minDelayMs(long)` | `2000` | Minimum polling interval |
| `maxDelayMs(long)` | `10000` | Maximum polling interval when idle |
| `burstDelayMs(long)` | `500` | Polling interval under high job volume |
| `idleThreshold(int)` | `3` | Empty polls before entering idle mode |
| `deepIdleDelayMs(long)` | `30000` | Polling interval in deep idle mode |
| `claimHeadroomFactor(int)` | `0` | Extra claim headroom over current worker capacity |

### Execution

| Builder method | Default | Description |
|---|---:|---|
| `defaultThreadingMode(ThreadingMode)` | `PLATFORM` | Pool a job runs on when it sets no target of its own |
| `virtualExecutorJndi(String)` | _(none)_ | Adds a second managed executor as the virtual pool; absent means virtual-targeted jobs fall back to platform |
| `virtualCounterAccounting(boolean)` | `false` | Opt the virtual pool into counter-based backpressure instead of a bounded semaphore |
| `queueSize(int)` | `100` | Reserved for custom executor implementations |
| `maxConcurrency("SINGLE", int)` | `20` | One-off job concurrency |
| `maxConcurrency("RECURRING", int)` | `5` | Recurring child concurrency |
| `maxConcurrency("BATCH_CHILD", int)` | `30` | Batch child concurrency |
| `maxConcurrency("BATCH_PARENT", int)` | `2` | Batch parent coordination concurrency |
| `maxConcurrency("CHAIN_STEP", int)` | `10` | Chain step concurrency |
| `maxConcurrency("WORKFLOW_BRANCH", int)` | `10` | Workflow branch concurrency |
| `maxConcurrency("WORKFLOW_JOIN", int)` | `10` | Workflow join concurrency |
| `virtualThreadLimit(String, int)` | unset | Backpressure limit for virtual-thread execution |
| `rateLimitPerMinute(String, int)` | unset | Per-execution-type rate limit |

### Node and store

| Builder method | Default | Description |
|---|---:|---|
| `nodeId(String)` | generated | Stable logical node id |
| `heartbeatIntervalSeconds(long)` | `10` | Heartbeat interval |
| `orphanGraceSeconds(long)` | `60` | Grace window before orphan recovery |
| `orphanScanIntervalMinutes(long)` | `5` | Orphan recovery scan cadence |
| `dynamicHeartbeatEnabled(boolean)` | `true` | Adjust heartbeat cadence by load |
| `store.isolationCheckMode(FAIL)` | `FAIL` | SQL isolation validation behavior |
| `store.priorityBoostIntervalMinutes(15)` | `15` | Age-based priority boost interval; `0` disables boosting |

### Retention and payloads

| Builder method | Default | Description |
|---|---:|---|
| `dlqPurgeEnabled(boolean)` | `true` | Enable DLQ purging |
| `dlqPurgeCron(String)` | `0 0 2 * * ?` | DLQ purge schedule |
| `dlqPurgeDays(long)` | `90` | Dead-letter retention |
| `jobArchiveEnabled(boolean)` | `true` | Enable job archiving |
| `jobArchiveCron(String)` | `0 0 1 * * ?` | Archive schedule |
| `jobRetentionDays(long)` | `90` | Completed-job retention |
| `jobArchiveBatchSize(int)` | `1000` | Jobs per archive pass |
| `logPurgeEnabled(boolean)` | `true` | Enable job log purging |
| `logPurgeCron(String)` | `0 30 2 * * ?` | Log purge schedule |
| `logRetentionDays(long)` | `30` | Job log retention |
| `payload.maxPayloadKb(int)` | `100` | UTF-8 serialized job payload cap in 1024-byte units, measured before encryption/store overhead |
| `payload.maxResultBytes(long)` | `65536` | Persisted result cap; `0` disables truncation |

### Recurring, timeouts, and circuit breakers

| Builder method | Default | Description |
|---|---:|---|
| `recurring.batchLimit(int)` | `20` | Due recurring jobs spawned per cycle |
| `recurring.pollMs(long)` | `1000` | Recurring scheduler poll interval |
| `recurring.maxPollMs(long)` | `60000` | Maximum recurring scheduler backoff |
| `recurring.startupGraceSeconds(long)` | `60` | Grace period before orphaned recurring masters fire |
| `recurring.convergenceWindowSeconds(long)` | `0` | Startup cleanup convergence window |
| `timeout.softTimeoutPercent(int)` | `80` | Percent of SLA where warning fires |
| `timeout.defaultSlaSeconds(long)` | `1800` | Default job SLA timeout |
| `circuitBreaker.enabled(boolean)` | `true` | Master switch for built-in circuit breakers |
| `circuitBreaker.profile(profile, builder)` | profile defaults | Per-profile thresholds |

## Source chain

When you call `RatchetOptionsFactory.fromEnvironment(...)` from inside your producer, the factory reads from this chain in order of precedence (highest first):

1. Caller-supplied `RatchetConfigSource` instances (passed as varargs)
2. MicroProfile Config, when present
3. Environment variables (canonical `RATCHET_*` names)
4. Built-in defaults

This is not a runtime fallback. Ratchet only reads it when your producer explicitly asks it to. If you use Option B (programmatic), your producer skips the chain entirely.

To overlay a platform-specific config source on top of env + MP Config, pass it as a vararg:

```java
@ApplicationScoped
public class SchedulerConfiguration {

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

The env lookup recognizes canonical `ratchet.*` property names and `RATCHET_*` environment variable names.

## SPI overrides

Override SPIs with CDI alternatives:

```java
import static jakarta.interceptor.Interceptor.Priority.APPLICATION;

@Alternative
@Priority(APPLICATION)
@ApplicationScoped
public class AppClassPolicy implements ClassPolicy {

    @Override
    public boolean isAllowed(String className) {
        return className.startsWith("com.example.");
    }
}
```

| SPI | Default | What to override |
|---|---|---|
| `RetryPolicy` | Uses job retry options | Custom retry/no-retry decisions |
| `ResilienceStrategy` | Built-in circuit breaker | External resilience library |
| `ClassPolicy` | Empty allowlist; deployment fails fast | Application package allowlist |
| `ErrorSanitizer` | Common PII and credential redaction | Domain-specific redaction |
| `ExecutionTuningProvider` | `RatchetOptions`-backed settings | Custom concurrency logic |
| `PollingStrategyProvider` | Adaptive polling strategy | Custom poll cadence |
| `ResultPersistenceStrategy` | JSON result metadata with size cap | Custom return-value persistence |
| `CircuitBreakerConfigProvider` | `RatchetOptions`-backed profile settings | Custom built-in breaker thresholds |
| `ClusterCoordinator` | No-op | Cross-node wakeups |
| `StartupCoordinator` | Store-backed startup lease | Custom startup coordination |
| `MetricsCollector` | No-op | Micrometer or another metrics backend |
| `ExecutorProvider` | Jakarta Concurrency managed executors | Custom executor ownership |
| `RatchetEntityManagerProvider` | Store default provider | Specific SQL persistence unit |
| `NodeIdentityProvider` | Hostname-based with heartbeat | Cloud-specific node identity |

## ClassPolicy

The default `PackagePrefixClassPolicy` has an empty allowlist. Ratchet refuses to start until you provide a real allowlist, because jobs execute application code by design.

For demos and tests only, you can opt out through options:

```java
RatchetOptions.builder()
    .security(s -> s.allowEmptyClassPolicy(true))
    .build();
```

In that mode the default policy still rejects every target class. Install a real `ClassPolicy` before running jobs.

## What's next

- **Batch processing** -- Build parallel batch jobs with progress tracking and streaming pipelines
- **Recurring jobs** -- Use `@Recurring` annotations and programmatic cron scheduling
- **Circuit breaker** -- Protect external service calls with `@CircuitBreakerProtected`
- **Custom stores** -- Use the TCK to validate your own persistence backend
