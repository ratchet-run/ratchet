---
sidebar_position: 6
title: SPI Implementation Guide
description: Complete guide to implementing Ratchet SPI interfaces for custom extensions
---

# SPI Implementation Guide

Ratchet is designed around a set of Service Provider Interfaces (SPIs) that decouple the core engine from specific implementations. Major extension points -- configuration, invocation resolution, result persistence, resilience, metrics, logging, storage, security, and cluster coordination -- are expressed as SPI interfaces that you can replace with your own implementation.

This guide covers the CDI wiring pattern, the complete SPI inventory, and Ratchet conformance tiers.
The TCK is split into four submodules: `ratchet-tck-store` (store SPI), `ratchet-tck-api`
(public-API, container-free), `ratchet-tck-jakarta` (Jakarta-EE conformance via Arquillian), and
`ratchet-tck-util` (shared JUnit helpers). Each earns a distinct compatibility label; see the
[README's tiered-conformance section](https://github.com/ratchet-run/ratchet#custom-store-implementation)
for the full matrix.

## The CDI @Alternative Pattern

All SPI interfaces in Ratchet have default implementations provided by the reference implementation (RI). To replace a default with your own implementation, use CDI's `@Alternative` mechanism with `@Priority`:

```java
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.interceptor.Interceptor;

@Alternative
@Priority(Interceptor.Priority.APPLICATION)
@ApplicationScoped
public class MyCustomSpi implements SomeRatchetSpi {
    // Your implementation
}
```

### How It Works

1. The RI provides a default bean for each SPI (annotated `@ApplicationScoped` or produced via `@Produces` in `RatchetProducer`).
2. Your `@Alternative` bean is discovered by CDI during deployment.
3. The `@Priority(Interceptor.Priority.APPLICATION)` (value 2000) ensures your bean takes precedence over the RI default.
4. CDI injects your implementation everywhere the SPI type is used.

No XML, no configuration files, no service loader entries. Just annotate your class and put it on the classpath.

### Priority Ordering

If multiple alternatives exist for the same SPI, the one with the highest `@Priority` value wins:

| Priority Constant | Value | Typical Use |
|-------------------|-------|-------------|
| `Interceptor.Priority.LIBRARY_BEFORE` | 0 | Library defaults |
| `Interceptor.Priority.APPLICATION` | 2000 | Application overrides |
| `Interceptor.Priority.APPLICATION + 100` | 2100 | Override another alternative |

### Verifying Your Override

After deployment, verify your bean is active by injecting the SPI and checking the concrete type:

```java
@Inject
JobInvocationResolver resolver;

// In a startup observer or health check:
log.info("Active JobInvocationResolver: " + resolver.getClass().getName());
// Should print your class, not DefaultJobInvocationResolver
```

## Complete SPI Reference

Ratchet defines SPI interfaces across the API, RI, and store modules. Each entry below shows the interface, its default implementation, and a skeleton for a custom override.

### 1. JobInvocationResolver

**Module:** `ratchet-api`
**Package:** `run.ratchet.spi`
**Default:** ASM-based callback analysis

Resolves submitted callbacks into persisted job invocations.

```java
public interface JobInvocationResolver {
    JobInvocation resolve(Serializable callback);
    JobInvocation resolve(Serializable callback, List<Object> runtimeArguments);
}
```

**Override:**

```java
@Alternative
@Priority(Interceptor.Priority.APPLICATION)
@ApplicationScoped
public class AppInvocationResolver implements JobInvocationResolver {

    @Override
    public JobInvocation resolve(Serializable callback) {
        return resolve(callback, List.of());
    }

    @Override
    public JobInvocation resolve(Serializable callback, List<Object> runtimeArguments) {
        return new JobInvocation("com.example.JobTargets", "run", "()V", false, runtimeArguments);
    }
}
```

See [Payload and Result Customization](./custom-serialization.md) for detailed guidance.

---

### 2. RetryPolicy

**Module:** `ratchet-api`
**Package:** `run.ratchet.spi`
**Default:** `DefaultRetryPolicy` (passthrough -- defers to job-level `maxRetries` and `backoffPolicy`)

Controls global retry behavior for failed jobs.

```java
public interface RetryPolicy {
    boolean shouldRetry(int attempt, Throwable cause);
    Duration getDelay(int attempt);
}
```

**Override:**

```java
@Alternative
@Priority(Interceptor.Priority.APPLICATION)
@ApplicationScoped
public class SmartRetryPolicy implements RetryPolicy {

    @Override
    public boolean shouldRetry(int attempt, Throwable cause) {
        return attempt <= 5 && isTransient(cause);
    }

    @Override
    public Duration getDelay(int attempt) {
        return Duration.ofSeconds(2L * (1L << Math.min(attempt - 1, 8)));
    }

    private boolean isTransient(Throwable t) {
        return t instanceof IOException
            || t instanceof TimeoutException;
    }
}
```

See [Custom Retry Policies](./custom-retry-policies.md) for detailed guidance.

---

### 3. ResilienceStrategy

**Module:** `ratchet-api`
**Package:** `run.ratchet.spi`
**Default:** `DefaultResilienceStrategy` (built-in circuit breaker via `CircuitBreakerRegistry`)
**Annotation:** `@Incubating`

Wraps job execution with resilience patterns (circuit breakers, bulkheads).

```java
@Incubating
public interface ResilienceStrategy {
    <T> T execute(String serviceName, Callable<T> task) throws Exception;
    boolean isServiceAvailable(String serviceName);
    default Duration getRetryDelay(String serviceName) {
        return Duration.ofSeconds(30);
    }
}
```

**Override (Resilience4j):**

```java
@Alternative
@Priority(Interceptor.Priority.APPLICATION)
@ApplicationScoped
public class Resilience4jStrategy implements ResilienceStrategy {

    @Inject
    private io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry r4jRegistry;

    @Override
    public <T> T execute(String serviceName, Callable<T> task) throws Exception {
        return r4jRegistry.circuitBreaker(serviceName).executeCallable(task);
    }

    @Override
    public boolean isServiceAvailable(String serviceName) {
        var state = r4jRegistry.circuitBreaker(serviceName).getState();
        return state != io.github.resilience4j.circuitbreaker.CircuitBreaker.State.OPEN;
    }
}
```

See [Circuit Breakers](./circuit-breakers.md) for detailed guidance.

---

### 4. MetricsCollector

**Module:** `ratchet-api`
**Package:** `run.ratchet.spi`
**Default:** `NoOpMetricsCollector` (empty methods)
**Adapter module:** `ratchet-micrometer` provides `MicrometerMetricsCollector`
**Annotation:** `@Incubating`

Receives job lifecycle callbacks for monitoring.

```java
@Incubating
public interface MetricsCollector {
    void jobStarted(UUID jobId, JobType type, JobPriority priority);
    void jobCompleted(UUID jobId, JobType type, long executionTimeMs);
    void jobFailed(UUID jobId, JobType type, Throwable cause, int attempt);
}
```

**Override:**

```java
@Alternative
@Priority(Interceptor.Priority.APPLICATION)
@ApplicationScoped
public class DatadogMetricsCollector implements MetricsCollector {

    @Inject
    private StatsDClient statsd;

    @Override
    public void jobStarted(UUID jobId, JobType type, JobPriority priority) {
        statsd.incrementCounter("ratchet.jobs.started",
            "type:" + type, "priority:" + priority);
    }

    @Override
    public void jobCompleted(UUID jobId, JobType type, long executionTimeMs) {
        statsd.incrementCounter("ratchet.jobs.completed", "type:" + type);
        statsd.recordExecutionTime("ratchet.jobs.duration", executionTimeMs,
            "type:" + type);
    }

    @Override
    public void jobFailed(UUID jobId, JobType type, Throwable cause, int attempt) {
        statsd.incrementCounter("ratchet.jobs.failed",
            "type:" + type, "exception:" + cause.getClass().getSimpleName());
    }
}
```

See [Metrics Collection](./metrics-collection.md) for detailed guidance.

---

### 5. JobLogger

**Module:** `ratchet-api`
**Package:** `run.ratchet.spi`
**Default:** Created by `DefaultJobLoggerFactory` as a per-execution `JBossLoggingJobLogger`, which bridges to JBoss Logging and publishes `JobLogLine` events through the internal event publisher.
**Annotation:** `@Incubating`

Per-job isolated logging.

```java
@Incubating
public interface JobLogger {
    void info(String message);
    void debug(String message);
    void warn(String message);
    void error(String message);
    void trace(String message);
}
```

See [Custom Logging](./custom-logging.md) for detailed guidance.

---

### 6. ClassPolicy

**Module:** `ratchet-api`
**Package:** `run.ratchet.spi`
**Default:** `PackagePrefixClassPolicy` (empty allowlist by default -- must be configured)
**Annotation:** `@Incubating`

Controls which classes can be loaded and executed as job targets. It gates which classes the engine will deserialize and run.

```java
@Incubating
public interface ClassPolicy {
    boolean isAllowed(String className);
}
```

**Override:**

```java
@Alternative
@Priority(Interceptor.Priority.APPLICATION)
@ApplicationScoped
public class AppClassPolicy implements ClassPolicy {

    private static final Set<String> ALLOWED_PREFIXES = Set.of(
        "com.mycompany.app.",
        "com.mycompany.shared."
    );

    @Override
    public boolean isAllowed(String className) {
        if (className == null || className.isEmpty()) {
            return false;
        }
        return ALLOWED_PREFIXES.stream()
            .anyMatch(className::startsWith);
    }
}
```

---

### 7. BeanResolver

**Module:** `ratchet-api`
**Package:** `run.ratchet.spi`
**Default:** `CdiBeanResolver` (resolves beans via CDI `Instance<Object>`)
**Annotation:** `@Incubating`

Resolves bean instances by type, abstracting the DI mechanism.

```java
@Incubating
@FunctionalInterface
public interface BeanResolver {
    <T> T resolve(Class<T> type);
}
```

**Override (Spring context):**

```java
@Alternative
@Priority(Interceptor.Priority.APPLICATION)
@ApplicationScoped
public class SpringBeanResolver implements BeanResolver {

    private final ApplicationContext springContext;

    @Inject
    public SpringBeanResolver(ApplicationContext springContext) {
        this.springContext = springContext;
    }

    @Override
    public <T> T resolve(Class<T> type) {
        return springContext.getBean(type);
    }
}
```

---

### 8. ExecutorProvider

**Module:** `ratchet-api`
**Package:** `run.ratchet.spi`
**Default:** `DefaultExecutorProvider` (Jakarta Concurrency managed executors via JNDI)
**Annotation:** `@Incubating`

Provides thread pools for job execution and scheduling.

```java
@Incubating
public interface ExecutorProvider {
    ExecutorService getJobExecutor();
    ScheduledExecutorService getScheduledExecutor();
}
```

**Override (custom virtual thread pool):**

```java
@Alternative
@Priority(Interceptor.Priority.APPLICATION)
@ApplicationScoped
public class VirtualThreadExecutorProvider implements ExecutorProvider {

    private final ExecutorService jobExecutor =
        Executors.newVirtualThreadPerTaskExecutor();

    private final ScheduledExecutorService scheduler =
        Executors.newScheduledThreadPool(2, Thread.ofVirtual().factory());

    @Override
    public ExecutorService getJobExecutor() {
        return jobExecutor;
    }

    @Override
    public ScheduledExecutorService getScheduledExecutor() {
        return scheduler;
    }

    @PreDestroy
    void shutdown() {
        jobExecutor.shutdown();
        scheduler.shutdown();
    }
}
```

---

### 9. NodeIdentityProvider

**Module:** `ratchet-api`
**Package:** `run.ratchet.spi`
**Default:** `DefaultNodeIdentityProvider` (generates a UUID, manages heartbeats)
**Annotation:** `@Incubating`

Provides the unique node identifier for multi-node deployments.

```java
@Incubating
public interface NodeIdentityProvider {
    String getNodeId();
}
```

**Override (hostname-based):**

```java
@Alternative
@Priority(Interceptor.Priority.APPLICATION)
@ApplicationScoped
public class HostnameNodeIdentityProvider implements NodeIdentityProvider {

    private final String nodeId;

    public HostnameNodeIdentityProvider() {
        try {
            String hostname = InetAddress.getLocalHost().getHostName();
            String pid = ProcessHandle.current().pid() + "";
            this.nodeId = hostname + "-" + pid;
        } catch (Exception e) {
            this.nodeId = UUID.randomUUID().toString();
        }
    }

    @Override
    public String getNodeId() {
        return nodeId;
    }
}
```

---

### 10. ClusterCoordinator

**Module:** `ratchet-api`
**Package:** `run.ratchet.spi`
**Default:** `NoOpClusterCoordinator` (single-node no-op)
**Annotation:** `@Incubating`

Coordinates job scheduling across cluster nodes by broadcasting wakeup signals.

```java
@Incubating
public interface ClusterCoordinator extends AutoCloseable {
    void notifyNewWork(JobPriority priority, NodeIdentity source, String executionTarget);
    void registerWakeupListener(Consumer<JobWakeupHint> listener);
    void close();
}
```

**Override (Redis pub/sub):**

```java
@Alternative
@Priority(Interceptor.Priority.APPLICATION)
@ApplicationScoped
public class RedisClusterCoordinator implements ClusterCoordinator {

    private static final String CHANNEL = "ratchet:wakeup";

    @Inject
    private RedisClient redis;

    private final List<Consumer<JobWakeupHint>> listeners = new CopyOnWriteArrayList<>();

    @PostConstruct
    void subscribe() {
        redis.subscribe(CHANNEL, hint -> {
            for (Consumer<JobWakeupHint> listener : listeners) {
                listener.accept(hint);
            }
        });
    }

    @Override
    public void notifyNewWork(JobPriority priority, NodeIdentity source, String executionTarget) {
        redis.publish(CHANNEL, priority.name());
    }

    @Override
    public void registerWakeupListener(Consumer<JobWakeupHint> listener) {
        listeners.add(listener);
    }

    @Override
    public void close() {
        redis.close();
    }
}
```

### 11. StartupCoordinator

**Module:** `ratchet-api`
**Package:** `run.ratchet.spi`
**Default:** `StoreBackedStartupCoordinator` (store-backed lease)
**Annotation:** `@Incubating`

Coordinates destructive startup work using a lease rather than an external leader-election system.

```java
@Incubating
public interface StartupCoordinator {
    boolean tryAcquire(String actionName, Duration leaseTtl);
    void release(String actionName);
}
```

---

### 12. JobLoggerFactory

**Module:** `ratchet-api`
**Package:** `run.ratchet.spi`
**Default:** `DefaultJobLoggerFactory`
**Annotation:** `@Incubating`

Creates the job-scoped logger bound into `JobContext`.

```java
@Incubating
public interface JobLoggerFactory {
    JobLogger create(JobLoggerContext context);
}
```

---

### 13. ErrorSanitizer

**Module:** `ratchet-api`
**Package:** `run.ratchet.spi`
**Default:** `DefaultErrorSanitizer` (strips JDBC URLs, credentials, emails, truncates to 2000 chars)
**Annotation:** `@Incubating`

Sanitizes exception messages before they are persisted to the job store or published in events.

```java
@Incubating
public interface ErrorSanitizer {
    String sanitize(Throwable ex);
}
```

**Override (custom PII patterns):**

```java
@Alternative
@Priority(Interceptor.Priority.APPLICATION)
@ApplicationScoped
public class StrictErrorSanitizer implements ErrorSanitizer {

    private static final Pattern SSN =
        Pattern.compile("\\b\\d{3}-\\d{2}-\\d{4}\\b");
    private static final Pattern CREDIT_CARD =
        Pattern.compile("\\b\\d{4}[- ]?\\d{4}[- ]?\\d{4}[- ]?\\d{4}\\b");
    private static final int MAX_LENGTH = 300;

    @Override
    public String sanitize(Throwable ex) {
        if (ex == null) return "null";

        String className = ex.getClass().getName();
        String message = ex.getMessage();
        if (message == null) return className;

        String sanitized = message;
        sanitized = SSN.matcher(sanitized).replaceAll("***SSN***");
        sanitized = CREDIT_CARD.matcher(sanitized).replaceAll("***CC***");

        String result = className + ": " + sanitized;
        if (result.length() > MAX_LENGTH) {
            result = result.substring(0, MAX_LENGTH - 3) + "...";
        }
        return result;
    }
}
```

The default implementation already handles JDBC URLs with embedded credentials, URLs with userinfo, email patterns, and common credential key-value patterns (`password=...`, `token=...`, etc.).

---

### 14. LambdaDescriptor

**Module:** `ratchet-api`
**Package:** `run.ratchet.spi`

This is a record (not a replaceable SPI) that describes the result of lambda analysis. It is included here for completeness:

```java
@Incubating
public record LambdaDescriptor(
    String targetClass,      // Fully qualified class name
    String methodName,       // Method name
    String methodDescriptor, // JVM method descriptor
    boolean isStatic,        // Whether the method is static
    Object[] capturedArgs    // Arguments captured from the lambda closure
) { }
```

## Store SPI: Custom Persistence

The store layer is the largest SPI surface in Ratchet. The mandatory `JobStore` interface composes only the persistence concerns every conforming store must provide. Capabilities a store may legitimately lack are advertised separately and discovered through a runtime probe, so a minimal backend implements the core and nothing more:

```java
public interface JobStore
    extends JobCrudStore,        // Basic CRUD for job entities
            JobClaimStore,       // Atomic job claiming for execution
            JobTerminalStore,    // Terminal success/failure/cancel transitions
            JobRetryStore,       // Retry scheduling
            JobPauseStore,       // Pause/resume transitions
            JobBatchStatusStore, // Non-terminal status and batch/orphan operations
            JobBulkStore,        // Bulk operations (orphan recovery, cleanup)
            NodeStore,           // Node registration, heartbeat, crash recovery
            TagStore             // Job tag writes
{
    // Probe for an optional capability this store may also implement. The default reflects Java
    // type membership: a store advertises a capability simply by implementing its interface.
    default <T> Optional<T> capability(Class<T> type) {
        return type.isInstance(this) ? Optional.of(type.cast(this)) : Optional.empty();
    }
}
```

A store opts into an optional capability by additionally implementing its interface. Callers never assume a capability is present; they probe for it, and the engine disables the dependent feature when it is absent:

```java
jobStore.capability(SignalStore.class)
    .ifPresent(signals -> signals.deliverSignalByKey(key, payload));
```

Ratchet ships MySQL, PostgreSQL, Oracle, SQL Server, and MongoDB implementations, all of which advertise every capability. To implement a custom store (for example DynamoDB, Redis, or an in-memory test backend), implement the core `JobStore`, plus any optional capabilities your backend can support, and validate it against the TCK.

### Core Store Interfaces (mandatory)

| Interface | Responsibility | Key Methods |
|-----------|---------------|-------------|
| `JobCrudStore` | Create, read, update, delete jobs | `save()`, `findById()`, `delete()` |
| `JobClaimStore` | Atomic job claiming for execution | `claimNextBatch()`, `claimNextBatchOptimized()` |
| `JobTerminalStore` | Terminal success, failure, and cancellation transitions | `markJobSucceeded()`, `markJobFailedTerminal()`, `cancelJob()` |
| `JobRetryStore` | Retry scheduling and attempt-state updates | `scheduleJobRetry()`, `incrementRetryAttempt()` |
| `JobPauseStore` | Pause and resume transitions | `transitionToPaused()`, `transitionFromPausedAtomic()` |
| `JobBatchStatusStore` | Non-terminal status, pickup, and orphan operations | `updateJobStatus()`, `compareAndSwapStatus()`, `resetRunningJobs()` |
| `JobBulkStore` | Bulk operations | `bulkInsert()`, `resetOrphanJobs()`, `deleteDlqOlderThan()` |
| `NodeStore` | Node registration, heartbeat, and crash recovery | `upsertHeartbeat()`, `findInactiveNodesSince()` |
| `TagStore` | Job tag writes | `insertTags()`, `deleteTagsByJobId()` |

### Optional Capabilities

A store advertises each of these by implementing the interface; the engine probes with `capability()` and disables the feature when the capability is absent.

| Capability | Responsibility | Key Methods |
|-----------|---------------|-------------|
| `RecurringJobStore` | Recurring-master persistence | `claimDueRecurring()`, `advanceNextFire()`, `cancelRecurringAndArchive()` |
| `BatchStore` | Batch progress tracking and metrics | `saveBatch()`, `incrementCompletedAtomic()`, `markBatchCompleteIfReady()` |
| `WorkflowConditionStore` | Workflow branch conditions | `saveCondition()`, `findConditionsByParentJobId()` |
| `SignalStore` | Signal-waiting jobs | `deliverSignalById()`, `deliverSignalByKey()`, `findTimedOutSignalJobs()` |
| `ResourcePermitStore` | Resource permits | `tryAcquirePermit()`, `releasePermit()` |
| `LockStore` | Distributed locks | `tryLock()`, `unlock()`, `renewLock()` |
| `ArchiveStore` | Job archiving | `archiveJob()`, `findArchivedJobs()` |
| `JobQueryStore` | Read-only admin/query projections and tag lookups | `searchJobs()`, `countJobs()`, `findJobIdsByTag()` |
| `JobAnalyticsStore` | Aggregate counts, rates, and percentiles | `countJobsByStatus()`, `getQueueWaitTimePercentile()` |
| `JobAuditStore` | Execution history and per-job logs | `saveExecution()`, `findExecutionsByJobId()`, `appendLog()` |
| `DlqAlertStore` | DLQ alerting | `saveDlqAlert()`, `existsRecentDlqAlert()` |
| `JobExtensionStore` | Indexed job properties and per-namespace extension state | `putProperty()`, `getPropertiesByPrefix()`, `initState()`, `updateState()` |

### Implementing a Custom Store

```java
import run.ratchet.store.spi.JobStore;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.BatchEntity;
import run.ratchet.store.id.UuidV7Factory;
// ... other entity imports

import java.util.UUID;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.interceptor.Interceptor;

@Alternative
@Priority(Interceptor.Priority.APPLICATION)
@ApplicationScoped
public class CustomDocumentJobStore implements JobStore {

    @Inject
    private MongoDatabase database;

    // --- JobCrudStore ---

    @Override
    public JobEntity save(JobEntity job) {
        MongoCollection<Document> collection = database.getCollection("ratchet_jobs");
        if (job.getId() == null) {
            job.setId(UuidV7Factory.create());
            Document doc = toDocument(job);
            collection.insertOne(doc);
        } else {
            Document doc = toDocument(job);
            collection.replaceOne(eq("_id", job.getId()), doc);
        }
        return job;
    }

    @Override
    public Optional<JobEntity> findById(UUID id) {
        Document doc = database.getCollection("ratchet_jobs")
            .find(eq("_id", id))
            .first();
        return Optional.ofNullable(doc).map(this::toJobEntity);
    }

    // --- JobClaimStore ---

    @Override
    public List<JobEntity> claimNextBatch(int limit, String nodeId) {
        // Use MongoDB findOneAndUpdate with atomic status transition
        // PENDING → RUNNING, set ownedBy = nodeId
        List<JobEntity> claimed = new ArrayList<>();
        for (int i = 0; i < limit; i++) {
            Document doc = database.getCollection("ratchet_jobs")
                .findOneAndUpdate(
                    and(eq("status", "PENDING"),
                        lte("scheduledAt", Instant.now())),
                    combine(
                        set("status", "RUNNING"),
                        set("ownedBy", nodeId),
                        set("startedAt", Instant.now())),
                    new FindOneAndUpdateOptions()
                        .sort(ascending("priority", "scheduledAt"))
                        .returnDocument(ReturnDocument.AFTER));
            if (doc == null) break;
            claimed.add(toJobEntity(doc));
        }
        return claimed;
    }

    // ... implement the remaining core JobStore methods. Add an optional capability only when your
    // backend supports it — e.g. `implements JobStore, SignalStore` — and the engine will probe
    // for it through capability(); leave it off and the dependent feature stays disabled.
}
```

### Validating with the TCK

The published store SPI Technology Compatibility Kit (TCK) provides abstract test contracts for the core surface and for each optional capability. A capability contract is conditional: it runs against a store that advertises the capability and is reported `N/A` against one that does not, so a core-only store stays conformant. Each contract base implements `JobStoreContractFixture`, so to validate your custom store you extend the contract and supply the fixture methods (`store()`, `newPendingJob()`, `newBatchParentJob()`, `cleanupStore()`):

```java
import run.ratchet.tck.store.JobStoreContractFixture;
import run.ratchet.tck.store.AbstractJobCrudStoreContract;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.JobStore;

// 1. Implement the fixture
public class MongoStoreFixture implements JobStoreContractFixture {

    private final CustomDocumentJobStore store;

    public MongoStoreFixture(MongoDatabase database) {
        this.store = new CustomDocumentJobStore(database);
    }

    @Override
    public JobStore store() {
        return store;
    }

    @Override
    public JobEntity newPendingJob() {
        JobEntity job = new JobEntity();
        job.setTags(List.of("test-" + UUID.randomUUID()));
        job.setStatus(JobStatus.PENDING);
        // ... set required fields
        return job;
    }

    @Override
    public JobEntity newBatchParentJob() {
        JobEntity job = newPendingJob();
        job.setJobType(JobExecutionType.BATCH_PARENT);
        return job;
    }

    @Override
    public void cleanupStore() {
        // Drop test collections or delete test data
    }
}

// 2. Extend TCK contracts. AbstractJobCrudStoreContract implements
// JobStoreContractFixture, so the test class supplies the fixture methods directly,
// delegating to the helper above.
class MongoJobCrudStoreTest extends AbstractJobCrudStoreContract {

    private final MongoStoreFixture fixture = new MongoStoreFixture(testDatabase);

    @AfterEach
    void cleanup() {
        fixture.cleanupStore();
    }

    @Override
    public JobStore store() {
        return fixture.store();
    }

    @Override
    public JobEntity newPendingJob() {
        return fixture.newPendingJob();
    }

    @Override
    public JobEntity newBatchParentJob() {
        return fixture.newBatchParentJob();
    }

    @Override
    public void cleanupStore() {
        fixture.cleanupStore();
    }
}
```

The TCK includes abstract contracts for each store sub-interface:

| TCK Contract | Tests |
|-------------|-------|
| `AbstractJobCrudStoreContract` | save, find, update, delete operations |
| `AbstractJobClaimStoreContract` | Atomic claiming, concurrent claim safety |
| `AbstractJobTerminalStoreContract` | Terminal success, failure, and cancellation transitions |
| `AbstractJobRetryStoreContract` | Retry scheduling |
| `AbstractJobPauseStoreContract` | Pause and resume transitions |
| `AbstractJobBatchStatusStoreContract` | Non-terminal status and batch/orphan operations |
| `AbstractJobBulkStoreContract` | Bulk recovery, stale job detection |
| `AbstractBatchStoreContract` | Batch progress tracking |
| `AbstractLockStoreContract` | Lock acquire, release, expiry |
| `AbstractNodeStoreContract` | Node registration, heartbeat, dead node detection |
| `AbstractArchiveStoreContract` | Job archiving and retrieval |
| `AbstractJobAuditStoreContract` | Execution history + per-job log persistence |
| `AbstractJobAnalyticsStoreContract` | Analytics/aggregation queries |
| `AbstractTagStoreContract` | Tag-based job queries |
| `AbstractWorkflowConditionStoreContract` | Workflow condition evaluation |
| `AbstractDlqAlertStoreContract` | DLQ alert lifecycle |
| `AbstractResourcePermitStoreContract` | Permit acquire and release |
| `AbstractDualWriteInvariantContract` | Cross-store invariants for dual hot/cold write paths |

Run all contract suites against your store implementation. All tests must pass before the store earns the "Ratchet Store Compatible" label. API and Jakarta-runtime compatibility are separate conformance tiers, validated by `ratchet-tck-api` and `ratchet-tck-jakarta` respectively.

### Adding the TCK Dependency

```xml
<dependency>
    <groupId>run.ratchet</groupId>
    <artifactId>ratchet-tck-store</artifactId>
    <version>${ratchet.version}</version>
    <scope>test</scope>
</dependency>
```

## Implementation Checklist

When implementing any SPI:

- [ ] **Thread safety** -- All SPI implementations are called from multiple threads concurrently. Use `@ApplicationScoped` (one instance, must be thread-safe) or ensure your instance handles concurrent access.

- [ ] **CDI proxy compatibility** -- If your implementation is `@ApplicationScoped`, include a protected no-arg constructor for the CDI proxy:

```java
@ApplicationScoped
public class MySpi implements SomeRatchetSpi {

    // Required by CDI proxy
    protected MySpi() {
        this.dependency = null;
    }

    @Inject
    public MySpi(SomeDependency dependency) {
        this.dependency = dependency;
    }
}
```

- [ ] **Null safety** -- Check the Javadoc for null contracts. Most SPI methods have non-null parameters, but exceptions may have null messages.

- [ ] **Exception handling** -- SPI methods should throw the documented exception types. Unexpected exceptions may cause the engine to fail jobs rather than retrying.

- [ ] **Lifecycle** -- Use `@PostConstruct` for initialization and `@PreDestroy` for cleanup (closing connections, shutting down thread pools).

- [ ] **Testing** -- Unit test your implementation in isolation, then integration test it within a CDI container to verify wiring.

## Quick Reference: SPI to Default Mapping

| SPI Interface | Default Implementation | CDI Scope | Module |
|---------------|----------------------|-----------|--------|
| `RatchetConfigSource` | Overlay for `RatchetOptionsFactory.fromEnvironment(...)` when the application's own config platform fronts env vars / MP Config | Optional `@ApplicationScoped` application bean | application |
| `JobInvocationResolver` | `DefaultJobInvocationResolver` | `@ApplicationScoped` | ratchet |
| `ResultPersistenceStrategy` | `DefaultResultPersistenceStrategy` | `@ApplicationScoped` | ratchet |
| `ExecutionTuningProvider` | `DefaultExecutionTuningProvider` | `@ApplicationScoped` | ratchet |
| `PollingStrategyProvider` | `DefaultPollingStrategyProvider` | `@ApplicationScoped` | ratchet |
| `CircuitBreakerConfigProvider` | `DefaultCircuitBreakerConfigProvider` | `@ApplicationScoped` | ratchet |
| `SchedulerLifecycleHook` | No default hook | Optional `@ApplicationScoped` alternative | application |
| `RetryPolicy` | `DefaultRetryPolicy` | `@ApplicationScoped` | ratchet |
| `ResilienceStrategy` | `DefaultResilienceStrategy` | Produced by `RatchetProducer` | ratchet |
| `MetricsCollector` | `NoOpMetricsCollector` | `@ApplicationScoped` | ratchet-api |
| `JobLoggerFactory` | `DefaultJobLoggerFactory` | `@ApplicationScoped` | ratchet |
| `StartupCoordinator` | `StoreBackedStartupCoordinator` | `@ApplicationScoped` | ratchet |
| `ClassPolicy` | `PackagePrefixClassPolicy` | Produced by `RatchetProducer` | ratchet |
| `BeanResolver` | `CdiBeanResolver` | `@ApplicationScoped` | ratchet |
| `ExecutorProvider` | `DefaultExecutorProvider` | `@ApplicationScoped` | ratchet |
| `NodeIdentityProvider` | `DefaultNodeIdentityProvider` | Produced by `RatchetProducer` | ratchet |
| `ClusterCoordinator` | `NoOpClusterCoordinator` | `@ApplicationScoped` | ratchet |
| `ErrorSanitizer` | `DefaultErrorSanitizer` | Produced by `RatchetProducer` | ratchet |
| `JobStore` | MySQL / PostgreSQL / Oracle / SQL Server / MongoDB | `@ApplicationScoped` | ratchet-store-* |
