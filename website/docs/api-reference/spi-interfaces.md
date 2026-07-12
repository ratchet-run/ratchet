---
sidebar_position: 13
title: SPI Interfaces Reference
description: Extension points for customizing Ratchet behavior including retry, resilience, serialization, metrics, and clustering.
---

# SPI Interfaces Reference

Extension points for customizing Ratchet behavior. All SPI interfaces live in the `run.ratchet.spi` package. To provide a custom implementation, create a CDI bean annotated with `@Alternative @Priority(APPLICATION)`.

## Registering an SPI implementation

```java
@Alternative
@Priority(APPLICATION)
@ApplicationScoped
public class MyCustomRetryPolicy implements RetryPolicy {
    // Your implementation
}
```

Ensure CDI auto-discovery is enabled or register in `beans.xml`.

## RetryPolicy

Controls retry and backoff decisions. The default RI implementation (`DefaultRetryPolicy`) is a passthrough that defers to the job's configured `BackoffPolicy` and `maxRetries`.

```java
@Incubating
public interface RetryPolicy {
    boolean shouldRetry(int attempt, Throwable cause);
    Duration getDelay(int attempt);
}
```

### shouldRetry

```java
boolean shouldRetry(int attempt, Throwable cause)
```

Determines whether a retry attempt should be made.

**Parameters:**
- `attempt` -- the current retry attempt number, starting from 1.
- `cause` -- the throwable that caused the failure.

**Returns:** `true` if another retry should be attempted; `false` to give up.

### getDelay

```java
Duration getDelay(int attempt)
```

Calculates the delay before the next retry.

**Parameters:**
- `attempt` -- the current retry attempt number, starting from 1.

**Returns:** the delay duration before the next retry.

### Example

```java
@Alternative @Priority(APPLICATION)
@ApplicationScoped
public class SmartRetryPolicy implements RetryPolicy {

    @Override
    public boolean shouldRetry(int attempt, Throwable cause) {
        // Never retry validation errors
        if (cause.getClass().isAnnotationPresent(DoNotRetry.class)) {
            return false;
        }
        // Retry transient errors up to 5 times
        if (cause instanceof TransientException) {
            return attempt <= 5;
        }
        // Default: retry up to 3 times
        return attempt <= 3;
    }

    @Override
    public Duration getDelay(int attempt) {
        // Exponential backoff: 1s, 2s, 4s, 8s...
        return Duration.ofSeconds((long) Math.pow(2, attempt - 1));
    }
}
```

## ResilienceStrategy

Wraps job execution with resilience patterns such as circuit breakers. The default RI provides a count-based circuit breaker. Users can plug in Resilience4j or MicroProfile Fault Tolerance.

:::info
This interface is marked `@Incubating` and may change.
:::

```java
@Incubating
public interface ResilienceStrategy {
    <T> T execute(String serviceName, Callable<T> task) throws Exception;
    boolean isServiceAvailable(String serviceName);
    default Duration getRetryDelay(String serviceName);
}
```

### execute

```java
<T> T execute(String serviceName, Callable<T> task) throws Exception
```

Executes the task with resilience protection.

**Parameters:**
- `serviceName` -- identifies the service being protected (from `@CircuitBreakerProtected.service()`).
- `task` -- the callable to execute.

**Returns:** the result of the task.

**Throws:** `CircuitBreakerOpenException` if a circuit-open rejection prevents the task from running; `Exception` if the task itself fails.

### isServiceAvailable

```java
boolean isServiceAvailable(String serviceName)
```

Checks whether calls to the named service are currently permitted (circuit not open). This is an advisory pre-check; callers still need to handle `CircuitBreakerOpenException` from `execute()`.

### getRetryDelay

```java
default Duration getRetryDelay(String serviceName)
```

Returns the recommended delay before retrying work rejected because the circuit is open. Implementations must not return `null` or a negative duration. Default implementation returns 30 seconds.

### Example

```java
@Alternative @Priority(APPLICATION)
@ApplicationScoped
public class Resilience4jStrategy implements ResilienceStrategy {

    private final Map<String, CircuitBreaker> breakers = new ConcurrentHashMap<>();

    @Override
    public <T> T execute(String serviceName, Callable<T> task) throws Exception {
        CircuitBreaker cb = breakers.computeIfAbsent(serviceName,
            name -> CircuitBreaker.ofDefaults(name));
        return cb.executeCallable(task);
    }

    @Override
    public boolean isServiceAvailable(String serviceName) {
        CircuitBreaker cb = breakers.get(serviceName);
        return cb == null || cb.getState() != CircuitBreaker.State.OPEN;
    }
}
```

## ClassPolicy

Controls which classes may be deserialized during job payload restoration. This is a security measure to prevent deserialization attacks.

:::info
This interface is marked `@Incubating` and may change.
:::

```java
@Incubating
public interface ClassPolicy {
    boolean isAllowed(String className);
}
```

### isAllowed

```java
boolean isAllowed(String className)
```

**Parameters:**
- `className` -- the fully qualified class name to check.

**Returns:** `true` if the class is allowed to be deserialized.

### Example

```java
@Alternative @Priority(APPLICATION)
@ApplicationScoped
public class StrictClassPolicy implements ClassPolicy {

    private static final Set<String> ALLOWED_PREFIXES = Set.of(
        "run.ratchet.",
        "com.myapp.",
        "java.lang.",
        "java.util."
    );

    @Override
    public boolean isAllowed(String className) {
        return ALLOWED_PREFIXES.stream().anyMatch(className::startsWith);
    }
}
```

## ErrorSanitizer

Sanitizes exception information before persistence to the job store or publication in events. Prevents leaking sensitive data (API keys, passwords, PII) in error messages.

:::info
This interface is marked `@Incubating` and may change.
:::

```java
@Incubating
public interface ErrorSanitizer {
    String sanitize(Throwable ex);
}
```

### sanitize

```java
String sanitize(Throwable ex)
```

**Parameters:**
- `ex` -- the exception to sanitize (never null).

**Returns:** a sanitized string representation suitable for database storage.

Implementations should:
- Preserve the exception class name for diagnostic value
- Truncate overly long messages
- Strip patterns containing sensitive data
- Return a non-null string

### Example

```java
@Alternative @Priority(APPLICATION)
@ApplicationScoped
public class PiiSanitizer implements ErrorSanitizer {

    private static final int MAX_LENGTH = 2000;

    @Override
    public String sanitize(Throwable ex) {
        String msg = ex.getClass().getName() + ": " + ex.getMessage();
        msg = msg.replaceAll("api[_-]?key=\\w+", "api_key=***")
                 .replaceAll("password=\\S+", "password=***")
                 .replaceAll("[\\w.]+@[\\w.]+", "***@***");
        return msg.length() > MAX_LENGTH ? msg.substring(0, MAX_LENGTH) : msg;
    }
}
```

## PayloadMaskingPolicy

Decides which payload fields are masked before a payload leaves the framework on a read or observability path (for example the `params` map on a job detail when `maskPayloads` is enabled). The durable store payload and anything a worker needs to execute are never altered. The built-in policy matches a fixed set of common credential and PII field names (`password`, `token`, `ssn`, ...); deployers produce their own implementation to change the field set.

```java
@Incubating
public interface PayloadMaskingPolicy {
    boolean isSensitiveField(String fieldName);

    default boolean isSensitiveField(String fieldName, MaskingContext context) {
        return isSensitiveField(fieldName);
    }
}

public record MaskingContext(UUID jobId, Map<String, String> jobProperties) {}
```

The context-aware overload exists for per-job decisions — the same field name can be secret on one job and not on another. `MaskingContext` carries the owning job's id and its extension properties, populated when the store advertises the `JobExtensionStore` capability (empty map otherwise). Existing name-only policies keep working unchanged through the default method.

## PayloadEncryption

Keyed authenticated-encryption (AEAD) engine that protects sensitive job data at rest. The engine owns its nonce and returns an opaque body carrying everything decryption needs except the key and the additional authenticated data. `algorithmId()` is recorded in each value's envelope so the matching engine is selected at read time. Reference AES-256-GCM and XChaCha20-Poly1305 engines ship in `ratchet-encryption`.

```java
@Incubating
public interface PayloadEncryption {
    String algorithmId();
    byte[] encrypt(byte[] plaintext, EncryptionContext ctx);
    byte[] decrypt(byte[] ciphertext, EncryptionContext ctx);
}
```

See the [Payload Encryption](/advanced/payload-encryption) guide for setup and the protected-surface boundary.

## KeyProvider

Owns key storage, the active write key, lookup by id, and the rotation lifecycle. Replace this seam to back encryption with a static key, an environment variable, a JCA `KeyStore`, or an external service such as AWS KMS, GCP KMS, or HashiCorp Vault. `currentKey()` answers "which key do I write with now?"; `keyById()` resolves a recorded key id. A provider must keep old keys resolvable until every row written under them has been rewritten or deleted. `SecretKeyProvider` in `ratchet-encryption` is a ready-made static-key implementation. `WrappedKeyProvider` (in `run.ratchet.spi`) is a sub-interface for KMS-wrapped data keys.

```java
@Incubating
public interface KeyProvider {
    EncryptionKey currentKey();
    EncryptionKey keyById(String keyId);
}
```

## EncryptionContext

The framework-computed context passed to every `encrypt` and `decrypt` call. It identifies the surface being protected (which job, which column) so the engine can bind the ciphertext to it as additional authenticated data, which is what stops a value from being moved between rows or fields. Applications never construct it; Ratchet hands it to the engine.

## JobInvocationResolver

Custom callback-to-job invocation resolution. The default RI uses ASM to derive a persisted target class, method, method descriptor, static flag, and argument list from serializable callbacks.

```java
@Incubating
public interface JobInvocationResolver {
    JobInvocation resolve(Serializable callback);
    JobInvocation resolve(Serializable callback, List<Object> runtimeArguments);
}
```

## InvocationSubmissionService

Submission seam for trusted extensions that construct `JobInvocation`s directly instead of going through the lambda-serializing builders on `JobSchedulerService`. Every submission converges on the same creation path as the public builders — same validation, idempotency, tags, batch metadata, workflow condition rows, and wakeup behavior. There is no separate persistence path.

The seam is trusted by convention, not a security barrier: anything that can inject it can persist an arbitrary invocation, exactly as `JobInvocationResolver` consumers can. An extension that accepts external input (block names, workflow definitions) must validate and authorize it before constructing the invocation. Submissions still pass the full standard chain — payload validation, the deployment's class policy, and `JobAuthorizationPolicy.checkCreate()` at persistence time; the worker re-applies the security validation (public-method and signature checks, `checkExecute()`) at dispatch.

```java
@Incubating
public interface InvocationSubmissionService {
    InvocationJobBuilder enqueueInvocation(JobInvocation invocation);
    InvocationJobBuilder scheduleInvocation(Duration delay, JobInvocation invocation);
    InvocationBatchBuilder enqueueInvocationBatch(String name);
    <T extends Serializable> InvocationStreamingBatchBuilder<T> invocationStreamingBatch(String name);
    WorkflowCondition invocationCondition(JobInvocation invocation);
}
```

`JobInvocation` is the serializable description of the dispatch target: fully qualified class name, method name, JVM method descriptor, static flag, and the argument list. Arguments must be JSON-representable — persistence converts the invocation through the payload factory, never JDK serialization.

The three builders mirror their public counterparts. `InvocationJobBuilder` carries the options and chaining surface of [JobBuilder](/api-reference/job-builder) (`then`, `branch`, `awaitSignal`, `withTags`, `withEncryptedPayload`, ...) with a `JobInvocation` in place of each lambda. `InvocationBatchBuilder` and `InvocationStreamingBatchBuilder<T>` mirror [BatchBuilder and StreamingBatchBuilder](/api-reference/batch-builder); the streaming variant expands each stream item into a child job through a `Function<T, JobInvocation>` factory and inherits the chunk-boundary persistence and replay behavior. A streaming chunk whose bulk insert fails emits [`BatchChunkFailureEvent`](/api-reference/event-system#batchchunkfailureevent) before the submission transaction rolls back.

`invocationCondition(...)` wraps a pre-resolved invocation as a `CUSTOM` workflow condition. The target method is dispatched reflectively at evaluation time with the parent's `JobResult` supplied as the trailing argument and must return `boolean`; the invocation persists as the condition's expression payload and is encrypted under the parent's predicate surface when payload encryption applies.

## PreExecutionArgResolver

Worker-side hook that can patch a job's invocation **arguments** at the last moment before reflective dispatch — after security validation, before argument coercion. Extensions use it for late binding: resolving placeholder arguments against state that did not exist at submission time, such as upstream step results in a workflow.

```java
@Incubating
public interface PreExecutionArgResolver {
    JobInvocation resolveArguments(UUID jobId, JobInvocation invocation);
}
```

Arguments only: the dispatched target class, method, and descriptor stay pinned to the payload that security validation cleared, and only the returned invocation's arguments are honored. Returning `null` or the input instance dispatches the persisted arguments unchanged, as does leaving the hook unregistered. A thrown exception fails the job through the normal failure path, so the retry policy applies. The hook covers the job's main invocation; success/failure callback payloads and workflow-condition predicates follow their own dispatch paths and are not resolved.

## ResultPersistenceStrategy

Serializes job return values before storing them on the job row.

```java
@Incubating
public interface ResultPersistenceStrategy {
    SerializedJobResult serialize(UUID jobId, Object result);
}
```

## RatchetConfig

Typed runtime configuration facade used internally by `RatchetOptionsFactory` to resolve keys against a chain of `RatchetConfigSource` instances. Most applications never interact with this directly; they either build `RatchetOptions` programmatically or call `RatchetOptionsFactory.fromEnvironment()` from their producer (see [Configuration](/getting-started/configuration)).

```java
@Incubating
public interface RatchetConfig {
    <T> T get(RatchetConfigKey<T> key);
    Optional<String> raw(RatchetConfigKey<?> key);
}
```

## RatchetConfigSource

Raw configuration source read by `RatchetOptionsFactory.fromEnvironment(RatchetConfigSource...)`. Pass instances as varargs from your `RatchetOptions` producer to overlay a platform-specific source ahead of the ambient MicroProfile Config / environment variable chain.

```java
@Incubating
public interface RatchetConfigSource {
    Optional<String> get(String propertyName, String environmentVariable);
}
```

## ExecutionTuningProvider

Controls per-execution-type concurrency and virtual-thread backpressure limits.

```java
@Incubating
public interface ExecutionTuningProvider {
    RatchetOptions.ThreadingMode defaultThreadingMode();
    int maxConcurrency(String executionTypeName, int defaultValue);
    int virtualThreadLimit(String executionTypeName, int defaultValue);
}
```

## PollingStrategyProvider

Creates the stateful adaptive polling delay strategy used by the RI poller.

```java
@Incubating
public interface PollingStrategyProvider {
    PollingDelayStrategy create(PollingConfig config);
}
```

## JobLoggerFactory

Creates the job-scoped `JobLogger` bound into `JobContext` for each execution.

```java
@Incubating
public interface JobLoggerFactory {
    JobLogger create(JobLoggerContext context);
}
```

## CircuitBreakerConfigProvider

Supplies enablement and per-profile settings for the built-in circuit breaker.

```java
@Incubating
public interface CircuitBreakerConfigProvider {
    boolean isEnabled();
    CircuitBreakerConfig configFor(CircuitBreakerProfile profile);
}
```

## SchedulerLifecycleHook

Optional CDI hook around scheduler startup and shutdown.

```java
@Incubating
public interface SchedulerLifecycleHook {
    default void beforeStart() {}
    default void afterStart() {}
    default void beforeStop() {}
    default void afterStop() {}
}
```

## ExecutorProvider

Provides thread pools for job execution and scheduling. Override to use virtual threads, custom thread factories, or managed executors.

:::info
This interface is marked `@Incubating` and may change.
:::

```java
@Incubating
public interface ExecutorProvider {
    ExecutorService getJobExecutor();
    ScheduledExecutorService getScheduledExecutor();
}
```

### getJobExecutor

```java
ExecutorService getJobExecutor()
```

Returns the executor for running job tasks.

### getScheduledExecutor

```java
ScheduledExecutorService getScheduledExecutor()
```

Returns the executor for scheduling polling and delayed tasks.

### Example

```java
@Alternative @Priority(APPLICATION)
@ApplicationScoped
public class VirtualThreadExecutorProvider implements ExecutorProvider {

    @Override
    public ExecutorService getJobExecutor() {
        return Executors.newVirtualThreadPerTaskExecutor();
    }

    @Override
    public ScheduledExecutorService getScheduledExecutor() {
        return Executors.newScheduledThreadPool(2, Thread.ofVirtual().factory());
    }
}
```

## BeanResolver

Resolves bean instances by type, abstracting the dependency injection mechanism. The default RI implementation, `CdiBeanResolver`, delegates to a CDI `Instance<Object>` (`select(type).get()`).

:::info
This interface is marked `@Incubating` and may change.
:::

```java
@Incubating
@FunctionalInterface
public interface BeanResolver {
    <T> T resolve(Class<T> type);
}
```

### resolve

```java
<T> T resolve(Class<T> type)
```

**Parameters:**
- `type` -- the class to resolve.

**Returns:** an instance of the specified type.

**Throws:** `IllegalStateException` if the instance cannot be resolved.

### Example

```java
@Alternative @Priority(APPLICATION)
@ApplicationScoped
public class SpringBeanResolver implements BeanResolver {

    @Inject ApplicationContext applicationContext;

    @Override
    public <T> T resolve(Class<T> type) {
        return applicationContext.getBean(type);
    }
}
```

## MetricsCollector

Emits job lifecycle metrics for integration with monitoring systems (Micrometer, StatsD, Prometheus, etc.).

:::info
This interface is marked `@Incubating` and may change. Additional lifecycle callbacks may be added in future releases.
:::

| Area | Callbacks |
|------|-----------|
| Job execution | `jobStarted(UUID, JobType, JobPriority)`, `jobCompleted(UUID, JobType, long)`, `jobFailed(UUID, JobType, Throwable, int)` |
| Success finalization | `successFinalizationRetried(UUID, JobType)`, `successFinalizationMinimal(UUID, JobType)`, `successFinalizationStuck(UUID, JobType)` |
| Claim and admission | `claimTransientFailure(String)`, `jobsClaimed(String, int)`, `gateRejected(String, String)` |
| Wakeups and routing | `localWakeup(String)`, `executionTargetFallback(String, String)`, `clusterWakeupPublished(String, String)`, `clusterWakeupReceived(String, String)` |
| Callbacks and signals | `callbackFailed(UUID, JobType, Throwable, int)`, `signalWaiting(UUID, JobType, String)`, `signalDelivered(UUID, JobType, String, SignalDecision.Outcome)`, `signalTimedOut(UUID, JobType, String)`, `signalCancelled(UUID, JobType, String)` |
| Store health | `storeOperation(String, String, String, long)`, `pollerBreakerState(String, String)` |
| Payload encryption | `encryptionIntegrityViolation(UUID, String)`, `encryptionEnvelopeVersionSkew(UUID, int, int)` |

The first ten callbacks are required for direct implementations; the rest have default no-op bodies for compatibility. See [Metrics Collection](../advanced/metrics-collection.md#metricscollector-spi) for callback semantics and the complete Micrometer meter catalog.

### Partial example

This example deliberately records only the three basic job outcomes. It extends `NoOpMetricsCollector` so that omission is explicit and the code remains compatible with the full SPI. A production replacement for the built-in Micrometer adapter should handle or delegate every callback.

```java
@Alternative @Priority(APPLICATION)
@ApplicationScoped
public class PartialMicrometerMetricsCollector extends NoOpMetricsCollector {

    @Inject MeterRegistry registry;

    @Override
    public void jobStarted(UUID jobId, JobType type, JobPriority priority) {
        registry.counter("ratchet.jobs.started", "type", type.name()).increment();
    }

    @Override
    public void jobCompleted(UUID jobId, JobType type, long executionTimeMs) {
        registry.counter("ratchet.jobs.completed", "type", type.name()).increment();
        registry.timer("ratchet.jobs.duration", "type", type.name())
            .record(executionTimeMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public void jobFailed(UUID jobId, JobType type, Throwable cause, int attempt) {
        registry.counter("ratchet.jobs.failed",
            "type", type.name(),
            "family", ExceptionFamily.classify(cause).name()).increment();
    }
}
```

## JobLogger

Custom logging backend for job execution. The default RI bridges to JBoss Logging.

:::info
This interface is marked `@Incubating` and may change.
:::

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

Each method accepts a single `String` message. Implementations are responsible for formatting, routing, and any context enrichment.

### Example

```java
@Alternative @Priority(APPLICATION)
@ApplicationScoped
public class StructuredJobLogger implements JobLogger {

    @Override
    public void info(String message) {
        logJson("INFO", message);
    }

    @Override
    public void debug(String message) {
        logJson("DEBUG", message);
    }

    @Override
    public void warn(String message) {
        logJson("WARN", message);
    }

    @Override
    public void error(String message) {
        logJson("ERROR", message);
    }

    @Override
    public void trace(String message) {
        logJson("TRACE", message);
    }

    private void logJson(String level, String message) {
        System.out.printf("{\"level\":\"%s\",\"message\":\"%s\",\"timestamp\":\"%s\"}%n",
            level, message, Instant.now());
    }
}
```

## ClusterCoordinator

Coordinates job scheduling across cluster nodes. Enables distributed wakeup notifications so that when a job is submitted on one node, other nodes immediately check for available work.

:::info
This interface is marked `@Incubating` and may change.
:::

```java
@Incubating
public interface ClusterCoordinator extends AutoCloseable {
    void notifyNewWork(JobPriority priority, NodeIdentity source, String executionTarget);
    void registerWakeupListener(Consumer<JobWakeupHint> listener);
    void close();
}
```

### notifyNewWork

```java
void notifyNewWork(JobPriority priority, NodeIdentity source, String executionTarget)
```

Publishes a best-effort notification that new work is available somewhere in the cluster. Called when jobs are submitted.

**Parameters:**
- `priority` -- the priority of the new work, allowing listeners to decide urgency; never null.
- `source` -- identity of the node submitting the notification, so subscribers can suppress self-wakeups; never null.
- `executionTarget` -- routing label of the originating job (e.g. `"platform"` or `"virtual"`); informational only, or null when the wakeup is not target-scoped.

### registerWakeupListener

```java
void registerWakeupListener(Consumer<JobWakeupHint> listener)
```

Registers a listener invoked when another node publishes new work. The listener receives a `JobWakeupHint` carrying priority, origin `NodeIdentity`, and optional execution target. Listeners must be fast, non-blocking, and thread-safe.

### close

```java
void close()
```

Releases transport resources held by this coordinator. Must be idempotent. Called by the scheduler lifecycle during shutdown.

## StartupCoordinator

Coordinates destructive startup work across nodes using a lease model. The reference implementation uses the store's distributed lock mechanism so startup cleanup does not require an external leader-election system.

:::info
This interface is marked `@Incubating` and may change.
:::

```java
@Incubating
public interface StartupCoordinator {
    boolean tryAcquire(String actionName, Duration leaseTtl);
    void release(String actionName);
}
```

### tryAcquire

```java
boolean tryAcquire(String actionName, Duration leaseTtl)
```

Attempts to acquire a startup lease for the named action.

### release

```java
void release(String actionName)
```

Releases a lease previously acquired by this node.

### Example

```java
@Alternative @Priority(APPLICATION)
@ApplicationScoped
public class RedisClusterCoordinator implements ClusterCoordinator {

    @Inject RedisClient redis;
    private Consumer<JobWakeupHint> wakeupListener;

    @Override
    public void notifyNewWork(JobPriority priority, NodeIdentity source, String executionTarget) {
        redis.publish("ratchet:wakeup", priority.name());
    }

    @Override
    public void registerWakeupListener(Consumer<JobWakeupHint> listener) {
        this.wakeupListener = listener;
        redis.subscribe("ratchet:wakeup", hint -> listener.accept(hint));
    }

    @Override
    public void close() {
        redis.close();
    }
}
```

## NodeIdentityProvider

Provides the unique identifier for the current node in a cluster. Used for job locking and heartbeat management.

:::info
This interface is marked `@Incubating` and may change.
:::

```java
@Incubating
public interface NodeIdentityProvider {
    String getNodeId();
}
```

### getNodeId

```java
String getNodeId()
```

Returns the unique, immutable node identifier. Must be consistent for the node's lifecycle.

### Example

```java
@Alternative @Priority(APPLICATION)
@ApplicationScoped
public class KubernetesNodeProvider implements NodeIdentityProvider {

    @Override
    public String getNodeId() {
        return System.getenv("HOSTNAME"); // Kubernetes pod name
    }
}
```

## NodeTagAffinityProvider

Restricts which jobs a worker node will claim, by tag. The provider returns a `NodeTagFilter` that the poller compiles into an `EXISTS` / `NOT EXISTS` guard on the claim query, so a node picks up only work whose tags match its affinity. The default applies no filter, and every node claims any job. This is the seam for pinning GPU, licensed, or tenant-specific work to the nodes that can run it.

```java
@Incubating
public interface NodeTagAffinityProvider {
    NodeTagFilter tagFilter();
}
```

See [Multi-Cell Deployment](/deployment/multi-cell) for a worked example.

## LambdaAnalyzer

Compatibility SPI for simple lambda metadata extraction. It is not the primary scheduler payload extension point; use `JobInvocationResolver`.

:::info
This interface is marked `@Incubating` and may change.
:::

```java
@Incubating
public interface LambdaAnalyzer {
    LambdaDescriptor analyze(Serializable lambda);
}
```

### LambdaDescriptor

```java
@Incubating
public record LambdaDescriptor(
    String targetClass,
    String methodName,
    String methodDescriptor,
    boolean isStatic,
    Object[] capturedArgs
)
```

| Component | Description |
|---|---|
| `targetClass` | Fully qualified name of the class containing the target method |
| `methodName` | Name of the target method |
| `methodDescriptor` | JVM method descriptor (e.g., `(Ljava/lang/String;)V`) |
| `isStatic` | Whether the method is static |
| `capturedArgs` | Arguments captured by the lambda closure |

### Example

```java
@Alternative @Priority(APPLICATION)
@ApplicationScoped
public class AsmLambdaAnalyzer implements LambdaAnalyzer {

    @Override
    public LambdaDescriptor analyze(Serializable lambda) {
        // Use ASM bytecode analysis to extract method reference details
        SerializedLambda sl = extractSerializedLambda(lambda);
        return new LambdaDescriptor(
            sl.getImplClass().replace('/', '.'),
            sl.getImplMethodName(),
            sl.getImplMethodSignature(),
            sl.getImplMethodKind() == MethodHandleInfo.REF_invokeStatic,
            extractCapturedArgs(sl));
    }
}
```

## See also

- [Annotations Reference](./annotations)
- [Event System](./event-system)
- [API Reference Overview](./overview)
