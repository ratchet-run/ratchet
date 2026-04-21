---
sidebar_position: 13
title: SPI Interfaces Reference
description: Extension points for customizing Ratchet behavior including retry, resilience, serialization, metrics, and clustering.
---

# SPI Interfaces Reference

Extension points for customizing Ratchet behavior. All SPI interfaces live in the `run.ratchet.spi` package. To provide a custom implementation, create a CDI bean annotated with `@Alternative @Priority(APPLICATION)`.

## Registering an SPI Implementation

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

**Throws:** `Exception` if the task fails or the service is unavailable.

### isServiceAvailable

```java
boolean isServiceAvailable(String serviceName)
```

Checks whether calls to the named service are currently permitted (circuit not open).

### getRetryDelay

```java
default Duration getRetryDelay(String serviceName)
```

Returns the recommended delay before retrying work rejected because the service is unavailable. Default implementation returns 30 seconds.

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

## JobInvocationResolver

Custom callback-to-job invocation resolution. The default RI uses ASM to derive a persisted target class, method, method descriptor, static flag, and argument list from serializable callbacks.

```java
public interface JobInvocationResolver {
    JobInvocation resolve(Serializable callback);
    JobInvocation resolve(Serializable callback, List<Object> runtimeArguments);
}
```

## ResultPersistenceStrategy

Serializes job return values before storing them on the job row.

```java
public interface ResultPersistenceStrategy {
    SerializedJobResult serialize(long jobId, Object result);
}
```

## RatchetConfig

Typed runtime configuration facade used by the RI to build `RatchetOptions` when the application does not produce options directly. Most applications should produce `RatchetOptions`; this contract exists for platform source-chain integrations.

```java
public interface RatchetConfig {
    <T> T get(RatchetConfigKey<T> key);
    Optional<String> raw(RatchetConfigKey<?> key);
}
```

## RatchetConfigSource

Raw configuration source used by `RatchetConfig`. CDI-provided sources are consulted before optional MicroProfile Config, environment variables, system properties, and built-in defaults.

```java
public interface RatchetConfigSource {
    Optional<String> get(String propertyName, String environmentVariable);
}
```

## ExecutionTuningProvider

Controls per-execution-type concurrency and virtual-thread backpressure limits.

```java
public interface ExecutionTuningProvider {
    boolean useVirtualThreads();
    int maxConcurrency(String executionTypeName, int defaultValue);
    int virtualThreadLimit(String executionTypeName, int defaultValue);
}
```

## PollingStrategyProvider

Creates the stateful adaptive polling delay strategy used by the RI poller.

```java
public interface PollingStrategyProvider {
    PollingDelayStrategy create(PollingConfig config);
}
```

## JobLoggerFactory

Creates the job-scoped `JobLogger` bound into `JobContext` for each execution.

```java
public interface JobLoggerFactory {
    JobLogger create(JobLoggerContext context);
}
```

## CircuitBreakerConfigProvider

Supplies enablement and per-profile settings for the built-in circuit breaker.

```java
public interface CircuitBreakerConfigProvider {
    boolean isEnabled();
    CircuitBreakerConfig configFor(CircuitBreakerProfile profile);
}
```

## SchedulerLifecycleHook

Optional CDI hook around scheduler startup and shutdown.

```java
public interface SchedulerLifecycleHook {
    default void beforeStart() {}
    default void afterStart() {}
    default void beforeStop() {}
    default void afterStop() {}
}
```

## SerializationStrategy

Compatibility SPI for object-to-byte serialization utilities. It is not the primary scheduler payload extension point; use `JobInvocationResolver` for submitted callbacks and `ResultPersistenceStrategy` for return values.

```java
public interface SerializationStrategy {
    byte[] serialize(Object obj);
    <T> T deserialize(byte[] data, Class<T> type);
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

Resolves bean instances by type, abstracting the dependency injection mechanism. The CDI implementation delegates to `CDI.current().select(type).get()`. The default RI implementation uses reflection.

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

```java
@Incubating
public interface MetricsCollector {
    void jobStarted(long jobId, JobType type, JobPriority priority);
    void jobCompleted(long jobId, JobType type, long executionTimeMs);
    void jobFailed(long jobId, JobType type, Throwable cause, int attempt);
}
```

### jobStarted

```java
void jobStarted(long jobId, JobType type, JobPriority priority)
```

Called when a job begins execution.

### jobCompleted

```java
void jobCompleted(long jobId, JobType type, long executionTimeMs)
```

Called when a job completes successfully.

### jobFailed

```java
void jobFailed(long jobId, JobType type, Throwable cause, int attempt)
```

Called when a job fails. The `attempt` parameter is 1-based and includes the failure being reported.

### Example

```java
@Alternative @Priority(APPLICATION)
@ApplicationScoped
public class MicrometerMetricsCollector implements MetricsCollector {

    @Inject MeterRegistry registry;

    @Override
    public void jobStarted(long jobId, JobType type, JobPriority priority) {
        registry.counter("ratchet.jobs.started", "type", type.name()).increment();
    }

    @Override
    public void jobCompleted(long jobId, JobType type, long executionTimeMs) {
        registry.counter("ratchet.jobs.completed", "type", type.name()).increment();
        registry.timer("ratchet.jobs.duration", "type", type.name())
            .record(executionTimeMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public void jobFailed(long jobId, JobType type, Throwable cause, int attempt) {
        registry.counter("ratchet.jobs.failed",
            "type", type.name(),
            "exception", cause.getClass().getSimpleName()).increment();
    }
}
```

## JobLogger

Custom logging backend for job execution. The default RI bridges to `java.util.logging`.

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
public interface ClusterCoordinator {
    void notifyNewWork(JobPriority priority);
    void registerWakeupListener(Runnable listener);
}
```

### notifyNewWork

```java
void notifyNewWork(JobPriority priority)
```

Publishes a notification that new work is available. Called when jobs are submitted.

**Parameters:**
- `priority` -- the priority of the new work, allowing listeners to decide urgency.

### registerWakeupListener

```java
void registerWakeupListener(Runnable listener)
```

Registers a listener invoked when another node publishes new work.

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
    private Runnable wakeupListener;

    @Override
    public void notifyNewWork(JobPriority priority) {
        redis.publish("ratchet:wakeup", priority.name());
    }

    @Override
    public void registerWakeupListener(Runnable listener) {
        this.wakeupListener = listener;
        redis.subscribe("ratchet:wakeup", msg -> listener.run());
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

## See Also

- [Annotations Reference](./annotations)
- [Event System](./event-system)
- [API Reference Overview](./overview)
