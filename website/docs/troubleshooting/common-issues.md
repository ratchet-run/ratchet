---
sidebar_position: 2
title: Common Issues
description: Solutions for frequently encountered problems with Ratchet job scheduling, serialization, security, and database errors.
---

# Common Issues

This page covers the most frequently encountered issues when running Ratchet, along with their root causes and solutions.

## Jobs Not Executing

**Symptom:** Jobs are submitted successfully (you get a `JobHandle` back) but never run.

### Check 1: Is the Poller Running?

The poller is the heartbeat of Ratchet. If it is not running, no jobs will be claimed for execution.

Look for this log message at startup:

```
Poller initialized (batch=50)
```

If you do not see it, the `RatchetLifecycle` CDI bean may not be initializing. Ensure your `beans.xml` has `bean-discovery-mode="all"` or that Ratchet's packages are included in scanning:

```xml
<!-- META-INF/beans.xml -->
<beans xmlns="https://jakarta.ee/xml/ns/jakartaee"
       version="4.0"
       bean-discovery-mode="all">
</beans>
```

### Check 2: Are Jobs Stuck in PENDING?

```sql
SELECT job_id, status, scheduled_time, job_type, attempts, last_error
FROM scheduler_job
WHERE status = 'PENDING'
  AND scheduled_time <= NOW()
ORDER BY scheduled_time ASC
LIMIT 20;
```

If you see rows with `scheduled_time` in the past, the poller is either not running, or the thread pool is at capacity. Check thread pool utilization by looking for the `ThreadPoolManager` log messages.

### Check 3: Did startup fail because of ClassPolicy?

The most common cause of a broken fresh deployment is an empty `ClassPolicy` allowlist. Ratchet ships with that empty by design and refuses to start until you provide an override. Look for this log message:

```
ERROR: ClassPolicy allowedPackages is empty — refusing to start. Provide an
@Alternative @Priority(APPLICATION) ClassPolicy bean with your application's package
prefixes, or opt out (ONLY for demos/tests) with
-Dratchet.allow-empty-class-policy=true
```

You must provide a `ClassPolicy` bean that allows your application packages:

```java
@Alternative
@Priority(APPLICATION)
@ApplicationScoped
public class AppClassPolicy implements ClassPolicy {
    private static final Set<String> ALLOWED = Set.of(
        "com.mycompany.myapp."
    );

    @Override
    public boolean isAllowed(String className) {
        return ALLOWED.stream().anyMatch(className::startsWith);
    }
}
```

If you explicitly set `-Dratchet.allow-empty-class-policy=true`, the application will start but the default policy still rejects every job target. In that opt-out mode, "jobs never run" is expected until you install a real `ClassPolicy`.

### Check 4: Is the Database Accessible?

Verify the datasource is working by checking for connection errors in your server logs. A misconfigured JTA datasource will cause the poller's `claimNextBatchOptimized` call to fail silently.

## Serialization Errors

**Symptom:** `ClassNotFoundException`, `NoSuchMethodException`, or `IllegalStateException` when jobs try to execute.

### Lambda Must Be a Method Reference

Ratchet uses ASM bytecode analysis to serialize lambda expressions. This means the lambda you pass to `enqueue()` must be a **single method reference**, not an inline lambda with complex logic:

```java
// This works - single method reference
scheduler.enqueue(myService::processData);

// This works - no-arg runnable
scheduler.enqueue(() -> myService.processData());

// This may fail - captured variables must be Serializable
String name = "test";
scheduler.enqueue(() -> myService.processData(name));  // 'name' is captured
```

If you see `IllegalStateException` during serialization, ensure:
1. The target class is accessible from the thread context classloader
2. The method is `public`
3. Any captured arguments implement `java.io.Serializable`

### Target Class Not Found at Execution Time

```
SEVERE: Job 12345 target class not found: com.myapp.jobs.OldService
```

This happens when:
- The class was renamed or removed after the job was enqueued
- A hot-redeploy changed the classloader and stale jobs reference the old classloader
- The class is in a module/WAR that is not visible to Ratchet's classloader

**Solution:** For redeployment scenarios, either drain the queue before redeploying or ensure class names remain stable across versions.

### Method Not Found or Not Public

```
SEVERE: Job 12345 target method not found: processData with descriptor (Ljava/lang/String;)V
```

Ratchet requires the target method to be `public`. If the method is `private`, `protected`, or package-private, you will see:

```
SecurityException: Method processData in class com.myapp.MyService is private
    — only public methods can be scheduled as jobs. Change the method visibility to public.
```

## ClassPolicy Rejecting Deserialization

**Symptom:** Jobs fail immediately with a `SecurityException` mentioning "not allowed for job execution."

```
SecurityException: Class com.myapp.service.PaymentService is not allowed for job execution.
```

The `PackagePrefixClassPolicy` checks if the target class name starts with any of the configured package prefixes. The default set is **empty**, meaning all classes are rejected.

**Diagnosis:**

```sql
-- Find which classes are being rejected
SELECT DISTINCT payload::jsonb ->> 'target' as target_class, last_error
FROM scheduler_job
WHERE status = 'FAILED'
  AND last_error LIKE '%not allowed%'
ORDER BY target_class;
```

**Solution:** Register a `ClassPolicy` bean that includes your application packages. See the example in the "Jobs Not Executing" section above.

:::caution
Do not add broad prefixes like `java.` or `javax.` to your allowed packages. The ClassPolicy exists to prevent remote code execution attacks where an attacker could invoke `Runtime.getRuntime().exec()` through a crafted job payload.
:::

## Duplicate Recurring Jobs

**Symptom:** The same recurring job runs multiple times per scheduled interval.

Recurring jobs use a **business key** for active-uniqueness. The database enforces a partial unique index:

```sql
-- PostgreSQL: only one active job per business key
CREATE UNIQUE INDEX idx_job_active_business_key
    ON scheduler_job (business_key)
    WHERE status IN ('PENDING', 'RUNNING', 'PAUSED')
      AND business_key IS NOT NULL;
```

Duplicates happen when:

1. **Idempotency key collision** -- two different jobs accidentally share the same idempotency key (each job gets a unique UUID by default, so this is rare)
2. **Business key not set** -- if you are creating recurring jobs programmatically without a business key, the uniqueness constraint does not apply
3. **Redeployment timing** -- the old recurring instance completes (moves to SUCCEEDED) just as the new deployment creates a fresh one

**Diagnosis:**

```sql
-- Check for duplicate active recurring jobs
SELECT business_key, COUNT(*) as active_count
FROM scheduler_job
WHERE job_type = 'RECURRING'
  AND status IN ('PENDING', 'RUNNING', 'PAUSED')
  AND business_key IS NOT NULL
GROUP BY business_key
HAVING COUNT(*) > 1;
```

**Solution:** Ratchet handles this automatically during startup via `RecurringAnnotationMaintenanceService`, which cancels orphaned recurring jobs whose `@Recurring` annotations no longer exist. If you are scheduling recurring jobs programmatically, always set a business key:

```java
scheduler.scheduleRecurring("0 */5 * * * ?", ZoneId.of("UTC"), myService::cleanup)
    .withBusinessKey("cleanup-job")
    .submit();
```

## Circuit Breaker Stuck Open

**Symptom:** Jobs for a specific service keep getting rescheduled with the message "Circuit breaker OPEN for service: X"

The built-in circuit breaker uses a sliding window of the last N calls (default 100). When the failure rate exceeds the threshold (default 50%), the circuit opens and stays open for a configured wait duration (default 30 seconds).

**Diagnosis:**

```
INFO: Job 12345 skipped - circuit breaker OPEN for service: PaymentService.charge
```

The circuit breaker transitions:
- **CLOSED** (normal) -- tracks success/failure in a sliding window
- **OPEN** (tripped) -- all calls rejected immediately; waits for the configured duration
- **HALF_OPEN** (testing) -- allows a small number of trial calls; if they succeed, returns to CLOSED; if any fail, returns to OPEN

**Why it gets "stuck":** If the underlying service is still failing when the circuit transitions to HALF_OPEN, the first trial call fails and the circuit immediately reopens. This cycle continues until the service recovers.

**Solutions:**

1. Fix the underlying service failure
2. Temporarily increase the wait duration to reduce retry pressure:
   ```bash
   export RATCHET_CB_DEFAULT_WAIT_SECONDS=120
   ```
3. If the circuit breaker is not appropriate for your use case, disable it:
   ```bash
   export RATCHET_CIRCUIT_BREAKER_ENABLED=false
   ```

**Tuning parameters:**

| Variable | Default | Purpose |
|---|---|---|
| `RATCHET_CB_DEFAULT_FAILURE_RATE` | `50` | Failure percentage to trip the breaker |
| `RATCHET_CB_DEFAULT_WAIT_SECONDS` | `30` | Seconds the circuit stays open |
| `RATCHET_CB_DEFAULT_WINDOW_SIZE` | `100` | Sliding window size for rate calculation |
| `RATCHET_CB_EXTERNAL_FAILURE_RATE` | `60` | Failure rate for external service profiles |
| `RATCHET_CB_EXTERNAL_WAIT_SECONDS` | `60` | Wait duration for external service profiles |

## Database Constraint Violations

**Symptom:** `ConstraintViolationException` or duplicate key errors in the logs.

### Idempotency Key Violation

```
ERROR: duplicate key value violates unique constraint "uk_idempotency_key"
```

Each job gets a unique idempotency key (UUID). This error means you are submitting the same job twice. Ratchet includes retry logic for idempotency conflicts (configurable via `RATCHET_IDEMPOTENCY_RETRY_MAX_ATTEMPTS`, default 3).

If you see persistent failures, check if your code is double-submitting in a retry loop.

### Active Business Key Violation

```
ERROR: duplicate key value violates unique constraint "idx_job_active_business_key"
```

Two active jobs (PENDING, RUNNING, or PAUSED) share the same business key. This is expected behavior -- the constraint prevents duplicate scheduling. The job that violated the constraint was correctly rejected.

If this is unexpected, query for the existing active job:

```sql
SELECT job_id, status, scheduled_time, created_at
FROM scheduler_job
WHERE business_key = 'your-business-key'
  AND status IN ('PENDING', 'RUNNING', 'PAUSED');
```

## Timeout Behavior

**Symptom:** Jobs are killed after a period of time with "Hard timeout exceeded."

Ratchet enforces timeouts using a watchdog thread that monitors each job execution:

1. **Soft timeout** (default 80% of limit): Logs a warning but does not interrupt the job
2. **Hard timeout** (100% of limit): Cancels the `Future` via `Thread.interrupt()` and marks the job FAILED

```
WARNING: Job 12345 approaching timeout - 80% threshold reached. Elapsed: 24m 0s, Timeout: 1800s
SEVERE: Job 12345 exceeded timeout of 1800s. Cancelling execution. Elapsed: 30m 1s
```

**Configuration:**

- Per-job timeout: set `timeoutSec` on the job entity (via `JobBuilder.withTimeout()`)
- Global default: `RATCHET_WORKER_DEFAULT_SLA` (default 1800 seconds / 30 minutes)
- Soft timeout percentage: `RATCHET_SOFT_TIMEOUT_PERCENT` (default 80)

**Important:** The hard timeout uses `Future.cancel(true)`, which sets the thread's interrupt flag. Your job code must check `Thread.interrupted()` or handle `InterruptedException` to stop cleanly. If your job ignores interrupts (e.g., stuck in a tight CPU loop with no blocking calls), the timeout cannot forcefully kill it.

**After timeout:** If the job has retries remaining, it is rescheduled for another attempt. If retries are exhausted, it moves to the DLQ.

## Thread Pool Exhaustion

**Symptom:** Jobs stay in PENDING even though the poller is running and claiming jobs.

Ratchet uses type-isolated thread pools with semaphore-based concurrency limits. Each job execution type has its own pool:

| Job Type | Default Pool Size | Env Variable |
|---|---|---|
| `SINGLE` | 20 | `RATCHET_THREAD_POOL_SIZE_SINGLE` |
| `RECURRING` | 5 | `RATCHET_THREAD_POOL_SIZE_RECURRING` |
| `BATCH_CHILD` | 30 | `RATCHET_THREAD_POOL_SIZE_BATCH_CHILD` |
| `BATCH_PARENT` | 2 | `RATCHET_THREAD_POOL_SIZE_BATCH_PARENT` |
| `CHAIN_STEP` | 10 | `RATCHET_THREAD_POOL_SIZE_CHAIN` |

When a pool is at capacity, the poller skips claiming jobs of that type. Look for:

```
ThreadPoolManager initialized with managed executors with semaphore-based limiting
```

**Diagnosis:**

```sql
-- Check how many jobs are currently RUNNING per type
SELECT job_type, COUNT(*) as running
FROM scheduler_job
WHERE status = 'RUNNING'
GROUP BY job_type
ORDER BY running DESC;
```

If the running count equals the pool size for a type, the pool is saturated.

**Solutions:**

1. **Increase pool size** for the bottleneck type via environment variables
2. **Enable virtual threads** to remove fixed pool limits:
   ```bash
   export RATCHET_WORKER_USE_VIRTUAL_THREADS=true
   ```
   Virtual threads still have configurable concurrency limits (default 1000 per type) to prevent unbounded growth.
3. **Check for stuck jobs** -- long-running jobs hold their thread slot until they complete or timeout:
   ```sql
   SELECT job_id, job_type, picked_at,
          EXTRACT(EPOCH FROM (NOW() - picked_at)) / 60 as running_minutes
   FROM scheduler_job
   WHERE status = 'RUNNING'
   ORDER BY picked_at ASC
   LIMIT 10;
   ```

## CDI Wiring Problems

**Symptom:** `UnsatisfiedResolutionException` or `AmbiguousResolutionException` at deployment time.

### Missing SPI Implementations

Ratchet requires several SPI beans to be present in the CDI container. If you see unsatisfied dependency errors, check that you have:

1. A `JobStore` implementation on the classpath (e.g., `ratchet-store-mysql` or `ratchet-store-postgresql`)
2. An `ExecutorProvider` bean (Ratchet provides `DefaultExecutorProvider`)
3. A `MetricsCollector` bean (Ratchet provides `NoOpMetricsCollector`)

### Bean Resolution Failures During Execution

```
SEVERE: Failed to resolve bean for instance method processData in class com.myapp.MyService
IllegalStateException: Cannot resolve bean for instance method processData in class
    com.myapp.MyService. Ensure the class is a managed bean or use a static method.
```

This means Ratchet tried to invoke an instance method but could not obtain the target bean from CDI. Ensure:

- The target class is a CDI managed bean (annotated with a scope like `@ApplicationScoped`)
- The class is in a bean archive (visible to CDI scanning)
- If using static methods, the lambda correctly captures a static method reference

### Ambiguous ClassPolicy

If you provide a custom `ClassPolicy` without the `@Alternative` and `@Priority` annotations, CDI will see two beans (yours and the default from `RatchetProducer`) and throw an `AmbiguousResolutionException`.

```java
// Correct way to override
@Alternative
@Priority(APPLICATION)
@ApplicationScoped
public class MyClassPolicy implements ClassPolicy { ... }
```
