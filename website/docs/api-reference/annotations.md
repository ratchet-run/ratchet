---
sidebar_position: 10
title: Annotations Reference
description: Declarative configuration annotations for recurring jobs, circuit breakers, retry control, and API stability.
---

# Annotations Reference

Declarative configuration for Ratchet jobs. These annotations provide a zero-code-configuration approach to scheduling, resilience, and retry behavior.

## @Recurring

Marks a method for cron-scheduled recurring execution. Annotated methods are automatically discovered at application startup and registered with the job scheduler.

**Package:** `run.ratchet.api`
**Target:** Method
**Retention:** Runtime

```java
@Recurring(
    cron = "0 0 2 * * ?",
    zone = "America/New_York",
    id = "nightly-cleanup",
    name = "Nightly Cleanup",
    enabled = "true",
    priority = 5,
    maxRetries = 3,
    backoffPolicy = BackoffPolicy.EXPONENTIAL,
    backoffDelayMs = 1000,
    timeoutSeconds = 3600,
    tags = {"maintenance", "nightly"}
)
```

### Attributes

| Attribute | Type | Default | Required | Description |
|---|---|---|---|---|
| `cron` | `String` | -- | **Yes** | Quartz cron expression (6-7 fields) |
| `zone` | `String` | `"UTC"` | No | Timezone for cron evaluation. Must be a valid `ZoneId`. |
| `id` | `String` | `""` | No | Unique job ID. If empty, defaults to fully qualified class + method name. Used as business key. |
| `name` | `String` | `""` | No | Human-readable name for logs and monitoring. Defaults to method name. |
| `enabled` | `String` | `"true"` | No | Whether the job is enabled. Supports property placeholders (e.g., `"${app.cleanup.enabled:true}"`). |
| `priority` | `int` | `5` | No | Priority on a 1-10 scale. Mapped to `JobPriority` (see [mapping](./job-options#priority-in-recurring)). |
| `maxRetries` | `int` | `3` | No | Maximum retry attempts on failure. |
| `backoffPolicy` | `BackoffPolicy` | `EXPONENTIAL` | No | Retry delay strategy: `NONE`, `FIXED`, or `EXPONENTIAL`. |
| `backoffDelayMs` | `long` | `1000` | No | Base delay in milliseconds for backoff calculations. |
| `timeoutSeconds` | `long` | `3600` | No | Maximum execution time in seconds (default: 1 hour). |
| `tags` | `String[]` | `{}` | No | Tags for filtering and categorization. |

### Cron Expression Format

Uses Quartz cron format with 6-7 fields:

```
second minute hour day-of-month month day-of-week [year]
```

| Expression | Schedule |
|---|---|
| `0 0 2 * * ?` | Every day at 2:00 AM |
| `0 */15 * * * ?` | Every 15 minutes |
| `0 0 9 ? * MON` | Every Monday at 9:00 AM |
| `0 0 0 1 * ?` | First day of every month at midnight |
| `0 30 8 ? * MON-FRI` | Weekdays at 8:30 AM |

### Method Requirements

- Must be `public`
- Must have **no parameters** or a single [`JobContext`](./job-context) parameter
- Must be in a CDI-managed bean (e.g., `@ApplicationScoped`)
- Return type is ignored

### Examples

```java
@ApplicationScoped
public class MaintenanceService {

    // Simple recurring job -- runs at 2 AM daily
    @Recurring(cron = "0 0 2 * * ?", name = "Nightly Cleanup")
    public void performCleanup() {
        // cleanup logic
    }

    // With JobContext for logging and parameters
    @Recurring(
        cron = "0 */30 * * * ?",
        zone = "America/New_York",
        name = "Data Sync"
    )
    public void syncData(JobContext ctx) {
        ctx.logger().info("Starting sync for job " + ctx.jobId());
        // sync logic
    }

    // High-priority with custom retry policy
    @Recurring(
        cron = "0 0 * * * ?",
        name = "Hourly Health Check",
        priority = 9,
        maxRetries = 5,
        backoffPolicy = BackoffPolicy.EXPONENTIAL,
        backoffDelayMs = 2000,
        tags = {"health", "monitoring"}
    )
    public void healthCheck() {
        // health check logic
    }

    // Conditionally enabled via configuration property
    @Recurring(
        cron = "0 0 3 * * ?",
        name = "Archive Old Records",
        enabled = "${app.archiving.enabled:false}"
    )
    public void archiveRecords() {
        // archiving logic
    }
}
```

### Lifecycle

1. At application startup, Ratchet scans CDI beans for `@Recurring` methods.
2. For each annotated method, a recurring job definition is registered using the `id` as the business key.
3. The scheduler creates individual job instances at each scheduled execution time.
4. If a job with the same business key is already active (PENDING/RUNNING), it is replaced.

## @CircuitBreakerProtected

Wraps method invocations through the configured [`ResilienceStrategy`](./spi-interfaces#resiliencestrategy) for circuit breaker protection. When the circuit opens, calls fail fast with `CircuitBreakerOpenException`.

**Package:** `run.ratchet.api`
**Target:** Type, Method
**Retention:** Runtime
**Meta-annotations:** `@InterceptorBinding`, `@Inherited`

:::info
This annotation is marked `@Incubating` and may change in future versions.
:::

```java
@CircuitBreakerProtected(
    service = "payment-gateway",
    profile = CircuitBreakerProfile.EXTERNAL_API
)
```

### Attributes

| Attribute | Type | Default | Required | Description |
|---|---|---|---|---|
| `service` | `String` | `""` | **Yes** | Service name for grouping circuit breakers. Used in logging and monitoring. |
| `profile` | `CircuitBreakerProfile` | `DEFAULT` | No | Pre-configured circuit breaker profile. |

### CircuitBreakerProfile

Pre-configured profiles for common use cases:

| Profile | Use Case | Failure Threshold | Sliding Window | Wait Duration |
|---|---|---|---|---|
| `DEFAULT` | General internal services | 50% | 100 calls | 60 seconds |
| `FAST` | Quick failure detection | 50% | 20 calls | 10 seconds |
| `CRITICAL` | High-availability services | 75% | 200 calls | 60 seconds |
| `EXTERNAL_API` | Third-party integrations | 60% | 50 calls | 60 seconds |

### Examples

```java
@ApplicationScoped
public class PaymentGateway {

    // Method-level circuit breaker
    @CircuitBreakerProtected(
        service = "stripe-api",
        profile = CircuitBreakerProfile.EXTERNAL_API
    )
    public PaymentResult charge(PaymentRequest req) {
        return stripeClient.charge(req);
    }
}

// Class-level circuit breaker (applies to all methods)
@CircuitBreakerProtected(service = "inventory-service")
@ApplicationScoped
public class InventoryClient {
    public int checkStock(String sku) { /* ... */ }
    public void reserveStock(String sku, int qty) { /* ... */ }
}
```

When the circuit breaker opens, calls throw `CircuitBreakerOpenException` immediately without invoking the protected method.

## @DoNotRetry

Marks an **exception class** as permanently non-retryable. When a job fails with an exception annotated with `@DoNotRetry`, the scheduler skips retry attempts and moves the job directly to the dead letter queue.

**Package:** `run.ratchet.api`
**Target:** Type (applied to exception classes)
**Retention:** Runtime

```java
@DoNotRetry("Validation errors should not be retried")
public class ValidationException extends Exception {
    // Exception marked as non-retryable
}
```

### Attributes

| Attribute | Type | Default | Description |
|---|---|---|---|
| `value` | `String` | `""` | Optional human-readable reason why retries should be skipped |

### Example

```java
// Define a non-retryable exception
@DoNotRetry("Business validation failures are permanent")
public class ValidationException extends RuntimeException {
    public ValidationException(String message) {
        super(message);
    }
}

// When this exception is thrown, the job goes straight to DLQ
@ApplicationScoped
public class OrderProcessor {
    public void processOrder(long orderId) {
        Order order = orderRepository.find(orderId);
        if (order.total().compareTo(BigDecimal.ZERO) < 0) {
            throw new ValidationException("Order total cannot be negative");
        }
        // process...
    }
}
```

Use `@DoNotRetry` for exceptions that represent permanent failures:
- Business rule violations
- Data validation errors
- Authorization failures
- Malformed input

Do **not** use it for transient failures that might succeed on retry (network timeouts, database lock contention, etc.).

## @Incubating

Marks APIs that are experimental and may change in future versions without following the normal deprecation cycle.

**Package:** `run.ratchet.api`
**Target:** Any element type
**Retention:** Runtime

```java
@Incubating
public interface ResilienceStrategy {
    // This SPI may change in a future version
}
```

`@Incubating` is applied to SPI interfaces and classes that are subject to change. If you implement an `@Incubating` interface, be aware that method signatures may be added, changed, or removed in minor releases.

Currently `@Incubating` SPIs include: `ResilienceStrategy`, `ClassPolicy`, `ErrorSanitizer`, `ExecutorProvider`, `BeanResolver`, `MetricsCollector`, `JobLogger`, `ClusterCoordinator`, `StartupCoordinator`, `NodeIdentityProvider`, `LambdaAnalyzer`, and `CircuitBreakerProtected`.

## See Also

- [Job Options and Enums](./job-options)
- [SPI Interfaces](./spi-interfaces)
- [JobContext Reference](./job-context)
