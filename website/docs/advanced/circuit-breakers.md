---
sidebar_position: 1
title: Circuit Breakers
description: Built-in circuit breaker pattern for protecting job execution against cascading failures
---

# Circuit Breakers

Ratchet includes a built-in circuit breaker implementation that protects job execution against cascading failures. The circuit breaker monitors failure rates across a sliding window of recent calls and automatically halts execution when a service becomes unreliable, giving it time to recover.

No external dependencies like Resilience4j are required -- the circuit breaker ships with the reference implementation and integrates directly with CDI.

## How Circuit Breakers Work

Ratchet's circuit breaker follows the standard three-state model:

```
CLOSED (normal operation)
  |
  |  failure rate >= threshold AND calls >= minimumCalls
  v
OPEN (all calls rejected immediately)
  |
  |  waitDuration expires
  v
HALF_OPEN (limited trial calls allowed)
  |                         |
  |  all trial calls pass   |  any trial call fails
  v                         v
CLOSED                    OPEN
```

**CLOSED** -- The circuit is healthy. All calls pass through normally. Success and failure outcomes are tracked in a sliding window (ring buffer of the last N calls). When the failure rate exceeds the configured threshold and the minimum call count has been reached, the circuit transitions to OPEN.

**OPEN** -- The circuit has tripped. All calls are rejected immediately with a `ServiceUnavailableException`, avoiding wasted work against a failing service. After the configured wait duration, the circuit transitions to HALF_OPEN.

**HALF_OPEN** -- The circuit is testing recovery. A limited number of trial calls are permitted. If all trial calls succeed, the circuit resets to CLOSED. If any trial call fails, the circuit returns to OPEN.

## Using @CircuitBreakerProtected

The simplest way to apply circuit breaker protection is the `@CircuitBreakerProtected` annotation. It works as a CDI interceptor binding -- place it on a method or class, and the `CircuitBreakerInterceptor` wraps each invocation in circuit breaker logic.

### Method-Level Protection

```java
import run.ratchet.api.CircuitBreakerProtected;
import run.ratchet.api.CircuitBreakerProfile;

@ApplicationScoped
public class PaymentService {

    @CircuitBreakerProtected(
        service = "payment-gateway",
        profile = CircuitBreakerProfile.EXTERNAL_API
    )
    public PaymentResult processPayment(PaymentRequest request) {
        return gateway.charge(request);
    }
}
```

### Class-Level Protection

When placed on a class, all public methods are protected by the same circuit breaker:

```java
@CircuitBreakerProtected(service = "inventory-api", profile = CircuitBreakerProfile.DEFAULT)
@ApplicationScoped
public class InventoryClient {

    public int checkStock(String sku) {
        return api.getStockLevel(sku);
    }

    public void reserveStock(String sku, int quantity) {
        api.reserve(sku, quantity);
    }
}
```

Method-level annotations take precedence over class-level annotations, allowing you to use a different profile for specific methods.

### Annotation Attributes

| Attribute | Type | Default | Description |
|-----------|------|---------|-------------|
| `service` | `String` | `""` (auto-derived) | Service name for the circuit breaker. If empty, defaults to `ClassName.methodName`. Must come from a bounded vocabulary -- do not use dynamic values like tenant IDs. |
| `profile` | `CircuitBreakerProfile` | `DEFAULT` | Pre-configured circuit breaker profile controlling thresholds and timing. |

### Service Name Best Practices

The service name identifies which circuit breaker instance is used. Calls with the same service name share the same breaker, which means failures in one method can trip the circuit for all methods targeting that service:

```java
// Both methods share the "email-provider" circuit breaker
@CircuitBreakerProtected(service = "email-provider")
public void sendWelcomeEmail(String to) { ... }

@CircuitBreakerProtected(service = "email-provider")
public void sendPasswordReset(String to) { ... }
```

**Warning:** Service names must come from a bounded, static vocabulary (class names, annotation values, known service identifiers). Using dynamic names like tenant IDs or user IDs will cause unbounded memory growth in the `CircuitBreakerRegistry`.

## CircuitBreakerProfile

Ratchet ships with five pre-configured profiles, each tuned for a different use case:

### DEFAULT

General-purpose profile for internal services.

| Parameter | Value |
|-----------|-------|
| Failure rate threshold | 50% |
| Sliding window size | 100 calls |
| Wait duration (OPEN) | 30 seconds |
| Slow call threshold | 10 seconds |
| Permitted calls in HALF_OPEN | 3 |
| Minimum calls before evaluation | 5 |

### FAST

Aggressive failure detection for latency-sensitive paths.

| Parameter | Value |
|-----------|-------|
| Failure rate threshold | 50% |
| Sliding window size | 20 calls |
| Wait duration (OPEN) | 10 seconds |
| Slow call threshold | 2 seconds |
| Permitted calls in HALF_OPEN | 2 |
| Minimum calls before evaluation | 3 |

### CRITICAL

Conservative profile for high-availability services where false positives are costly.

| Parameter | Value |
|-----------|-------|
| Failure rate threshold | 75% |
| Sliding window size | 200 calls |
| Wait duration (OPEN) | 60 seconds |
| Slow call threshold | 30 seconds |
| Permitted calls in HALF_OPEN | 5 |
| Minimum calls before evaluation | 10 |

### EXTERNAL_API

Tuned for third-party integrations with higher latency tolerance and longer recovery windows.

| Parameter | Value |
|-----------|-------|
| Failure rate threshold | 60% |
| Sliding window size | 50 calls |
| Wait duration (OPEN) | 60 seconds |
| Slow call threshold | 5 seconds |
| Permitted calls in HALF_OPEN | 3 |
| Minimum calls before evaluation | 5 |

### CLAIM_PATH

Internal profile used by the poller when claiming work from the store. It trips quickly, recovers quickly, and uses a single half-open probe so a sick database does not keep every poll tick busy-looping on transient failures.

| Parameter | Value |
|-----------|-------|
| Failure rate threshold | 50% |
| Sliding window size | 20 calls |
| Wait duration (OPEN) | 5 seconds |
| Slow call threshold | 2 seconds |
| Permitted calls in HALF_OPEN | 1 |
| Minimum calls before evaluation | 5 |

## Profile Overrides via Configuration

Profile defaults can be overridden per deployment with `RatchetOptions`:

```java
RatchetOptions.builder()
    .circuitBreaker(cb -> cb
        .profile(CircuitBreakerProfile.DEFAULT, p -> p
            .failureRateThreshold(60)
            .slidingWindowSize(50)
            .waitDurationMs(20_000)
            .slowCallThresholdMs(5_000)
            .permittedCallsInHalfOpen(5)
            .minimumCalls(10))
        .profile(CircuitBreakerProfile.EXTERNAL_API, p -> p
            .failureRateThreshold(40)
            .waitDurationMs(120_000))
        .profile(CircuitBreakerProfile.CLAIM_PATH, p -> p
            .waitDurationMs(10_000)))
    .build();
```

If no `RatchetOptions` bean exists, the fallback source chain can read canonical `ratchet.circuit-breaker.<profile>.*` properties and `RATCHET_CB_<PROFILE>_...` environment variables.

Set `RatchetOptions.builder().circuitBreaker(cb -> cb.enabled(false))` to make both the scheduler resilience wrapper and the `@CircuitBreakerProtected` interceptor pass through without consulting circuit state.

## Programmatic Access via CircuitBreakerRegistry

For cases where annotation-based protection is insufficient, inject the `CircuitBreakerRegistry` directly:

```java
import run.ratchet.ri.resilience.CircuitBreakerRegistry;
import run.ratchet.ri.resilience.CircuitBreaker;
import run.ratchet.api.CircuitBreakerProfile;

@ApplicationScoped
public class AdaptiveJobService {

    @Inject
    private CircuitBreakerRegistry registry;

    public void executeWithProtection(String serviceName, Runnable work) {
        CircuitBreaker breaker = registry.getBreaker(serviceName, CircuitBreakerProfile.DEFAULT);

        try {
            breaker.execute(() -> {
                work.run();
                return null;
            });
        } catch (ServiceUnavailableException e) {
            // Circuit is OPEN -- queue for later or use fallback
            log.warning("Service " + serviceName + " unavailable, queuing for retry");
        }
    }
}
```

### Registry Operations

```java
// Get or create a breaker with default profile
CircuitBreaker breaker = registry.getBreaker("my-service");

// Get or create with a specific profile
CircuitBreaker breaker = registry.getBreaker("my-service", CircuitBreakerProfile.FAST);

// Check current state (returns null if breaker not yet created)
CircuitBreaker.State state = registry.getBreakerState("my-service");

// Manually open a circuit (e.g., during maintenance)
registry.openBreaker("my-service");

// Reset a circuit to CLOSED
registry.resetBreaker("my-service");

// Register a custom configuration
registry.registerConfig("custom", new CircuitBreakerConfiguration(
    40.0f,   // failureRateThreshold
    30,      // slidingWindowSize
    45_000L, // waitDurationMs
    8_000L,  // slowCallThresholdMs
    4,       // permittedCallsInHalfOpen
    8        // minimumCalls
));
```

## Implementing a Custom ResilienceStrategy

The `ResilienceStrategy` SPI allows you to replace the built-in circuit breaker with an external library like Resilience4j or MicroProfile Fault Tolerance.

### SPI Interface

```java
package run.ratchet.spi;

@Incubating
public interface ResilienceStrategy {
    <T> T execute(String serviceName, Callable<T> task) throws Exception;
    boolean isServiceAvailable(String serviceName);
    default Duration getRetryDelay(String serviceName) {
        return Duration.ofSeconds(30);
    }
}
```

### Resilience4j Example

```java
import run.ratchet.spi.ResilienceStrategy;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.interceptor.Interceptor;

@Alternative
@Priority(Interceptor.Priority.APPLICATION)
@ApplicationScoped
public class Resilience4jStrategy implements ResilienceStrategy {

    private final CircuitBreakerRegistry registry;

    @Inject
    public Resilience4jStrategy(CircuitBreakerRegistry registry) {
        this.registry = registry;
    }

    @Override
    public <T> T execute(String serviceName, Callable<T> task) throws Exception {
        CircuitBreaker breaker = registry.circuitBreaker(serviceName);
        return breaker.executeCallable(task);
    }

    @Override
    public boolean isServiceAvailable(String serviceName) {
        CircuitBreaker breaker = registry.circuitBreaker(serviceName);
        return breaker.getState() != CircuitBreaker.State.OPEN;
    }

    @Override
    public Duration getRetryDelay(String serviceName) {
        CircuitBreaker breaker = registry.circuitBreaker(serviceName);
        long waitMs = breaker.getCircuitBreakerConfig()
            .getWaitIntervalFunctionInOpenState()
            .apply(1);
        return Duration.ofMillis(waitMs);
    }
}
```

## Handling ServiceUnavailableException

When a circuit breaker is OPEN, calls throw `ServiceUnavailableException`. The Ratchet job engine handles this automatically by deferring the job and respecting the `getRetryDelay()` value from the `ResilienceStrategy`. For direct programmatic use, you should catch and handle this exception:

```java
import run.ratchet.ri.resilience.ServiceUnavailableException;

try {
    breaker.execute(() -> externalApi.call());
} catch (ServiceUnavailableException e) {
    // Service is down -- use a fallback or queue for later
    return cachedResult;
}
```

## Best Practices

**Choose the right profile.** Use `EXTERNAL_API` for third-party services with unpredictable latency. Use `FAST` for internal services where you want quick failure detection. Use `CRITICAL` for services where false positives (unnecessarily tripping the circuit) are more costly than a few extra failures.

**Use bounded service names.** Always use static, bounded service names (class names, known endpoint identifiers). Dynamic names like `"tenant-" + tenantId` create a new circuit breaker per tenant, leading to memory leaks.

**Share breakers across related operations.** If multiple methods call the same downstream service, use the same `service` name so failures in any method contribute to tripping the circuit. This prevents sending traffic to a service that is already known to be failing.

**Monitor state transitions.** Inject `CircuitBreakerRegistry` and periodically check breaker states for operational visibility. Consider exposing breaker state via a health check endpoint.

**Prefer manual reset sparingly.** The `resetBreaker()` method forces a circuit back to CLOSED. Use this only for operational recovery (e.g., after confirming a downstream service is restored), not as part of normal application flow.
