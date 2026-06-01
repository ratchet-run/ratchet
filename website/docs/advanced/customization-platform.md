---
sidebar_position: 1
title: Customization Platform
description: The supported CDI extension points for configuration, execution, payload resolution, logging, polling, and lifecycle hooks
---

# Customization Platform

Ratchet customization is CDI-first. To override the reference implementation, provide an `@ApplicationScoped` bean for the SPI you want to replace and mark it as an enabled CDI alternative:

```java
import static jakarta.interceptor.Interceptor.Priority.APPLICATION;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;

@Alternative
@Priority(APPLICATION)
@ApplicationScoped
public class AppExecutionTuningProvider implements ExecutionTuningProvider {
    // ...
}
```

No string-based implementation class names are loaded by Ratchet. This keeps the customization model portable across Jakarta runtimes and testable with normal CDI tooling.

## Extension Points

| SPI | Default | Use it to customize |
|-----|---------|---------------------|
| `RatchetConfigSource` | Source-chain fallback behind `RatchetOptions` | Where raw fallback config comes from, such as database-backed config or a platform config service |
| `ExecutionTuningProvider` | `RatchetOptions`-backed thread pool and virtual-thread settings | Per-execution-type concurrency and virtual-thread backpressure limits |
| `PollingStrategyProvider` | Adaptive RI polling strategy | Poll cadence, backoff, burst behavior, or an externally coordinated polling policy |
| `JobInvocationResolver` | ASM-based lambda/method-reference analysis | How submitted callbacks become persisted target-method invocations |
| `ResultPersistenceStrategy` | JSON result metadata with a size cap | Return-value serialization, truncation, redaction, or disabling result persistence |
| `JobLoggerFactory` | JBoss Logging-backed per-job logger | Job-scoped structured logging and log event publishing |
| `CircuitBreakerConfigProvider` | `RatchetOptions`-backed built-in breaker settings | Circuit breaker enablement and per-profile thresholds |
| `ResilienceStrategy` | Built-in circuit breaker wrapper | Replace the built-in resilience layer entirely |
| `SchedulerLifecycleHook` | No hooks by default | Startup/shutdown integration with external systems |
| `ClassPolicy` | Empty allowlist, fail-fast at startup | Security boundary for job target classes |
| `ErrorSanitizer` | Common PII redaction | Domain-specific error redaction |
| `RetryPolicy` | Defers to job retry settings | Exception-aware retry decisions |
| `MetricsCollector` | No-op | Metrics backend integration |
| `BeanResolver` | CDI lookup | Custom target instance resolution |
| `ExecutorProvider` | Default managed executor provider | Executor ownership and scheduling executor selection |
| `NodeIdentityProvider` | Store-backed node identity and heartbeat | Cloud or orchestrator-specific node identity |
| `ClusterCoordinator` | No-op | Cross-node coordination beyond the store-backed wakeup path |
| `StartupCoordinator` | Store-backed startup lock | Startup leadership and one-time maintenance gates |

`LambdaAnalyzer` remains in the API, but the scheduler's primary payload path is `JobInvocationResolver`. Return values are handled by `ResultPersistenceStrategy`.

## Configuration Model

Applications must produce an `@ApplicationScoped RatchetOptions` bean — Ratchet refuses to start otherwise. See [Configuration](/getting-started/configuration) for the canonical patterns.

`RatchetConfigSource` is an advanced overlay: produce it only when your platform already owns raw configuration and you want a producer that calls `RatchetOptionsFactory.fromEnvironment(yourSource)` to fold that platform on top of the ambient `RATCHET_*` / MicroProfile Config chain.

## Deeper Customization

Use these hooks when changing scheduler behavior rather than just tuning values:

```java
@Alternative
@Priority(APPLICATION)
@ApplicationScoped
public class TenantAwareExecutionTuning implements ExecutionTuningProvider {
    @Override
    public RatchetOptions.ThreadingMode defaultThreadingMode() {
        return RatchetOptions.ThreadingMode.VIRTUAL;
    }

    @Override
    public int maxConcurrency(String executionTypeName, int defaultValue) {
        return switch (executionTypeName) {
            case "BATCH_CHILD" -> 80;
            case "DLQ_ALERT" -> 4;
            default -> defaultValue;
        };
    }

    @Override
    public int virtualThreadLimit(String executionTypeName, int defaultValue) {
        return "WORKFLOW_JOIN".equals(executionTypeName) ? 500 : defaultValue;
    }
}
```

For payload customization, implement `JobInvocationResolver`. The resolver must return a stable class name, method name, JVM method descriptor, static/instance flag, and argument list. This is the contract that is persisted in the job store.

For lifecycle integration, implement `SchedulerLifecycleHook`. Hook failures are logged and do not prevent Ratchet startup or shutdown; use them for integration and telemetry, not as a hard deployment gate.
