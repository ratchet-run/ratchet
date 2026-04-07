---
sidebar_position: 1
title: Architecture Overview
description: High-level architecture of Ratchet and how it fits into a Jakarta EE application
---

# Architecture Overview

Ratchet is a portable, CDI-based job scheduler for Jakarta EE applications. It provides persistent, cluster-safe background job scheduling with a fluent API -- covering batching, chaining, workflows, and transactional enqueueing out of the box.

## Where Ratchet Fits

In a typical Jakarta EE application, Ratchet sits between your business logic and the database. You inject `JobSchedulerService`, enqueue work using lambda expressions, and Ratchet handles persistence, polling, execution, retries, and lifecycle events.

```
┌─────────────────────────────────────────────────────────┐
│                    Your Application                     │
│                                                         │
│   @Inject JobSchedulerService scheduler;                │
│   scheduler.enqueue(() -> orderService.process(id))     │
│       .withRetries(3)                                   │
│       .submit();                                        │
│                                                         │
├─────────────────────────────────────────────────────────┤
│                     Ratchet Engine                       │
│                                                         │
│  ┌──────────┐  ┌────────┐  ┌──────────┐  ┌──────────┐  │
│  │ Poller   │  │JobTask │  │ Workflow │  │  Batch   │  │
│  │          │──│        │──│Scheduler │──│ Service  │  │
│  │ Adaptive │  │Executor│  │          │  │          │  │
│  │ Polling  │  │        │  │ Chains + │  │ Parent/  │  │
│  │          │  │ Retry  │  │Conditions│  │ Child    │  │
│  └──────────┘  └────────┘  └──────────┘  └──────────┘  │
│                                                         │
├─────────────────────────────────────────────────────────┤
│                    JobStore SPI                          │
│           (15 sub-interfaces, one marker)                │
├─────────────────────────────────────────────────────────┤
│        MySQL Store    │     PostgreSQL Store             │
│     (SKIP LOCKED)     │      (SKIP LOCKED)              │
└───────────────────────┴─────────────────────────────────┘
```

## Module Structure

Ratchet is organized into modules following the Jakarta EE API / RI / TCK pattern:

```
ratchet/
├── ratchet-api          Zero-dependency public API, SPIs, events, annotations
├── ratchet           Reference implementation + CDI integration
├── ratchet-store-core   Shared JPA entities (internal, not user-facing)
├── ratchet-store-mysql  MySQL JobStore + DDL
├── ratchet-store-postgresql  PostgreSQL JobStore + DDL
├── ratchet-tck          Technology Compatibility Kit
├── ratchet-testsuite    Integration tests
└── ratchet-bom          Bill of Materials POM
```

### Module Dependency Graph

```
                 ┌──────────────┐
                 │  ratchet-api │  (zero dependencies)
                 └──────┬───────┘
                        │
          ┌─────────────┼──────────────┐
          │             │              │
          ▼             ▼              ▼
   ┌─────────────┐ ┌──────────┐  ┌──────────┐
   │ ratchet  │ │store-core│  │   tck    │
   │ (engine +   │ │(internal)│  │          │
   │  CDI)       │ └────┬─────┘  └──────────┘
   └─────────────┘      │
                   ┌────┴────┐
                   │         │
                   ▼         ▼
            ┌──────────┐ ┌──────────────┐
            │store-mysql│ │store-postgres│
            └──────────┘ └──────────────┘
```

**Key design constraint:** `ratchet-api` has zero runtime dependencies beyond Jakarta EE APIs. Your application can depend on `ratchet-api` for event types and annotations without pulling in the engine.

## Core Concepts

### Pull-Based Architecture

Ratchet uses a **pull model** where worker threads poll the database for available jobs. This provides natural backpressure -- workers only claim new jobs when they have capacity. The [Poller](./execution-model.md) uses adaptive algorithms to balance responsiveness against database load.

### Database as the Queue

Unlike message-broker-based schedulers, Ratchet uses your existing relational database as the job queue. Jobs are stored as rows in the `scheduler_job` table and claimed atomically using `SELECT ... FOR UPDATE SKIP LOCKED`. This gives you:

- **Transactional enqueueing** -- job creation participates in your existing transaction
- **Durability** -- jobs survive application restarts
- **Visibility** -- query job status with standard SQL
- **No additional infrastructure** -- no Redis, RabbitMQ, or Kafka required

### Lambda-Based API

Jobs are defined as serializable lambda expressions. Ratchet uses ASM bytecode analysis to extract the target class, method, and arguments from the lambda, then persists this information as a portable payload. At execution time, the method is invoked reflectively on a CDI-managed bean:

```java
// This lambda is analyzed at enqueue time, not executed
scheduler.enqueue(() -> orderService.processOrder(orderId))
    .submit();

// Ratchet extracts: target=OrderService, method=processOrder, args=[orderId]
// At execution time: CDI resolves OrderService, invokes processOrder(orderId)
```

### SPI-Driven Extension

Ratchet separates API contracts from implementation through Service Provider Interfaces. The engine consults SPIs for persistence, serialization, retry logic, security, metrics, and cluster coordination. Default implementations are provided, and you can replace any of them:

| SPI | Purpose | Default |
|-----|---------|---------|
| `JobStore` | Persistence (15 sub-interfaces) | MySQL / PostgreSQL modules |
| `SerializationStrategy` | Payload serialization | JDK serialization |
| `RetryPolicy` | Custom retry decisions | Passthrough (uses job config) |
| `ClassPolicy` | Security allowlist | Package-prefix matching |
| `MetricsCollector` | Observability hooks | No-op |
| `ClusterCoordinator` | Cross-node wakeups | No-op (single-node) |

SPIs marked `@Incubating` may change before 1.0:

| SPI | Purpose | Default |
|-----|---------|---------|
| `LambdaAnalyzer` | Lambda bytecode analysis | ASM-based analyzer |
| `NodeIdentityProvider` | Cluster node identity | Hostname + PID |
| `ExecutorProvider` | Thread pool management | Container-managed executors |
| `JobLogger` | Structured job logging | JUL-based logger |
| `ErrorSanitizer` | Exception message sanitization | Truncate + strip PII |

### Event System

Ratchet publishes lifecycle events that your application can observe. Events live in `ratchet-api` so you can depend on event types without pulling in the engine. In CDI environments, use standard `@Observes`:

```java
public void onJobFailed(@Observes JobFailedEvent event) {
    alertService.notify(event.jobId(), event.errorMessage());
}
```

In non-CDI environments, register a programmatic listener:

```java
scheduler.addEventListener(event -> {
    if (event instanceof JobDlqEvent dlq) {
        log.severe("Job " + dlq.jobId() + " moved to DLQ");
    }
});
```

## Minimal Setup

Add Ratchet to your Jakarta EE application:

```xml
<dependencyManagement>
  <dependencies>
    <dependency>
      <groupId>run.ratchet</groupId>
      <artifactId>ratchet-bom</artifactId>
      <version>${ratchet.version}</version>
      <type>pom</type>
      <scope>import</scope>
    </dependency>
  </dependencies>
</dependencyManagement>

<dependencies>
  <dependency>
    <groupId>run.ratchet</groupId>
    <artifactId>ratchet</artifactId>
  </dependency>
  <dependency>
    <groupId>run.ratchet</groupId>
    <artifactId>ratchet-store-mysql</artifactId>
    <!-- or ratchet-store-postgresql -->
  </dependency>
</dependencies>
```

Apply the DDL schema from the store module's `ddl/` directory, then inject and use:

```java
@Inject JobSchedulerService scheduler;

// Fire-and-forget
scheduler.enqueueNow(() -> emailService.sendWelcome(userId));

// Configured job
scheduler.enqueue(() -> reportService.generate(month))
    .withPriority(JobPriority.HIGH)
    .withTimeout(Duration.ofMinutes(30))
    .withMaxRetries(3)
    .withBackoff(BackoffPolicy.EXPONENTIAL, Duration.ofSeconds(10))
    .withTags("reports", "monthly")
    .submit();

// Recurring job via annotation
@Recurring(cron = "0 0 2 * * ?", name = "Nightly Cleanup")
public void cleanup() { ... }
```

## What's Next

- [Job Lifecycle](./job-lifecycle.md) -- Understand the complete state machine
- [Job Types](./job-types.md) -- SINGLE, BATCH, CHAIN, WORKFLOW, RECURRING, and SYSTEM
- [Scheduling](./scheduling.md) -- Immediate, delayed, and cron-based scheduling
- [Execution Model](./execution-model.md) -- How jobs are polled, claimed, and executed
- [Error Handling](./error-handling.md) -- Retries, DLQ, and `@DoNotRetry`
- [Persistence](./persistence.md) -- Entity model and JobStore SPI
- [Clustering](./clustering.md) -- Multi-node coordination
