---
sidebar_position: 1
title: Introduction
description: What is Ratchet, why use it, and how it compares to other job schedulers in the Jakarta EE ecosystem
---

# Introduction

## What is Ratchet?

Ratchet is a portable, CDI-based job scheduler built for Jakarta EE applications. It gives you a clean, annotation-driven API for background job scheduling with persistent storage, automatic retries, workflow orchestration, and built-in resilience -- all without pulling in heavyweight frameworks or non-standard dependencies.

If you've ever needed to run a task in the background, retry it on failure, chain it with other tasks, or schedule it on a cron timer inside a Jakarta EE application, Ratchet is built for exactly that.

```java
@Inject
JobSchedulerService scheduler;

public void onNewOrder(Order order) {
    scheduler.enqueueNow(() -> processOrder(order.getId()));
}
```

That's it. One injection, one method call. The job is persisted to your database, picked up by a poller, executed with retry semantics, and monitored through a rich event system. No XML configuration, no external processes, no message broker required.

## Why Ratchet?

Enterprise Java applications need background job processing, but the existing options force tradeoffs that Ratchet was designed to avoid.

### The problem with existing solutions

**Jakarta Batch (JSR 352) / jBeret** is the standard, but it was designed for large-scale batch processing -- think ETL jobs and report generation. Its programming model (readers, processors, writers, job XML) is heavyweight for the common case of "run this method in the background and retry if it fails." It also has no built-in concept of job chaining, conditional workflows, or circuit breaker protection.

**Quartz Scheduler** is widely used, but it predates CDI and Jakarta EE 10. Its `Job` interface requires a separate class per job, its trigger model is complex, and its persistence layer uses its own schema and connection management rather than integrating with your application's existing data source. Adding Quartz to a modern Jakarta EE application means managing two persistence layers and wiring CDI manually.

**MicroProfile Fault Tolerance** handles retries and circuit breakers well, but it's a resilience library, not a job scheduler. It has no concept of persistent jobs, deferred execution, or cron scheduling.

### What Ratchet does differently

Ratchet was designed from the ground up for the common case: **schedule a method call, persist it, retry it, and observe it** -- all within the CDI programming model you already use.

| Design decision | Why it matters |
|----------------|----------------|
| **Lambda-based API** | No `Job` interface to implement. Pass a method reference and you're done. |
| **CDI-native** | Inject `JobSchedulerService` like any other bean. No special wiring. |
| **Pluggable store SPI** | Use MySQL, PostgreSQL, or MongoDB. Or implement the TCK and bring your own. |
| **Built-in resilience** | Retries, backoff, circuit breakers, and dead letter queues are first-class. |
| **Schema as DDL** | Plain SQL files. No Flyway dependency, no migration lock-in. Apply the schema however your project manages DDL. |
| **Workflow primitives** | Chain jobs, branch on results, and compose multi-step workflows without an external orchestrator. |

## Feature Overview

### Scheduling

Schedule jobs for immediate execution, after a delay, or on a cron schedule. Jobs are persisted to your database before returning, so nothing is lost if the server restarts between submission and execution.

```java
// Immediate
scheduler.enqueueNow(() -> sendEmail(userId));

// Delayed
scheduler.schedule(Duration.ofMinutes(30), () -> sendReminder(orderId))
    .submit();

// Recurring (annotation-driven)
@Recurring(cron = "0 0 2 * * ?", name = "Nightly Cleanup")
public void cleanupExpiredSessions() { /* ... */ }
```

### Workflows and Job Chaining

Build multi-step workflows with conditional branching. Each step is a separate persisted job, so failures in step 3 don't lose the results of steps 1 and 2.

```java
scheduler.enqueue(() -> validatePayment(orderId))
    .thenOnSuccess(() -> fulfillOrder(orderId))
    .thenOnFailure(() -> notifyPaymentFailure(orderId))
    .submit();
```

### Resilience

Every job supports configurable retries with backoff (fixed or exponential), a built-in circuit breaker for protecting external services, and a dead letter queue for jobs that exhaust their retries.

```java
scheduler.enqueue(() -> callPaymentGateway(paymentId))
    .withMaxRetries(3)
    .withBackoff(BackoffPolicy.EXPONENTIAL, Duration.ofSeconds(2))
    .submit();
```

### Batch Processing

Process collections in parallel with progress tracking, or stream large datasets through a chunked pipeline without loading everything into memory.

```java
scheduler.enqueueBatch("process-invoices")
    .forEach(invoices, invoice -> processInvoice(invoice))
    .thenOnBatchSuccess(() -> sendCompletionReport())
    .submit();
```

### Observability

Monitor job lifecycle through CDI events or programmatic listeners. Every state transition fires an event you can observe.

```java
public void onJobFailed(@Observes JobFailedEvent event) {
    alerting.notify("Job " + event.getJobId() + " failed: " + event.getErrorMessage());
}
```

### Job Control

Manage jobs at runtime: pause, resume, cancel, and manually retry failed jobs. Every operation is CAS-based for safe concurrent access.

```java
JobHandle handle = scheduler.enqueue(() -> longRunningTask())
    .withTimeout(Duration.ofMinutes(30))
    .withBusinessKey("quarterly-import")
    .submit();

long jobId = handle.id();

scheduler.pauseJob(jobId);     // Prevent execution
scheduler.resumeJob(jobId);    // Return the job to its pre-pause state
scheduler.cancelJob(jobId);    // Cancel permanently
scheduler.retryJob(jobId);     // Reset a failed job to PENDING
```

### Extensible SPI

Ratchet's internals are composed of focused SPI interfaces. The reference implementation provides defaults for all of them, but you can replace any default by providing a CDI `@Alternative @Priority(APPLICATION)` bean:

| SPI Interface | What you can customize |
|---------------|----------------------|
| `RetryPolicy` | Custom retry/no-retry decisions |
| `ResilienceStrategy` | Plug in Resilience4j or MicroProfile Fault Tolerance |
| `ClassPolicy` | Security policy for payload deserialization |
| `ErrorSanitizer` | Custom PII/credential redaction |
| `MetricsCollector` | Export metrics to your preferred backend |
| `ClusterCoordinator` | Cross-node wakeups when you supply an implementation |
| `StartupCoordinator` | Store-backed startup leases for destructive initialization tasks |
| `ExecutorProvider` | Virtual threads or custom thread pools |

## Target Audience

Ratchet is built for **Jakarta EE developers** building applications on servers like WildFly, Open Liberty, Payara, or any Jakarta EE 10-compatible runtime. You should be comfortable with:

- CDI injection and bean scoping
- JPA / relational databases
- Maven dependency management

If you're building a microservice on Spring Boot or Quarkus and not targeting a full Jakarta EE server, Ratchet may still work with explicit CDI wiring and the standalone executor fallback, but the reference implementation is primarily designed and tested against Jakarta EE runtimes with managed executors.

## Requirements

| Component | Version |
|-----------|---------|
| **Java** | 17+ |
| **Jakarta EE** | 10 -- CDI 4.0, JPA 3.1, Interceptors 2.1, Jakarta Concurrency 3.0 |
| **Runtime** | Jakarta EE 10 compatible server with managed executor support (WildFly, Open Liberty, Payara) |
| **Database** | MySQL 8+, PostgreSQL 14+, or MongoDB 6+ |

## Project Status

Ratchet is currently at version **0.1.0-SNAPSHOT**. The core API is stabilizing, but interfaces marked with `@Incubating` (such as `CircuitBreakerProtected` and `CircuitBreakerProfile`) may change in future releases. Feedback and contributions are welcome.

## What's Next

- [Installation](./installation.md) -- Add Ratchet to your Maven project
- [Quick Start](./quickstart.md) -- Run your first job in under 5 minutes
- [Your First Job](./first-job.md) -- Build a complete job with retries, callbacks, and monitoring
- [Configuration](./configuration.md) -- Tune the CDI wiring and runtime behavior
