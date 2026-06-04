---
sidebar_position: 1
title: API Reference Overview
description: Index of Ratchet API classes, package structure, and how to navigate the reference documentation.
---

# API Reference Overview

Ratchet's public API is organized into two packages: `run.ratchet.api` for the user-facing API, and `run.ratchet.spi` for extension points. This page provides a complete index and explains how the pieces fit together.

## Package Structure

### `run.ratchet.api` — User-Facing API

Classes and interfaces you use directly when scheduling jobs, building workflows, and observing events.

| Class / Interface | Kind | Purpose |
|---|---|---|
| [`JobSchedulerService`](./job-scheduler-service) | Interface | Primary entry point for scheduling, canceling, pausing, and retrying jobs |
| [`JobQueryService`](./job-query-service) | Interface | Read-only dashboards/admin API for job lists, detail views, history, and queue health |
| [`JobBuilder`](./job-builder) | Interface | Fluent builder for configuring individual jobs before submission |
| [`RecurringJobBuilder`](./job-scheduler-service#schedulerecurring) | Interface | Builder for configuring recurring (cron-based) jobs |
| [`BatchBuilder`](./batch-builder) | Interface | Builder for in-memory batch processing of collections |
| [`StreamingBatchBuilder`](./batch-builder#streamingbatchbuilder) | Interface | Builder for memory-efficient streaming batch processing |
| [`JobContext`](./job-context) | Final class | Thread-local access to job ID, job-scoped logger binding, and parameters during execution |
| `SignalDecision` | Record | Structured approval/rejection payload for signal-waiting jobs |
| [`JobResult<T>`](./job-result) | Class | Captures execution outcome: success/failure, return value, timing, metadata |
| [`JobHandle`](./job-scheduler-service#jobhandle) | Interface | Lightweight receipt returned after job submission, provides the job ID |
| [`JobOptions`](./job-options) | Record | Immutable configuration for priority, retries, backoff, and timeout |
| `JobFilter` | Record | Immutable query criteria for list/search operations |
| `JobPage<T>` | Record | Page metadata and cursor returned by query operations |
| `JobSummary` / `JobDetail` | Records | Lightweight and full read-only job projections |
| [`WorkflowCondition`](./workflow-condition) | Record | Defines conditions for workflow branching (success, failure, custom, batch) |
| [`WorkflowBranch`](./workflow-condition#workflowbranch) | Record | Pairs a condition with a task and optional description |
| [`BatchContext`](./batch-context) | Record | Progress snapshot for batch jobs (total, completed, failed items) |
| [`StreamingBatchContext`](./batch-context#streamingbatchcontext) | Record | Progress snapshot during the streaming phase of batch construction |
| [`JobPriority`](./job-options#jobpriority) | Enum | Priority levels: LOWEST, LOW, NORMAL, HIGH, CRITICAL |
| [`BackoffPolicy`](./job-options#backoffpolicy) | Enum | Retry delay strategies: NONE, FIXED, EXPONENTIAL |
| [`JobType`](./job-options#jobtype) | Enum | Job categories: SINGLE, RECURRING, BATCH, CHAIN, WORKFLOW, SYSTEM |
| [`CircuitBreakerProfile`](./annotations#circuitbreakerprotected) | Enum | Pre-configured circuit breaker profiles: DEFAULT, FAST, CRITICAL, EXTERNAL_API, CLAIM_PATH |

### `run.ratchet.api` — Annotations

| Annotation | Target | Purpose |
|---|---|---|
| [`@Recurring`](./annotations#recurring) | Method | Declares a method as a cron-scheduled recurring job |
| [`@CircuitBreakerProtected`](./annotations#circuitbreakerprotected) | Type, Method | Wraps invocations through the ResilienceStrategy circuit breaker |
| [`@DoNotRetry`](./annotations#donotretry) | Type | Marks exception classes that should never be retried |
| [`@Incubating`](./annotations#incubating) | Any | Marks experimental APIs that may change without deprecation |

### `run.ratchet.api` — Functional Interfaces

| Interface | Signature | Purpose |
|---|---|---|
| [`SerializableCheckedRunnable`](./functional-interfaces#serializablecheckedrunnable) | `void run() throws Exception` | Primary job task definition |
| [`SerializableConsumer<T>`](./functional-interfaces#serializableconsumert) | `void accept(T t)` | Success callbacks, progress hooks |
| [`SerializableBiConsumer<T,U>`](./functional-interfaces#serializablebiconsumert-u) | `void accept(T t, U u)` | Failure callbacks (context + error) |
| [`SerializableFunction<T,R>`](./functional-interfaces#serializablefunctiont-r) | `R apply(T t)` | Value-based workflow conditions |
| [`SerializablePredicate<T>`](./functional-interfaces#serializablepredicatet) | `boolean test(T t)` | Custom workflow conditions |
| [`SerializableCheckedConsumer<T>`](./functional-interfaces#serializablecheckedconsumert) | `void accept(T t) throws Exception` | Streaming batch item processing |

### `run.ratchet.api.event` — Event Types

| Event | Fires When |
|---|---|
| [`JobStartedEvent`](./event-system#jobstartedevent) | Job begins execution |
| [`JobCompletedEvent`](./event-system#jobcompletedevent) | Job completes successfully |
| [`JobFailedEvent`](./event-system#jobfailedevent) | Job fails after final attempt |
| [`JobRetryingEvent`](./event-system#jobretryingevent) | Job is being retried |
| [`JobCancelledEvent`](./event-system#jobcancelledevent) | Job cancellation confirmed |
| [`JobPausedEvent`](./event-system#jobpausedevent) | Job paused |
| [`JobResumedEvent`](./event-system#jobresumedevent) | Job resumed |
| [`JobDlqEvent`](./event-system#jobdlqevent) | Job moved to dead letter queue |
| `JobSignalWaitingEvent` | Job created in `WAITING` state for a named signal |
| `JobSignaledEvent` | Signal delivered to a waiting job |
| `JobSignalTimedOutEvent` | Waiting job exceeded its signal timeout |
| [`BatchCompletingEvent`](./event-system#batchcompletingevent) | Batch is finishing |
| [`BatchCompletedEvent`](./event-system#batchcompletedevent) | Batch fully complete |
| [`ChainStartedEvent`](./event-system#chainevent-types) | Workflow chain begins |
| [`ChainCompletedEvent`](./event-system#chainevent-types) | Workflow chain succeeds |
| [`ChainFailedEvent`](./event-system#chainevent-types) | Workflow chain fails |
| [`WorkflowBranchTriggeredEvent`](./event-system#workflowbranchtriggeredevent) | A workflow condition matches |

### `run.ratchet.spi` — Extension Points

| Interface | Purpose |
|---|---|
| [`RetryPolicy`](./spi-interfaces#retrypolicy) | Custom retry/backoff logic |
| [`ResilienceStrategy`](./spi-interfaces#resiliencestrategy) | Circuit breaker / bulkhead wrapping |
| [`ClassPolicy`](./spi-interfaces#classpolicy) | Deserialization class allowlist |
| [`ErrorSanitizer`](./spi-interfaces#errorsanitizer) | Scrub sensitive data from error messages |
| [`JobInvocationResolver`](./spi-interfaces#jobinvocationresolver) | Custom callback-to-job invocation resolution |
| [`ResultPersistenceStrategy`](./spi-interfaces#resultpersistencestrategy) | Custom job return-value persistence |
| [`ExecutorProvider`](./spi-interfaces#executorprovider) | Supply custom thread pools or virtual threads |
| [`BeanResolver`](./spi-interfaces#beanresolver) | Custom bean instantiation (default: CDI) |
| [`MetricsCollector`](./spi-interfaces#metricscollector) | Emit custom metrics (Micrometer, StatsD, etc.) |
| [`JobLogger`](./spi-interfaces#joblogger) | Custom job logging backend |
| [`ClusterCoordinator`](./spi-interfaces#clustercoordinator) | Distributed wakeup notifications |
| [`StartupCoordinator`](./spi-interfaces#startupcoordinator) | Store-backed startup leases for destructive initialization tasks |
| [`NodeIdentityProvider`](./spi-interfaces#nodeidentityprovider) | Identify nodes in a cluster |
| `RatchetOptions` | Required `@ApplicationScoped` CDI-produced runtime options — applications must produce one or the scheduler refuses to start |

## How to Read This Reference

### Entry Point

Start with [`JobSchedulerService`](./job-scheduler-service) -- it is the write-side interface you inject for scheduling and lifecycle operations. Every scheduling operation begins here:

```java
@Inject
JobSchedulerService scheduler;
```

For dashboards, admin tools, and support workflows, inject [`JobQueryService`](./job-query-service) separately. Keeping the query API separate lets applications grant read-only access without exposing mutation methods.

### Building Jobs

`JobSchedulerService.enqueue()` returns a [`JobBuilder`](./job-builder), which provides the fluent API for configuring retries, priorities, timeouts, workflows, and callbacks before calling `submit()`.

### Inside a Running Job

When Ratchet executes your job, [`JobContext`](./job-context) is available on the current thread. Use it to access parameters, the job ID, and the current job-scoped logger binding.

### Handling Results

[`JobResult<T>`](./job-result) captures everything about a completed execution and is used in workflow conditions to drive branching logic.

### API vs SPI

- **API** (`run.ratchet.api`): Classes you use directly. Stable across minor versions.
- **SPI** (`run.ratchet.spi`): Extension points you implement to customize behavior. Interfaces marked `@Incubating` may change between minor versions.

To provide a custom SPI implementation, create a CDI bean annotated with `@Alternative @Priority(APPLICATION)`.

## Maven Coordinates

```xml
<dependency>
  <groupId>run.ratchet</groupId>
  <artifactId>ratchet-api</artifactId>
</dependency>
```

The `ratchet-api` module has no runtime dependencies beyond Jakarta EE API types (`jakarta.enterprise.cdi-api` and `jakarta.interceptor-api`, both provided by the runtime). It defines the complete public API and SPI surface.

## See Also

- [Getting Started](/getting-started/first-job)
- [Concepts](/concepts/job-lifecycle)
- [SPI Interfaces](./spi-interfaces)
