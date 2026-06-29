---
sidebar_position: 6
title: Ratchet vs db-scheduler
description: When the smaller, simpler db-scheduler is the better choice, and when Ratchet's extra features earn the extra weight.
---

# Ratchet vs db-scheduler

[db-scheduler](https://github.com/kagkarlsson/db-scheduler) is a small, focused job scheduling library by Gustaf Karlsson. It has a single design goal: schedule tasks to run later, persist them across restarts in a single database table, and coordinate workers via row locks. That is it. No DI integration, no framework assumptions, no workflows, no resilience SPI. Just a clean, well-built scheduler in one jar.

That focus is the entire reason to use it. It is also the reason you might outgrow it.

## TL;DR

| Pick **db-scheduler** if                                  | Pick **Ratchet** if                                       |
|-----------------------------------------------------------|-----------------------------------------------------------|
| You want the smallest possible library that gets the job done | You want CDI integration and Jakarta EE features        |
| You do not need workflows, branching, or job chains       | You need workflow chains and conditional branching        |
| Your store is Postgres, MySQL, Oracle, MSSQL, or H2       | You need MongoDB support                                  |
| You wire your own DI by hand                              | You want a built-in circuit breaker and dead letter queue |
| You like reading the entire codebase before adopting      | You need worker tag affinity, signal-waiting, or auth SPI |

## What db-scheduler is

db-scheduler is roughly 5,000 lines of Java. It does three things:

1. Persists a task and its execution time to a single database table.
2. Polls the table on a configurable interval, claims due tasks via row locks, and hands them to a worker thread pool.
3. Supports one-time tasks, recurring tasks (fixed delay or cron), and tasks with data payloads.

That is the whole library. There is no DI integration: you instantiate a `Scheduler` directly with a `DataSource` and a list of registered task definitions. You wire it into your application however you like.

```java
RecurringTask<Void> nightlyCleanup = Tasks
    .recurring("nightly-cleanup", FixedDelay.ofMinutes(60))
    .execute((inst, ctx) -> cleanupExpiredSessions());

Scheduler scheduler = Scheduler.create(dataSource)
    .startTasks(nightlyCleanup)
    .registerShutdownHook()
    .build();

scheduler.start();
```

For one-shot jobs:

```java
OneTimeTask<MyPayload> myTask = Tasks
    .oneTime("my-task", MyPayload.class)
    .execute((inst, ctx) -> processPayload(inst.getData()));

scheduler.schedule(myTask.instance("id-1", payload), Instant.now().plus(Duration.ofMinutes(30)));
```

It is clean, it is minimal, and it works.

## What Ratchet is

Ratchet is bigger. It assumes CDI, ships multiple stores including MongoDB, has workflow primitives, captures `CallerPrincipal`, has a circuit breaker, a `RetryPolicy` SPI, a dead letter queue, and worker tag affinity. The reference implementation is around 112 files of source code in the `ratchet` module alone.

Where db-scheduler asks "what's the smallest correct way to schedule a task?", Ratchet asks "what do Jakarta EE apps actually need from a scheduler, including the things you do not realize you need until you are in production?"

Both answers are valid. They are answers to different questions.

## Programming model

db-scheduler is task-definition-centric. You declare your tasks at startup time as `Task<T>` instances, register them with the scheduler, and schedule executions of those tasks with payload data.

Ratchet is lambda-centric. You pass a method reference at submission time. The "task definition" is implicit in the lambda; Ratchet uses ASM to extract the method reference and captured arguments.

```java
// db-scheduler
scheduler.schedule(
    sendEmailTask.instance("welcome-" + userId, new EmailPayload(userId)),
    Instant.now()
);

// Ratchet
scheduler.enqueueNow(() -> emailService.sendWelcome(userId));
```

The db-scheduler model is more explicit; the Ratchet model is more concise. The db-scheduler model also forces you to think about serialization at definition time (because you declare your payload type), which can prevent some "wait, this isn't serializable" surprises that Ratchet's lambda capture occasionally produces.

## Persistence

db-scheduler is single-table and SQL-only. It supports Postgres, MySQL, MariaDB, Oracle, MSSQL, and H2 through standard JDBC. The schema is one table (`scheduled_tasks`) with a documented column layout, and the library is happy for you to apply the DDL however you want.

Ratchet uses a multi-table schema (jobs, batches, recurring jobs, archived jobs, locks, signals, etc.) and supports MySQL, PostgreSQL, Oracle, SQL Server, and MongoDB via a pluggable `JobStore` SPI with a TCK. The schema is bigger, the model is richer, and the trade-off is real: more code, more tables, more to understand.

If your store is Mongo, db-scheduler is not an option.

If your store is SQL and you want one table you can `SELECT *` from to see what is scheduled, db-scheduler is the cleaner answer.

## Workflows

db-scheduler has no workflows. A task that needs to schedule another task does so explicitly inside its execute method:

```java
.execute((inst, ctx) -> {
    validatePayment(orderId);
    scheduler.schedule(fulfillOrderTask.instance(...), Instant.now());
})
```

That works. It is also exactly the "roll your own workflow" pattern that motivated Ratchet's `thenOnSuccess` / `thenOnFailure` builder. Whether that is enough for you depends on how complex your workflows get.

## Resilience

db-scheduler has retry-on-failure with backoff (via `FailureHandler`). That is the whole resilience surface. There is no circuit breaker, no dead letter queue (failed tasks just stay in the table), and no SPI for swapping in custom retry strategies beyond the `FailureHandler` interface.

Ratchet ships a circuit breaker, a `RetryPolicy` SPI, a dead letter queue, signal-waiting jobs, and worker tag affinity. If you need any of those, db-scheduler will require you to build them yourself.

## DI and integration

db-scheduler is intentionally DI-agnostic. You instantiate the scheduler yourself in whatever bootstrapping code your app has. There are Spring Boot starters (`db-scheduler-spring-boot-starter`) for the Spring case, but the library itself does not assume anything about your container.

Ratchet is CDI-native. `@Inject JobSchedulerService` works out of the box on WildFly, Payara, Open Liberty, and GlassFish. If you are not on CDI, Ratchet is harder to wire up than db-scheduler, not easier.

## Clustering and coordination

Both libraries are cluster-aware out of the box. db-scheduler coordinates multiple worker JVMs via row locks on its single `scheduled_tasks` table. Clean, well-known mechanism, no separate lock table needed.

Ratchet uses a similar store-coordinated model through its `LockStore` SPI: advisory locks on SQL stores, equivalent claim semantics on Mongo, and a `SingletonLeaseService` on top for "run this on at most one node" use cases (singleton recurring jobs, cleanup tasks, schema migrations). The result is the same: multiple worker JVMs cooperate through the database without needing a separate coordination service.

Recovery from a worker crash is automatic in both libraries, which is the bar you actually want for production. db-scheduler's `DeadExecutionsDetector` finds tasks whose last heartbeat is too old and either reschedules them or marks them for re-execution. That's exactly the mechanism that makes "the JVM died mid-job" a non-event. Ratchet's `OrphanRecoveryTimer` does the equivalent: claimed jobs whose worker has gone stale are returned to `PENDING` automatically. Different code, same outcome. (For context, the schedulers in this comparison that *don't* auto-recover are Spring Batch and jBeret; their `JobOperator` model leaves stranded jobs to your application code, which is the kind of thing that turns a worker crash from "annoying" into "outage." db-scheduler and Ratchet are aligned here.)

Where the two libraries diverge is when you want to *steer* work, not just claim it. Ratchet's worker tag affinity routes specific jobs to specific worker pools (e.g., "GPU jobs only run on workers tagged `gpu`"). db-scheduler has no equivalent, every worker is fungible.

## What db-scheduler does better

- **Simplicity.** It is small, focused, and easy to understand end to end. You can read the source in an afternoon.
- **No assumptions about your container.** It does not care whether you use Spring, CDI, Micronaut, or no DI at all.
- **Single table.** One `scheduled_tasks` table, one SQL query to see what is scheduled. Easy to operate.
- **Stability.** It is past 1.0, actively maintained, and battle-tested. Ratchet is alpha.
- **Lower memory and startup overhead.** Small jar, small dependency footprint, fast cold start.

## What Ratchet does better

- **Jakarta EE integration.** CDI-first, with caller-principal capture and a `JobAuthorizationPolicy` SPI.
- **MongoDB support.** db-scheduler does not have it.
- **Workflows as a first-class primitive.** `thenOnSuccess`, `thenOnFailure`, result-aware branching.
- **Built-in resilience.** Circuit breaker, retry policy SPI, dead letter queue.
- **Batch primitives.** Parallel batch with progress tracking, streaming batch for large datasets.
- **Worker tag affinity and signal-waiting.** Advanced orchestration features that db-scheduler does not have.

## When to use which

Use db-scheduler if your problem really is "I need to run methods later, persist them, and survive restarts." Most apps that think they need a "job scheduler" actually need exactly this, and the simplicity is the point. Ratchet would be overkill.

Use Ratchet if you find yourself needing more than the basics: workflows, circuit breakers, retry policy SPI, MongoDB, caller identity, batch processing, worker affinity. These are not features you can graft onto db-scheduler without effectively writing them yourself.

A reasonable migration path: start with db-scheduler. If you find yourself building the same scaffolding around it that Ratchet ships in the box, consider switching. The reverse path (Ratchet to db-scheduler) is less common because you tend to add features over time, not remove them.
