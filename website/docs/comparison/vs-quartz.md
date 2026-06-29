---
sidebar_position: 2
title: Ratchet vs Quartz
description: How Ratchet differs from Quartz Scheduler in API design, persistence, CDI integration, and resilience, with honest tradeoffs.
---

# Ratchet vs Quartz

[Quartz](http://www.quartz-scheduler.org/) has been the default Java job scheduler since 2001. It is in every enterprise Java codebase older than five years, and there is a very good reason for that: it works, and the people who built it knew what they were doing. If you are reading this and Quartz is already running in production for you, do not rip it out without a reason.

Quartz also predates a lot of what modern Jakarta EE applications take for granted: CDI as the default DI model, lambdas, and stores that aren't relational databases. Ratchet starts from those defaults rather than retrofitting around them.

## TL;DR

| Pick **Quartz** if                                    | Pick **Ratchet** if                                       |
|-------------------------------------------------------|-----------------------------------------------------------|
| You already have it running and it works             | You are starting a new Jakarta EE 10/11 app               |
| You need maximum trigger / calendar flexibility       | You want a lambda-based API, not a `Job` class per task   |
| You need a 1.0-stable, ubiquitous, well-known library | You need pluggable persistence including MongoDB          |
| Your team has Quartz expertise on staff               | You need workflow chains and circuit breakers built in    |

## API: a class per job vs a method reference

The single biggest day-to-day difference is the programming model.

**Quartz** requires a `Job` class per job type, with state passed through a `JobDataMap`:

```java
public class SendReminderJob implements Job {
    @Override
    public void execute(JobExecutionContext ctx) throws JobExecutionException {
        long orderId = ctx.getMergedJobDataMap().getLong("orderId");
        // ... do the work
    }
}

JobDetail job = JobBuilder.newJob(SendReminderJob.class)
    .usingJobData("orderId", orderId)
    .build();

Trigger trigger = TriggerBuilder.newTrigger()
    .startAt(Date.from(Instant.now().plus(Duration.ofMinutes(30))))
    .build();

scheduler.scheduleJob(job, trigger);
```

**Ratchet** takes a method reference. The job class and trigger are inferred:

```java
scheduler.schedule(Duration.ofMinutes(30), () -> sendReminder(orderId))
    .submit();
```

The Ratchet version is less code, and the data flow is type-checked at compile time. Quartz's `JobDataMap` is `Map<String, Object>`, so a typo in the key name fails at runtime instead.

This is not a small thing. Most production Quartz bugs we have seen come from `JobDataMap` keys drifting between schedulers and job classes during refactors.

## DI integration

Quartz predates CDI. To inject beans into a Quartz `Job`, you have to write a `JobFactory` and wire it up at scheduler construction time:

```java
public class CdiAwareJobFactory implements JobFactory {
    @Inject private BeanManager beanManager;

    @Override
    public Job newJob(TriggerFiredBundle bundle, Scheduler scheduler) {
        Class<? extends Job> jobClass = bundle.getJobDetail().getJobClass();
        // Look up via BeanManager, dispose context manually, etc.
    }
}
```

Ratchet is `@ApplicationScoped` and injects with `@Inject`:

```java
@Inject
JobSchedulerService scheduler;
```

If your app uses CDI for everything else, the wiring cost is zero.

## Persistence

Quartz ships its own JDBC `JobStore`. It works, but it has constraints:

- **SQL only.** No Mongo, no Cassandra. There is a community RAMJobStore for testing, and that is the only non-SQL option in the box.
- **Quartz-owned schema.** The DDL is Quartz's, the tables start with `QRTZ_`, and migrations are your problem. Several wrapper projects exist (`quartz-mongodb`, etc.) but they are not officially supported.
- **Connection management is separate.** Quartz wants its own `DataSource`, configured through `quartz.properties` or its own setter calls. Sharing your application's existing JPA `DataSource` requires extra plumbing.

Ratchet ships a `JobStore` SPI with five reference implementations (MySQL, PostgreSQL, Oracle, SQL Server, MongoDB) that all pass the same TCK. The DDL is plain SQL files you can apply with whatever migration tool you already use (Flyway, Liquibase, plain `psql`). You can also opt in to a `SchemaMigrationLifecycleHook` that runs at startup if you want zero-config.

For Mongo, no third-party adapter is needed. Indexes are created at startup because index correctness gates claim safety.

## Clustering

Quartz has supported multi-instance clustering since 2.0 via its JDBC JobStore in clustered mode. Multiple Quartz instances share the same database, coordinate via row locks, and any instance can fire any trigger. This is a real and long-standing Quartz feature, and it works.

Ratchet has equivalent multi-node coordination through its `JobStore` SPI: any worker node can claim any pending job, claim safety is enforced by the store's advisory locks (or equivalent on Mongo), and a built-in `SingletonLeaseService` provides "run this on at most one node at a time" semantics for recurring jobs and other singleton work. That's the practical equivalent of leader election for the cases that need it.

The mechanism differs (Quartz uses its own schema and lock tables; Ratchet uses a pluggable `LockStore` and `NodeIdentityProvider`), but the basic outcome of "run N workers, they coordinate through the store, no message broker required" is the same.

The same parity holds for crash recovery, with one caveat. Quartz's clustered JDBC JobStore has a `RecoveryService` that reclaims triggers from failed instances on instance startup. If a Quartz node dies mid-trigger, another node picks up the work. That's real and it works. **The caveat** is that this only applies in clustered mode; single-node Quartz with the default RAM JobStore (or even single-node JDBC JobStore) doesn't have it, and the clustered configuration is a separate operational story from the default. Ratchet's recovery is automatic regardless of deployment shape: every claimed job has a stale-timeout, the `OrphanRecoveryTimer` runs continuously, and a crashed worker's in-flight jobs return to `PENDING` without any configuration switch. For multi-node Quartz deployments the difference is minor; for single-node or "we'll cluster later" deployments it's meaningful.

## Workflows

Quartz has `JobChainingJobListener` for "run job B after job A succeeds." It works for simple cases, but it isn't a workflow primitive. There is no built-in conditional branching, no failure path, and no way to pass a result from job A to job B without parking it in external state.

Ratchet treats chaining as a first-class API:

```java
scheduler.enqueue(() -> validatePayment(orderId))
    .thenOnSuccess(() -> fulfillOrder(orderId))
    .thenOnFailure(() -> notifyPaymentFailure(orderId))
    .submit();
```

Each step is a separate persisted job, so a crash between step 2 and step 3 does not lose the work from step 1. If you have ever rolled your own "job that schedules the next job" pattern on top of Quartz, this is what it would look like if a library shipped it for you.

## Resilience

Quartz has misfire policies (what to do if a trigger fires late) but no built-in retry policy, no circuit breaker, and no dead letter queue. The standard answer is "wrap your job code in MicroProfile Fault Tolerance" or "catch the exception and reschedule yourself."

Ratchet ships:

- Configurable retry with fixed or exponential backoff.
- A pluggable `RetryPolicy` SPI for custom strategies (per-exception, per-tenant, etc.).
- A circuit breaker with a `ResilienceStrategy` SPI, exposed as a CDI interceptor via `@CircuitBreakerProtected`.
- A dead letter queue for jobs that exhaust their retries.
- A `@DoNotRetry` exception marker for permanent failures.

These are not separate dependencies. They are part of the scheduler.

## Identity and authorization

In Jakarta EE 10, `SecurityContext.getCallerPrincipal()` tells you who is logged in. Ratchet captures that principal at the moment a job is submitted and persists it with the job record. Two things follow:

1. The submitting principal is captured and persisted with the job. The executor thread does not re-establish that caller's Subject, so job bodies run with no live `CallerPrincipal`. The captured principal is for owner-based authorization, not re-authentication: a `JobAuthorizationPolicy.checkExecute` hook can deny execution by captured owner (for example, when an account is deactivated after submission).
2. A `JobAuthorizationPolicy` SPI lets you write rules like "this user can cancel jobs they submitted, an admin can cancel any job."

Quartz has no notion of caller identity. Add it yourself if you need it.

## What Quartz does better

This list is real, and Ratchet is not pretending otherwise.

- **Maturity.** Quartz has been in production since 2001. Ratchet is alpha.
- **Trigger expressiveness.** Quartz has `CronTrigger`, `SimpleTrigger`, `CalendarIntervalTrigger`, `DailyTimeIntervalTrigger`, holiday calendars, and the ability to combine them. Ratchet has cron and delay. If you need to skip US federal holidays automatically, Quartz wins.
- **Documentation and Stack Overflow surface area.** Twenty years of "how do I do X in Quartz" answers exist. Ratchet has hundreds of pages of docs but no community-built ecosystem yet.
- **Spring Boot integration.** Quartz has had Spring's `SchedulerFactoryBean` for a decade.

## When to migrate

If you are starting a new Jakarta EE app and your team has not picked a scheduler yet, Ratchet's tradeoffs make sense. The API is faster to write, the workflows and resilience are built in, and the multi-store support matters more every year as Mongo and per-tenant SQL setups become common.

If you are running Quartz today, the question is whether the gains justify a migration. The answer is usually no unless one of the following is true:

- You are already rewriting the job code for some other reason.
- You are hitting limits with `JobDataMap` (typos in production, serialization issues, etc.).
- You need a feature Ratchet has and Quartz does not (Mongo, caller identity, first-class workflows, circuit breaker).

A migration path that has worked for us: run both side-by-side, route new jobs through Ratchet, let existing Quartz jobs ride out their lifecycle. They share a database connection but not a schema, so there is no conflict.
