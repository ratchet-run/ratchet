---
title: Scheduled & recurring jobs
description: Cron and delayed jobs that survive restarts and never double-fire across a cluster, run inside the Jakarta EE server you already operate — no separate scheduler.
---

# Scheduled & recurring jobs

Every application grows a list of things that have to happen on a clock. Purge expired sessions at 2 AM. Send the weekly digest on Monday morning. Retry the failed exports an hour later. Poll a partner's API every fifteen minutes during business hours.

The usual answer is Quartz, or a `@Scheduled` method, or a cron line on one box. Each one has the same two holes. The schedule lives outside your database, so a restart in the wrong second drops a run on the floor. And the moment you run more than one node, the cron fires on all of them at once, and now three servers are sending the same digest.

Ratchet treats a scheduled run as what it already is: a job. It gets written to your database before it runs, claimed by exactly one node, retried on failure, and picked up again after a crash. The schedule is a row, not a thread on a single machine. If you already run Ratchet for background work, recurring work is the same engine with a cron string attached.

::: tip Verified
The Java on this page compiles against `ratchet-api` `0.1.1-SNAPSHOT`. It shows real API usage, not pseudocode — the running app needs a Jakarta EE server and a configured store.
:::

## The declarative path: `@Recurring`

For schedules you know at build time, annotate a method on any CDI bean. At startup Ratchet scans your beans, validates each annotated method, and registers it. No XML, no scheduler factory, no trigger objects.

```java
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobContext;
import run.ratchet.api.Recurring;

import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class MaintenanceService {

  @Recurring(cron = "0 0 2 * * ?", name = "Nightly cleanup")
  public void purgeExpiredSessions() {
    // Runs at 02:00 every day, in UTC.
  }

  @Recurring(
      cron = "0 */15 9-17 ? * MON-FRI",
      zone = "America/New_York",
      name = "Business-hours health check",
      priority = 8,
      maxRetries = 5,
      backoffPolicy = BackoffPolicy.EXPONENTIAL,
      tags = {"health", "monitoring"})
  public void healthCheck(JobContext context) {
    context.logger().info("health check running");
  }
}
```

The method has to be `public`, live on a CDI bean, and take either nothing or a single `JobContext`. The return value is ignored. The cron string is a Quartz expression — six or seven fields, `second minute hour day-of-month month day-of-week [year]` — and it is evaluated in the `zone` you name, which matters the two days a year that daylight saving moves the clock. Leave `zone` off and you get UTC.

The second method shows the knobs you would otherwise wire up by hand: a priority, a retry budget, exponential backoff, and tags you can filter on later. They are annotation attributes, not a separate configuration file.

## Why it does not double-fire

This is the part a single-node cron cannot give you. Each `@Recurring` method gets an identity — its `id`, which defaults to the fully qualified class and method name. Ratchet uses that identity as a **business key**, and business keys are unique among active jobs. So no matter how many nodes boot the same code, only one recurring master can exist for that method. The cron ticks once, on one node, and the resulting work is claimed by one worker.

You get exactly-once registration from the same constraint that stops two business-key jobs from running at once. There is no second distributed lock to operate, and nothing extra to configure when you scale from one node to five.

## The programmatic path: `scheduleRecurring`

Some schedules are not known until runtime — a per-tenant report whose cron a customer picks in a settings screen. Build those with the fluent API instead of an annotation.

```java
import java.time.Duration;
import java.time.ZoneId;
import java.util.List;

import run.ratchet.api.JobOptions;
import run.ratchet.api.JobSchedulerService;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class ReportScheduler {

  @Inject JobSchedulerService scheduler;

  /** A schedule the operator defines at runtime, not at compile time. */
  public void scheduleHourlyReport(String tenantId, String cron) {
    scheduler
        .scheduleRecurring(cron, ZoneId.of("UTC"), () -> generateReport(tenantId))
        .withOptions(JobOptions.defaults()
            .withMaxRetries(3)
            .withTimeout(Duration.ofMinutes(10)))
        .withTags(List.of("reports", "tenant:" + tenantId))
        .withBusinessKey("hourly-report-" + tenantId)
        .submit();
  }
}
```

The `withBusinessKey` here is doing real work. It scopes the recurring master per tenant, so re-running this method for the same tenant does not stack up a second schedule. The lambda is a method reference on a CDI bean — Ratchet serializes only the arguments (`tenantId`) and resolves the bean from CDI when the job runs, so injected dependencies are live at execution time, not captured at submission.

## One-shot, deferred

Not every timed job repeats. "Send a reminder in 30 minutes" is a single run at a future instant, and `schedule` takes a `Duration`:

```java
scheduler.schedule(Duration.ofMinutes(30), () -> sendReminder(userId)).submit();
```

The delay is computed once, at submission: the job is persisted now and stays invisible to the poller until the clock passes `now + 30m`. Survive a restart in between and the reminder still fires, because the due time is a column, not a timer in memory.

## Honest scope

A few things worth saying plainly:

- Cron is **Quartz syntax** (6–7 fields with `?` and `L`/`W` support), not the 5-field Unix crontab. `0 0 2 * * ?` is daily at 2 AM, and the leading field is seconds.
- Schedules default to **UTC**. Set `zone` per job when a schedule has to track a wall clock through DST.
- A recurring job is regular, not real-time. Fire times are honored at the resolution of the poll cycle, so a 15-minute schedule is reliable; a "run at exactly 14:30:00.000" guarantee is not what this is.

## Why run it through Ratchet

You could keep Quartz, or a `@Scheduled` bean, or a crontab. The argument for folding the schedule into Ratchet is the same one it makes everywhere else: the run is durable because it was written to your database before it happened, and it is cluster-safe because one row can only be claimed once. The scheduler is not a separate service with its own tables and its own failure modes. It is the job engine you already run, plus a cron string.

## Next steps

- [Scheduling](../concepts/scheduling.md) — every mode, cron grammar, and timezone behavior in depth
- [Clustering](../concepts/clustering.md) — how one row gets claimed by exactly one node
- [Retry strategies](../concepts/retry-strategies.md) — tune backoff and attempts per schedule
- [Annotations](../api-reference/annotations.md) — the full `@Recurring` attribute reference
- [Quickstart](../getting-started/quickstart.md) — get a first job running
