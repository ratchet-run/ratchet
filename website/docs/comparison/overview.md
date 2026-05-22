---
sidebar_position: 1
title: How Ratchet compares
description: Side-by-side comparisons of Ratchet with Quartz, JobRunr, Spring Batch, jBeret, and db-scheduler, including where each competitor wins.
---

# How Ratchet compares

Ratchet is not the right answer for every Java job-scheduling problem. This section lays out how it stacks up against the libraries you are most likely already considering, what each one does well, and when you should pick something else.

If you only read one thing on this page, read the decision tree below.

## Pick the right tool

```
Are you on Spring Boot, and is Spring already your DI container?
├── Yes → JobRunr or Spring Batch (depending on whether it's
│           fire-and-forget jobs or chunked batch processing).
└── No
    ├── Is your app a Jakarta EE / MicroProfile / CDI app?
    │   ├── Yes → Ratchet (this is the case it was built for).
    │   └── No → continue below.
    └── Do you need persistent jobs, retries, and workflows
        without pulling in a framework at all?
        ├── Just persistent jobs + cron, nothing fancy → db-scheduler.
        ├── 20-year-old codebase already using Quartz → keep Quartz.
        └── Full JSR-352 batch spec required → jBeret.
```

## At a glance

<ComparisonMatrix />

**Footnotes**

1. JobRunr ships starters for Spring Boot, Quarkus, and Micronaut. It works in a vanilla CDI app, but Jakarta EE is not the design center.
2. db-scheduler has no DI integration of its own; you wire it up by hand in whatever container you use.
3. Quartz supports trigger listeners and job chaining via `JobChainingJobListener`, but it is bolted on rather than a first-class workflow primitive.
4. JobRunr Core is LGPL. JobRunr Pro is a paid commercial license that locks a substantial set of features (clustering with leader election, dashboard auth/SSO, batches, multi-tenancy, parameter encryption, custom/prioritized queues, atomic enqueue-on-condition, job continuations, PostgreSQL LISTEN/NOTIFY, and others) behind it, plus a commercial support tier with SLAs. See [the JobRunr pricing page](https://www.jobrunr.io/en/pricing/) for the current feature split.
5. JobRunr Core supports success-only parent-child continuations (`BackgroundJob.enqueue(parentId, ...)`). Conditional branching, failure-path callbacks, and result-aware branching require either custom code inside the parent job or JobRunr Pro's batch builder. Ratchet ships `.thenOnSuccess(...)`, `.thenOnFailure(...)`, and result-aware branching at the submission site. See [Ratchet vs JobRunr → Workflows](./vs-jobrunr.md#workflows) for the head-to-head.
6. Ratchet has no in-core dashboard by design. A scheduler dashboard has real tradeoffs (Jakarta-portable servlet/REST limits modern frontend tooling; standalone HTTP-embedded designs like JobRunr's are not Jakarta-portable; both inherit ops surface), and bundling one into core would couple the scheduler's API to a UI's release cadence. The intended path is the **query layer SPI** plus integration with existing tools (Grafana/Datadog via the Micrometer adapter, or your own UI built on the query API). An optional admin/control panel may ship as a separate module post-1.0, never as part of core.

## Where Ratchet is the wrong choice

Be honest: there are real cases where you should pick something else.

- **You are on Spring Boot and you do not want a second DI model.** Use JobRunr or Spring Batch. Ratchet works in Spring via a `BeanResolver` adapter, but the language and idioms in our docs assume CDI.
- **You need a production-grade web dashboard today.** JobRunr's dashboard is excellent. Ratchet does not have one yet.
- **You need a battle-tested 1.0.** Ratchet is alpha (`0.x`). The API surface is mostly settled, but we have not committed to SemVer guarantees. If you cannot tolerate breaking changes between minor versions for the next 6–12 months, wait for 1.0.
- **You need the JSR-352 spec, literally.** Use jBeret (or IBM JBatch on WebSphere). Ratchet is JSR-352-inspired in places but does not claim spec compliance.
- **All you need is "run this method on a cron, persist it across restarts."** db-scheduler is smaller, simpler, and a perfect fit for that. Ratchet is overkill.

## Where Ratchet was built to win

The cases where Ratchet is meaningfully better than the alternatives are narrow and specific:

1. **Jakarta EE 10/11 apps that don't want Spring.** Ratchet injects through CDI like any other bean. No `SchedulerFactoryBean`, no `@EnableBatchProcessing`, no separate configuration model. If your app already uses `@Inject`, you are most of the way there.
2. **Multi-store deployments where some tenants run MySQL, some Postgres, and some MongoDB.** Ratchet's store SPI is enforced by a TCK that every store implementation has to pass, so the behavior you see on Postgres is the behavior you see on Mongo. Quartz and Spring Batch are SQL-only. JobRunr supports Mongo but does not have a TCK across stores.
3. **Apps that need to capture *who* scheduled a job for audit or authorization.** Ratchet captures the Jakarta Security `CallerPrincipal` at submission time and exposes a `JobAuthorizationPolicy` SPI for governance rules. Nothing else in this list does this natively.
4. **Apps that want resilience built into the scheduler, not bolted on.** The scheduler ships a circuit breaker, retry policies (with SPI), dead letter queue, and worker tag affinity. You do not need MicroProfile Fault Tolerance or Resilience4j on top.

If two or more of those four bullets describe your app, this is the section to keep reading.

## Head-to-head pages

- [Ratchet vs Quartz](./vs-quartz.md)
- [Ratchet vs JobRunr](./vs-jobrunr.md)
- [Ratchet vs Spring Batch](./vs-spring-batch.md)
- [Ratchet vs jBeret (Jakarta Batch / JSR-352)](./vs-jberet.md)
- [Ratchet vs db-scheduler](./vs-db-scheduler.md)
