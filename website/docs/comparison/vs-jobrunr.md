---
sidebar_position: 3
title: Ratchet vs JobRunr
description: How Ratchet differs from JobRunr on lambda capture, DI model, store options, dashboard, licensing, and commercial support.
---

# Ratchet vs JobRunr

[JobRunr](https://www.jobrunr.io/) is the most direct comparison to Ratchet in this lineup. Both libraries take a method reference, run it in the background, and rely on bytecode analysis with ASM to turn the lambda into a serialized job description. Both target modern Java rather than the Quartz-era enterprise stack.

If you are picking between Ratchet and JobRunr, the decision is rarely about features. The feature surface is similar. The decision is usually about your container, your persistence story, whether you need a dashboard, and your stance on a dual-licensed product with a paid support tier.

## TL;DR

| Pick **JobRunr** if                                       | Pick **Ratchet** if                                       |
|-----------------------------------------------------------|-----------------------------------------------------------|
| You are on Spring Boot, Quarkus, or Micronaut             | You are on Jakarta EE 10/11 or a CDI-only stack           |
| You need a production-grade web dashboard today           | You need Jakarta Security caller-principal capture        |
| You want a paid commercial support tier (JobRunr Pro)     | You want a TCK-enforced store SPI for cross-store parity  |
| You want a 1.0-stable library today                       | You want everything Apache 2.0 with no paid tier          |
| You are fine with LGPL core and Pro behind a paid license | You need a built-in circuit breaker and dead letter queue |

## The honest version

The single biggest reason to pick JobRunr over Ratchet today is that JobRunr has been past 1.0 for years. Ratchet is alpha. If you cannot tolerate API churn between minor versions for the next 6–12 months, this section is enough; pick JobRunr.

Everything that follows assumes you are choosing between them on technical merits, not maturity.

## Programming model

The surface API is similar. Both libraries take a lambda and turn it into a persisted job.

**JobRunr:**

```java
BackgroundJob.enqueue(() -> emailService.sendWelcome(userId));
BackgroundJob.schedule(Instant.now().plusSeconds(3600),
                      () -> reminderService.send(orderId));
```

**Ratchet:**

```java
scheduler.enqueueNow(() -> emailService.sendWelcome(userId));
scheduler.schedule(Duration.ofHours(1), () -> reminderService.send(orderId)).submit();
```

JobRunr historically used a static `BackgroundJob` facade, with a `JobScheduler` available for instance-based injection. Ratchet leads with the injected `JobSchedulerService`. The difference is mostly cosmetic; both work.

Under the covers, the two libraries do roughly the same thing. They inspect the lambda's bytecode at submission time with ASM, extract the method reference and captured arguments, and serialize them so the job can be reconstructed later. They also share the same broad failure modes: you cannot capture a `Connection`, a `Stream`, or anything else that does not serialize cleanly.

## DI integration

This is where the libraries split.

**JobRunr** has first-class starters for Spring Boot, Quarkus, and Micronaut. In a Spring Boot app, `@Autowired private JobScheduler scheduler;` Just Works. Configuration is via `application.properties`. In a vanilla CDI / Jakarta EE app, you can still use JobRunr, but you wire it up through its `BackgroundJobServerConfiguration` API rather than through CDI's bean container.

**Ratchet** is the inverse. It is CDI-native: `@Inject JobSchedulerService scheduler;` works out of the box on WildFly, Payara, Open Liberty, and GlassFish. The bean activator looks up `BeanResolver` through CDI, captures `CallerPrincipal` through `Instance<SecurityContext>`, and registers lifecycle hooks through CDI events. A Spring Boot starter is planned but not shipped.

The practical implication: if your application is Spring Boot, you will fight Ratchet on integration. If your application is Jakarta EE / CDI, you will fight JobRunr on integration. Pick the one that aligns with your container.

## Persistence

Both libraries support multiple stores, but the model differs.

**JobRunr** supports SQL (Postgres, MySQL, Oracle, SQL Server, H2, DB2, MariaDB), MongoDB, and ElasticSearch via storage providers. Each provider is part of the JobRunr codebase. The schema is JobRunr-owned and migrated automatically at startup unless you opt out.

**Ratchet** ships three stores (MySQL, PostgreSQL, MongoDB) implemented against a public `JobStore` SPI. Every store implementation has to pass a Technology Compatibility Kit before it ships. Schema migrations are off by default. DDL files ship as plain SQL, and you apply them with your existing tooling. There is an opt-in `SchemaMigrationLifecycleHook` if you want auto-apply.

The TCK is a real technical difference. It is the reason we can promise that claim semantics, retry behavior, and visibility timing are identical whether you are running Mongo or Postgres. JobRunr's storage providers are tested individually; Ratchet's are tested against a shared contract.

Whether you care depends on whether you need parity across stores. Most apps run a single store and the TCK is invisible. Multi-tenant apps where some customers run Postgres and others run Mongo benefit measurably.

## Dashboard

JobRunr has a web dashboard. Ratchet does not. If you need one *today*, JobRunr is the answer. That much is clear.

What deserves a paragraph rather than a one-line concession is *why* Ratchet doesn't ship one and what the alternative path looks like. "Missing dashboard" is the framing that makes JobRunr look ahead, when the actual situation is a deliberate design boundary with real tradeoffs on both sides.

JobRunr's dashboard works by embedding its own lightweight HTTP server inside the JVM. That gets you cross-runtime portability (it works on any Java app, Spring or otherwise) at the cost of running a second HTTP endpoint *alongside* whatever container is hosting your application, with its own auth model, its own TLS configuration, and its own ops surface. It's a well-built piece of software; it's also explicitly not Jakarta EE-portable in the sense that it doesn't sit inside the container's servlet pipeline.

For Ratchet, which is built for Jakarta EE / CDI apps, that design isn't a clean fit. A Jakarta-portable dashboard would have to use Jakarta Servlet/REST/WebSocket/Security and deploy as a WAR or sub-module inside the user's container, workable, but it forces frontend choices that look dated next to a React SPA, and bundling it into core would couple the scheduler's release cadence to a UI's. So Ratchet's position is: the scheduler ships primitives, dashboards live as separate modules.

The primitives that make this work today:

- A **query layer SPI** (already shipped) that exposes job search, detail lookup, execution history, dependency trees, batch children, recurring masters, and queue health as typed contracts. Building a dashboard on top of this is the intended path.
- A **Micrometer adapter** for plugging metrics into Grafana, Datadog, Prometheus, or anything else that speaks Micrometer.
- A **CDI event system** for streaming lifecycle changes (`JobCompletedEvent`, `JobFailedEvent`, etc.) into any downstream system.

An optional admin/control panel module may ship post-1.0 as a separate project, never as part of core. If you'd rather use external tools and skip the dashboard entirely, that's the supported path. If you'd rather have a packaged UI, JobRunr is honestly the better tool today and probably will remain so until someone (us or a community contributor) builds out the separate Ratchet dashboard module.

So the matrix entry showing "No (by design)" for Ratchet's dashboard isn't a placeholder for "we haven't gotten to it yet." It's a position statement about where this responsibility lives.

## Licensing and commercial support

This one matters for some teams more than others.

**JobRunr Core** is licensed under LGPL v3. **JobRunr Pro** is a paid commercial license that bundles two things together: a substantial set of additional features and a commercial support tier with SLAs from the JobRunr team.

The Pro feature list is long enough that it's worth checking [the JobRunr pricing page](https://www.jobrunr.io/en/pricing/) for the current version, but to give you a sense of scale, capabilities that live in Pro rather than Core include: leader election and advanced cluster coordination (Core supports multiple servers running against the same DB, but the more sophisticated leader-aware coordination is in Pro), server-side dashboard authentication and SSO, batch and parent-child job orchestration, multi-tenancy, end-to-end encryption of job parameters, atomic enqueue-on-condition, job continuations, custom and prioritized queues, dynamic job priority, advanced retention and archival policies, PostgreSQL `LISTEN/NOTIFY` integration, instant job processing without polling, carbon-aware scheduling, lifecycle notifications via email/Slack, and several others. The split is large enough that "which JobRunr features are you actually planning to use?" is a real evaluation question, not a footnote.

Worth noting for the leader-election point specifically: Ratchet has a built-in `SingletonLeaseService` that provides "run this on at most one node at a time" semantics through the same store coordination it uses for job claiming. It isn't a full Raft-style consensus protocol, but for the practical use cases leader election usually solves in a scheduler (single-instance recurring jobs, cleanup tasks, schema migrations), it's equivalent. That's available in Ratchet Core, no paid tier required.

For teams that need a name to call when production goes sideways, the paid support contract is often the deciding factor on its own, separate from any of the Pro features.

**Ratchet** is Apache 2.0 throughout. There is no paid tier. There is no plan for one. There is also no commercial support contract today; if you need a vendor SLA, this is a real gap.

If your organization avoids LGPL, prefers not to evaluate Core-vs-Pro feature gating, and is willing to handle support through the open-source community (or in-house), Ratchet's license model is simpler. If you actively want a vendor relationship and someone on the hook for production issues, JobRunr Pro is the easier procurement story.

## Caller identity and authorization

Ratchet captures `SecurityContext.getCallerPrincipal()` at job submission time and persists it with the job record. A `JobAuthorizationPolicy` SPI lets you express rules like "users can only cancel jobs they submitted."

JobRunr has no equivalent. If you need to know who submitted a job, you have to pass the identity through as a method argument and trust that it was set correctly. JobRunr Pro has multi-tenancy primitives that overlap with this in spirit, but they are not the same thing.

## Resilience

JobRunr ships retry-with-backoff and a `RetryFilter` extension point. Failed jobs that exhaust their retries go to a `FAILED` state and surface in the dashboard.

Ratchet ships retry-with-backoff, a `RetryPolicy` SPI, a circuit breaker (with `@CircuitBreakerProtected` CDI interceptor), a `@DoNotRetry` exception marker, a dead letter queue, signal-waiting jobs, and worker tag affinity.

The shape of "what counts as resilience" is broader in Ratchet. If you only need retries, the two libraries are equivalent. If you need circuit breaker, DLQ, and worker affinity built into the same library, that is Ratchet.

**Both libraries handle crash recovery automatically**, which is worth calling out because it's where most schedulers fall over. JobRunr Core's `BackgroundJobServer` writes a heartbeat to the storage provider; if a server stops heartbeating, its in-flight jobs are reclaimed by other servers. Ratchet's `OrphanRecoveryTimer` does the equivalent for claimed jobs whose worker has gone stale. Neither requires application-side intervention to recover from a crashed worker. (Spring Batch and jBeret are the schedulers on the other end of this spectrum, they leave the stranded-jobs problem to your application code. JobRunr and Ratchet are aligned here, even though Pro adds more sophisticated coordination on top.)

## Workflows

This is the area where the two libraries diverge the most, and where the "Limited" rating in the matrix above earns its caveat.

**What JobRunr Core gives you** is a parent-child continuation: `BackgroundJob.enqueue(parentJobId, () -> step2())` schedules step 2 to run after the parent job succeeds. That's it. The continuation only fires on success. There is no built-in failure path, no result-aware branching, no declarative way to say "if A succeeds, do B; if it fails, do C." If you want any of those, you write the logic yourself inside the job, or you wire up a separate event listener that filters on job state.

**What JobRunr Pro adds** is a `BatchJobBuilder` with parent-child relationships, `awaitsResultOf` semantics, and atomic batch enqueueing. These move JobRunr Pro into "real workflow primitive" territory. They are behind the paid commercial license.

**What Ratchet ships in Core** is closer to JobRunr Pro than JobRunr Core. The builder API expresses both branches at submission time:

```java
// Ratchet
scheduler.enqueue(() -> validatePayment(orderId))
    .thenOnSuccess(() -> fulfillOrder(orderId))
    .thenOnFailure(() -> notifyPaymentFailure(orderId))
    .submit();
```

The equivalent in JobRunr Core requires the failure path to live inside `validatePayment`:

```java
// JobRunr Core (the failure path has to be inside the parent)
JobId parentId = BackgroundJob.enqueue(() -> {
    try {
        validatePayment(orderId);
    } catch (Exception e) {
        BackgroundJob.enqueue(() -> notifyPaymentFailure(orderId));
        throw e;
    }
});
BackgroundJob.enqueue(parentId, () -> fulfillOrder(orderId));
```

That works, but it puts the workflow topology inside the job code rather than at the submission site. Refactoring the workflow means editing the methods themselves. The Ratchet version keeps the topology declarative and the steps independent.

The same gap shows up with result-aware branching. Ratchet can branch on what `validatePayment` returns; JobRunr Core jobs return `void` from the scheduler's perspective, so the result has to be carried out of band (database, message, status field) and inspected by the next step.

Each Ratchet step is a separate persisted job, which has a second-order benefit: a crash between step 2 and step 3 preserves the completed work from step 1 without any extra effort. JobRunr Core has the same property for its continuations, so you don't lose that, you just have fewer ways to wire them up.

If your workflows are "after A, do B" with no branching, both libraries cover it. If you need conditional or failure-path branching declared at the submission site rather than buried inside job methods, that's Core in Ratchet and Pro in JobRunr.

## What JobRunr does better

JobRunr's web dashboard is the most visible win. It is ready today, it looks polished, and it covers the operational questions you actually have to answer in production. Ratchet does not ship one.

The Spring Boot, Quarkus, and Micronaut starters are the second big win. If your container is one of those, JobRunr lowers the integration cost to nearly zero. Configuration goes through `application.properties` and autoconfiguration handles the rest.

Maturity is the third. JobRunr has been past 1.0 for years, has a real user base, and has accumulated enough community Q&A that searching "jobrunr how do I X" tends to find an answer. Ratchet is alpha and has none of that yet.

JobRunr Pro adds a fourth: a commercial support tier with SLAs. For organizations that need a vendor on the hook for production incidents, that contract is sometimes the entire decision, regardless of feature differences.

Finally, the JobRunr team has invested in documentation, examples, and onboarding flows in a way that is clearly visible. It is one of the better-documented open-source Java libraries of its size, and the polish translates to real productivity for new users.

## When to pick each

Pick JobRunr if your container is Spring Boot, Quarkus, or Micronaut. Pick it if a dashboard matters to you today, if you need 1.0 stability, or if you want a paid commercial support contract.

Pick Ratchet if your container is Jakarta EE 10/11 or CDI without Spring. Pick it if you need caller-identity capture and a `JobAuthorizationPolicy`, if you need a TCK-enforced store SPI for cross-store parity, if you need a circuit breaker and dead letter queue baked into the scheduler, or if your organization needs an Apache 2.0 license with no paid tier.

Both libraries are good. The decision usually comes down to your container, your stance on the dashboard, and whether you want vendor support, not to the API shape.
