---
title: Durable Workflows in Java Without a Workflow Server
description: Run multi-step Java workflows that survive crashes and restarts, with no workflow server to deploy. Ratchet stores each step as a job in the database you already run, and any node in your cluster can run the next step.
---

# Durable Workflows in Java Without a Workflow Server

Ratchet runs durable multi-step workflows inside your Java application. Each step is a job stored in the database you already run: PostgreSQL, MySQL, Oracle, SQL Server or MongoDB. Any node in your cluster can claim the next step. A finished step stays finished, so after a crash or a restart the workflow carries on from the first step that had not finished. You don't deploy a workflow server, a message broker or a second cluster. Ratchet is open source under Apache-2.0 and runs on Jakarta EE, Quarkus and Spring Boot.

The examples below follow one order through a chain of steps, a risk check that can wait days for a reviewer, and a fan-out that packs each line item in parallel.

::: tip Verified
The Java on this page compiles against `ratchet-api` `0.5.0`, with a stand-in `Orders` interface for your own order service. Every job lambda in it also passes the `0.5.0` runtime's lambda analysis, which `submit()` runs before it writes a job. A running app also needs a `ClassPolicy` that allows your packages (see the [Quickstart](/getting-started/quickstart)). The review step needs a store with the `SignalStore` capability, which every bundled store has.
:::

## How do I chain steps so a crash doesn't lose progress?

```java
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;

import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobSchedulerService;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class OrderWorkflow {

  @Inject JobSchedulerService scheduler;

  @Inject Orders orders;

  // Stores all three steps, then returns. Workers run them one after another.
  public JobHandle start(UUID orderId) {
    return scheduler
        .enqueue(() -> reserveStock(orderId))
        .then(() -> chargePayment(orderId))
        .then(() -> ship(orderId))
        .withMaxRetries(3)
        .withBackoff(BackoffPolicy.EXPONENTIAL, Duration.ofSeconds(10))
        .withTimeout(Duration.ofMinutes(5))
        .withIdempotencyKey(keyFor("order-workflow", orderId))
        .submit();
  }

  // Idempotency keys hold at most 36 characters and share one namespace across all jobs,
  // so derive a UUID from the purpose and the order ID.
  public static String keyFor(String purpose, UUID orderId) {
    return UUID.nameUUIDFromBytes((purpose + ":" + orderId).getBytes(StandardCharsets.UTF_8))
        .toString();
  }

  public void reserveStock(UUID orderId) {
    orders.reserveStock(orderId);
  }

  public void chargePayment(UUID orderId) {
    orders.charge(orderId);
  }

  public void ship(UUID orderId) {
    orders.ship(orderId);
  }
}
```

`submit()` writes all three steps to the database in one transaction and returns. The first step can run at once. Each later step becomes runnable only when the one before it succeeds. `Orders` stands for your own service; the job methods only call into it.

Steps added with `.then()` take the builder's retry, backoff and timeout settings. Here every step gets up to three retries, with exponential backoff starting at 10 seconds, and five minutes per attempt. If a step still fails after its last retry, Ratchet cancels the steps after it, so `ship` never runs after a failed charge.

The idempotency key makes `start()` safe to call twice. A second call for the same order returns the first workflow's handle and creates nothing. Ratchet keeps idempotency keys after it deletes old job history, so a call months later still finds the original. That also holds when the first workflow failed: to run an order again after a permanent failure, use a new key, for example by adding an attempt number to the purpose. Keys hold at most 36 characters and every job shares the same key space, which is why `keyFor` turns a purpose and the order ID into a name-based UUID.

Steps don't hand values to each other. Each lambda captures its arguments when you call `submit()`, here just the order ID, and a step reads anything else it needs from your own tables when it runs. Job methods must be `public`; `submit()` rejects a private or package-private target.

## What happens if a node crashes in the middle of a workflow?

Ratchet records each step's outcome in the database, so a crash only affects the step that was running at the time.

- Steps that finished stay finished and don't run again.
- Orphan recovery picks up the step that was running once the crashed node's heartbeat grace period has passed. It returns the step to pending, and any node can run it again from the start. Each recovery counts against `ratchet.node.max-crash-redeliveries` (3 by default), which is separate from the step's own retries. When that budget runs out, the next crash fails the step.
- Steps that hadn't started wait their turn as before.

Delivery is at-least-once. A step runs twice if its node dies after the work is done but before Ratchet records the result. Write each step so a second run does no harm: check whether the stock is already reserved, and send your payment provider an idempotency key of its own.

Ratchet reruns a single step. It never replays the whole workflow to rebuild state, so step code is ordinary Java with no determinism rules. A step can read the clock, call another service or generate a random number.

## How do I branch on a result or wait for a person?

```java
import java.time.Duration;
import java.util.UUID;

import run.ratchet.api.JobContext;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.SignalDecision;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class OrderScreening {

  @Inject JobSchedulerService scheduler;

  @Inject Orders orders;

  @Inject OrderWorkflow fulfilment;

  // Scores the order, then runs exactly one of the two branches.
  public JobHandle screen(UUID orderId) {
    return scheduler
        .enqueue(() -> riskScore(orderId))
        .whenResult(RiskRules::isLow, () -> release(orderId))
        .whenResult(RiskRules::needsReview, () -> holdForReview(orderId))
        .submit();
  }

  public Integer riskScore(UUID orderId) {
    return orders.riskScore(orderId);
  }

  public void release(UUID orderId) {
    fulfilment.start(orderId);
  }

  // Parks a job that holds no thread until a reviewer decides or two days pass.
  // One retry means a timeout wakes afterReview with no decision instead of failing the job.
  public void holdForReview(UUID orderId) {
    scheduler
        .enqueue(() -> afterReview(orderId))
        .awaitSignal("order-review-" + orderId, Duration.ofDays(2))
        .withMaxRetries(1)
        .withIdempotencyKey(OrderWorkflow.keyFor("order-review", orderId))
        .submit();
  }

  // Called by the review screen. Returns false when no job was waiting, such as on a double click.
  public boolean approve(UUID orderId, String reviewer) {
    return scheduler.deliverSignal("order-review-" + orderId, SignalDecision.approved(reviewer))
        > 0;
  }

  public boolean reject(UUID orderId, String reason) {
    return scheduler.deliverSignal(
            "order-review-" + orderId, SignalDecision.rejected(null, reason))
        > 0;
  }

  public void afterReview(UUID orderId) {
    SignalDecision decision = JobContext.current().signalPayload(SignalDecision.class);
    if (decision != null && decision.isApproved()) {
      fulfilment.start(orderId);
    } else {
      orders.cancel(orderId, decision == null ? "review timed out" : decision.rejectionReason());
    }
  }
}
```

```java
public final class RiskRules {
  private RiskRules() {}

  public static boolean isLow(Integer score) {
    return score != null && score < 30;
  }

  public static boolean needsReview(Integer score) {
    return score != null && score >= 30;
  }
}
```

`whenResult` hands the value that `riskScore` returned to each check, in the order you added them. The first check that returns `true` runs its branch, and Ratchet cancels the other one. Ratchet stores each check as a method reference, so keep the comparison inside a public static method like the ones in `RiskRules`.

`holdForReview` submits a job that starts in the `WAITING` state. The wait is a row in the database, so it holds no thread and survives restarts. `approve` and `reject` deliver a decision by signal key. Once it arrives, `afterReview` runs and reads the decision with `JobContext.current().signalPayload(SignalDecision.class)`.

Every wait needs a timeout. This job has one retry, so if two days pass with no decision, Ratchet runs `afterReview` with a `null` decision and the order is cancelled. With no retries, the timeout instead fails the job with a `SignalTimeoutException` and sends it to the dead-letter queue. [Human-in-the-Loop Jobs](./human-in-the-loop.md) covers signals in more depth.

Branch steps get no retries. The jobs that `whenResult`, `when`, `thenOnSuccess` and `thenOnFailure` create also get no backoff and no timeout, whatever you set on the builder. That is why `release` and `holdForReview` do nothing except submit more work. The work they submit has its own settings and its own idempotency key, so if a crash makes a branch step run again, the rerun finds the existing job and creates nothing new.

## How do I fan out work and continue when it's all done?

```java
import java.util.List;
import java.util.UUID;

import run.ratchet.api.JobHandle;
import run.ratchet.api.JobSchedulerService;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class Packing {

  @Inject JobSchedulerService scheduler;

  @Inject Orders orders;

  // One child job per line item, run in parallel, then one follow-up once they have all finished.
  public JobHandle packOrder(UUID orderId) {
    List<UUID> lineItems = orders.lineItems(orderId);
    return scheduler
        .enqueueBatch("pack-" + orderId)
        .withMaxRetries(3)
        .forEach(lineItems, lineItemId -> pickAndPack(lineItemId))
        .thenOnBatchSuccess(() -> notifyShipped(orderId))
        .thenOnBatchFailure(() -> escalate(orderId))
        .submit();
  }

  public void pickAndPack(UUID lineItemId) {
    orders.pickAndPack(lineItemId);
  }

  public void notifyShipped(UUID orderId) {
    orders.notifyShipped(orderId);
  }

  public void escalate(UUID orderId) {
    orders.escalate(orderId);
  }
}
```

`forEach` creates one child job per line item. The children run in parallel on any node with free workers, and each one gets the batch's three retries. Once every child has finished, Ratchet runs `notifyShipped` if they all succeeded, or `escalate` if any of them failed for good. Those two handlers don't take the batch's retry settings and run with no retries, so keep them as small as the branch steps above. A batch can also branch on its success rate or failure count; see [Batches](../concepts/batches.md).

## What do I need to run it?

- Java 17 or later.
- A Jakarta EE 10 or 11 server (WildFly, Open Liberty, Payara or GlassFish), Quarkus with the `ratchet-quarkus` extension, or the Spring Boot 3.5 or 4.1 starters.
- A database you already run: PostgreSQL, MySQL, Oracle, SQL Server 2022 or later, or MongoDB.
- Ratchet's tables in that database. SQL stores ship a plain SQL schema script you apply with your usual DDL tooling, and the MongoDB store creates its collections at startup. See [Installation](/getting-started/installation#database-schema-setup).
- A `ClassPolicy` that allows your application's packages. Ratchet won't start without one.

You don't need a broker or a separate server. Nodes coordinate through the database, so you scale out by starting another copy of your application. The examples use CDI annotations. The [Spring Boot](/deployment/spring-boot) and [Quarkus](/deployment/quarkus) guides cover setup for those runtimes.

## When is a workflow engine the better choice?

Ratchet fixes a workflow's shape when you call `submit()`, and each step is a job. A workflow engine treats the whole workflow as a function and makes that function durable. Some systems are better served by that model. The facts below were checked against each project's documentation in October 2026.

- **Long-running workflow logic written as code.** Temporal records an event history on the Temporal Service and replays your workflow code against it, which rebuilds the workflow's local state after a restart. A workflow can run for years. In return, workflow code must be deterministic, and changing it calls for versioning such as `Workflow.getVersion()`.
- **Workers in several languages.** Temporal has SDKs for many languages, including Go, Java, Python, TypeScript and .NET. Ratchet is Java only.
- **A managed service.** Temporal Cloud and Restate Cloud run the server for you.
- **Checkpoints committed with your own writes.** DBOS Transact for Java is a library, like Ratchet, with no separate server. Its steps can commit your database write and the step's checkpoint in one transaction. It stores checkpoints in Postgres.

| | Where your code runs | What else you operate | License |
|---|---|---|---|
| Ratchet | Inside your Java application | Nothing beyond a PostgreSQL, MySQL, Oracle, SQL Server or MongoDB database | Apache-2.0 |
| [DBOS Transact for Java](https://github.com/dbos-inc/dbos-transact-java) | Inside your Java application | Nothing beyond a Postgres database. The Conductor control plane is optional | MIT |
| [Temporal](https://docs.temporal.io/) | Workers inside your application | The Temporal Service, backed by Cassandra, MySQL or PostgreSQL plus a visibility store, or Temporal Cloud | Server MIT, Java SDK Apache-2.0 |
| [Restate](https://docs.restate.dev/) | Handlers you deploy as services, which the Restate Server calls | The Restate Server, a single binary that can also run as a cluster, or Restate Cloud | Server BSL 1.1, Java SDK MIT |

Ratchet fits best when your workflows are a known sequence of steps and you would rather not run another service to get retries, schedules, batches and workflows in Java.

## Honest scope

- **At-least-once, never exactly-once.** A step can run more than once. Make every step safe to repeat.
- **Branch steps and batch handlers don't inherit settings.** Jobs created by `whenResult`, `when`, `thenOnSuccess`, `thenOnFailure`, `thenOnBatchSuccess` and `thenOnBatchFailure` run with no retries and no timeout. Keep them small, or have them submit work that carries its own settings.
- **Idempotency keys are permanent.** A key that started a workflow never starts another, even after that workflow failed. Use a new key to try again.
- **The declared shape is fixed at submit.** You declare steps and branches before calling `submit()`, and a workflow can't add steps to itself later. A step can still start follow-on work, as `release` does when it starts the fulfilment chain.
- **No values passed along a chain.** Only branch checks see a step's return value. Pass IDs, and keep state in your own tables.
- **Stored steps name a class, a method and its arguments.** Renaming a job class or method can break steps that are already queued. Drain the queue first, or keep those names stable across releases.
- **Java only.** Every worker runs your Java code.

## Related

- [Workflows](../concepts/workflows.md): chains, branch conditions and how Ratchet stores them
- [Human-in-the-Loop Jobs](./human-in-the-loop.md): signals, decisions and timeouts
- [Durable LLM & Agent Workflows](./durable-llm-workflows.md): the same building blocks applied to agent pipelines
- [Batches](../concepts/batches.md): fan-out, progress and batch conditions
- [Clustering](../concepts/clustering.md): orphan recovery and at-least-once delivery
- [Comparison overview](../comparison/overview.md): Ratchet next to other Java schedulers
