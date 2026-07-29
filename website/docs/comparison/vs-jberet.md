---
sidebar_position: 5
title: Ratchet vs jBeret (Jakarta Batch / JSR-352)
description: How Ratchet differs from jBeret and the Jakarta Batch spec, when to pick each, and where the spec itself is the deciding factor.
---

# Ratchet vs jBeret (Jakarta Batch / JSR-352)

[jBeret](https://jberet.gitbook.io/jberet-user-guide) is the reference implementation of Jakarta Batch (formerly JSR-352). It ships inside WildFly and is the default batch runtime there. If your project's contract says "we use the Jakarta Batch spec," jBeret is the implementation you are using.

Ratchet is not a Jakarta Batch implementation, and it does not claim to be one. If you came here looking for a spec-compliant alternative, this page will save you time: pick jBeret.

If you came here trying to figure out whether you actually need the spec, keep reading.

## TL;DR

| Pick **jBeret** if                                       | Pick **Ratchet** if                                       |
|----------------------------------------------------------|-----------------------------------------------------------|
| You need Jakarta Batch (JSR-352) spec compliance         | You want to schedule background jobs in a Jakarta EE app  |
| Your team writes batch jobs in JSL (the XML job format)  | You want a lambda-based API, not XML                      |
| You are deploying to WildFly and want the built-in option | You need cron, delays, and workflow chains as primitives |
| Your auditors or procurement want a ratified Java spec   | You need a circuit breaker and dead letter queue built in |

## What the spec actually buys you

JSR-352 / Jakarta Batch defines a portable batch programming model. The core abstractions are:

- A job is described in JSL (Job Specification Language), which is XML.
- A job has steps. A step has a `Batchlet` (a single unit of work) or a chunk-oriented pipeline of `ItemReader`, `ItemProcessor`, `ItemWriter`.
- The runtime manages execution state, checkpoints, restart, and step-level partitioning.
- `JobOperator` is the API for starting, stopping, restarting, and inspecting jobs.

If you write your job against the spec, you can in theory deploy it on jBeret, IBM JBatch (WebSphere), or any other JSR-352 implementation, and it should run. That portability is the entire point of the spec.

That said, the spec is heavyweight. The XML format takes some getting used to. The chunk-oriented model is great for large dataset processing and awkward for "run this method in the background." And in practice, most teams that adopt Jakarta Batch are already committed to a specific application server, so the cross-runtime portability is more theoretical than operational.

## Programming model

**jBeret** wants you to write a JSL file:

```xml
<job id="processOrders" xmlns="http://xmlns.jcp.org/xml/ns/javaee" version="1.0">
  <step id="processOrdersStep">
    <chunk item-count="100">
      <reader ref="orderReader"/>
      <processor ref="orderProcessor"/>
      <writer ref="orderWriter"/>
    </chunk>
  </step>
</job>
```

…and provide CDI beans for the reader, processor, and writer. Then you start the job through `JobOperator`:

```java
@Inject
JobOperator jobOperator;

public long startProcessing() {
    return jobOperator.start("processOrders", new Properties());
}
```

This is a lot of ceremony for "process 1000 orders." It pays off when the dataset is large enough that you actually need the spec's checkpoint, restart, and partitioning semantics. It is overkill for most fire-and-forget background work.

**Ratchet** takes a method reference:

```java
scheduler.enqueueNow(() -> orderService.processBatch(orderIds));
```

For batch-style processing, Ratchet has its own batch primitives:

```java
scheduler.enqueueBatch("process-orders")
    .forEach(orderIds, id -> orderService.processOrder(id))
    .submit();
```

These do not implement JSR-352. They cover the cases where you need a parallel batch with progress tracking, not the full spec.

## DI and container fit

Both libraries are CDI-friendly. Both work inside Jakarta EE application servers.

jBeret is the deeper integration on WildFly. It is part of the platform there, started automatically with the application server, and managed through standard tools (`jboss-cli`, the WildFly console, etc.). On other servers (Payara, Open Liberty, GlassFish), you can usually get jBeret running, but it is a separate library rather than a platform service. Open Liberty has its own JSR-352 implementation. Payara uses jBeret. GlassFish uses jBeret.

Ratchet runs the same way on all four. The integration story is uniform: add the dependencies, install a `ClassPolicy`, configure your store. There is no "depends on what container you are on" wrinkle.

## Persistence

jBeret persists job state through its own JPA model into a configured `DataSource`. WildFly's default is the built-in H2 (which you absolutely should swap out for production), pointing at the same database your application uses. The schema is jBeret's, the tables are jBeret's, and migrations are managed by jBeret at startup.

Ratchet persists through its pluggable `JobStore` SPI. The default schema is your problem to apply (DDL ships as plain SQL), or you opt in to `SchemaMigrationLifecycleHook` for auto-apply. The stores supported out of the box are MySQL, PostgreSQL, Oracle, SQL Server, and MongoDB. jBeret is SQL-only.

If you have a Mongo-based stack, jBeret is not the answer.

## Workflows

jBeret supports step flow within a job (sequential steps, conditional `<next on="...">` transitions, decision elements, splits and flows for parallel execution). The orchestration model is expressive for multi-step batch jobs. But it is all expressed in XML, and the unit of expression is "step within a job," not "another job submitted from this job."

Ratchet expresses workflows as separate persisted jobs chained at submission time:

```java
scheduler.enqueue(() -> validateOrder(orderId))
    .thenOnSuccess(() -> fulfillOrder(orderId))
    .thenOnFailure(() -> notifyFailure(orderId))
    .submit();
```

Different shape, different idiom. Ratchet's model is better when each step might run on a different worker or at a different time. jBeret's model is better when the steps are all part of one logical batch run.

## What jBeret does better

- **Spec compliance.** If you need JSR-352, jBeret is the answer. Ratchet is not.
- **Mid-iteration crash recovery.** jBeret checkpoints between chunks *during* the source iteration. If the JVM dies at row 87,000,000, jBeret resumes iterating the source from row 87,000,000. Ratchet's streaming batch persists every item as a child job before any work runs, so a crash after the parent has been built recovers per-item (more granular than per-chunk); but if the crash hits *during* the submitter's source iteration, items beyond the crash point are gone. For typical ETL where source iteration is fast and processing is slow, this never matters. For workloads where reading the source is itself a long process, jBeret's model is the one that recovers fully.
- **WildFly integration.** It is part of the platform there. No setup beyond adding job artifacts.
- **Source-level partition distribution.** Jakarta Batch's partitioning model lets each worker JVM iterate its own slice of the input data, coordinated by a master step. That matters when the source iteration itself is the bottleneck (slow reads, very large cursors). Ratchet's streaming batch persists each item as a child job on the submitter and any worker can claim them, so parallel execution across JVMs is fine, but the *source iteration* still happens on the submitting JVM, not distributed.
- **Item-level skip and retry policies that survive restarts.** Jakarta Batch lets you say "skip up to N of these specific exceptions" or "retry on these and keep state across restarts." Ratchet retries each child job, but it doesn't have a built-in batch-level skip-N-exceptions policy.
- **Maturity and standards backing.** jBeret has been around since JSR-352 was finalized in 2013.

What jBeret does *not* hold over Ratchet (despite the assumption that "Jakarta Batch is the partitioning story"): **parallel execution of a single batch across multiple JVMs.** Ratchet's streaming batch persists every item as a separately-claimable child job, so N workers across N JVMs naturally process N items in parallel. The parent batch tracks completion and fires success/failure callbacks when all children finish. jBeret's partitioning model is more structured (source-level distribution, master-step coordination), but the basic outcome of "linear parallelism across worker JVMs" is the same.

## What Ratchet does better

- **API ergonomics for the common case.** Lambda-based submission beats writing JSL for "run this method in the background."
- **Cron and delay scheduling.** Jakarta Batch has no native concept of "fire this job at 2am every night." You bolt on a timer EJB or `@Schedule`.
- **Multi-store including MongoDB.** Jakarta Batch is SQL-only.
- **Built-in resilience.** Circuit breaker, retry policies with an SPI, dead letter queue, signal-waiting jobs, worker tag affinity. None of these are part of Jakarta Batch.
- **Operator-level crash recovery.** If jBeret's JobOperator crashes or its JVM dies mid-job, in-flight jobs are stranded in `STARTED` status in the database with no running coordinator. The spec has no automatic recovery mechanism, your application code has to notice the stranded jobs and call `JobOperator.restart()` for each one, and a JVM restart is often the only way to clear the executor pool. Ratchet's design treats the store as the single source of truth: workers claim jobs with a stale-timeout, an `OrphanRecoveryTimer` runs continuously and returns claimed-but-abandoned jobs to `PENDING` automatically, and a worker restart picks the work up from where the previous worker left it without any application-side intervention. The difference determines whether a JVM crash is "annoying" or "outage."
- **Caller-principal capture.** Ratchet captures the current platform principal at submission and exposes a `JobAuthorizationPolicy` SPI. Jakarta Batch has no equivalent.

## When to use which

Use jBeret if your team is committed to the Jakarta Batch spec, or if your job model is mostly chunked dataset processing with restart-from-checkpoint requirements. The spec exists because that problem is real and hard, and the spec captures decades of accumulated learning about how to do it correctly.

Use Ratchet for the rest of background work in a Jakarta EE app: recurring jobs, queued work, workflow chains, scheduled tasks. The spec was not designed for this case, and it shows when you try.

They can coexist in the same app: Jakarta Batch for nightly data warehouse loads and Ratchet for everything else. There is no conflict.
