---
sidebar_position: 4
title: Ratchet vs Spring Batch
description: Why Spring Batch and Ratchet are different tools for different problems, and when you actually need each.
---

# Ratchet vs Spring Batch

Most "Ratchet vs Spring Batch" comparisons end up apples-to-oranges. They're not in the same category. Spring Batch is a batch processing framework. Ratchet is a job scheduler. The overlap is real but narrow, and getting the distinction right matters before you pick one.

## TL;DR

| Pick **Spring Batch** if                                | Pick **Ratchet** if                                       |
|---------------------------------------------------------|-----------------------------------------------------------|
| You are processing millions of rows in a chunked pipeline | You are running background jobs in a Jakarta EE app      |
| You need restart-from-failed-chunk semantics            | You need cron, delays, and method-level scheduling        |
| You are already on Spring Boot                          | You want lambda-based submission, not job XML or `Tasklet` classes |
| You need item readers, processors, and writers          | You need workflows, retries, and resilience inline        |

## What Spring Batch actually is

Spring Batch is a framework for long-running batch jobs that process collections of items. The core abstractions are:

- `Job`: the unit of execution
- `Step`: a phase within a job
- `ItemReader`, `ItemProcessor`, `ItemWriter`: the read-transform-write pipeline inside a chunk-oriented step
- `JobRepository`: persistent storage for job and step metadata
- `JobLauncher`: the thing that actually starts a job

The classic use case is ETL: read 5 million rows from a database, transform them, write them somewhere else, in chunks of 1000, with checkpoint-and-restart if anything fails midway. Spring Batch is very good at that. It tracks where you were when the job died, restarts from the last committed chunk, handles skip and retry policies per item, and persists step-level metadata so you can resume across JVM restarts.

It is not designed for "send a welcome email after a user signs up" or "run this cleanup task every night at 2am." You can make it do those things, but the ceremony is high. Most people who do this also have Spring's `@Scheduled` running somewhere to fire the job, plus a `JobLauncher` configuration, plus the job XML or Java config, plus a `Tasklet` that wraps the actual method call. For one-off background work, that is a lot.

## What Ratchet actually is

Ratchet is a job scheduler. You pass a method reference, it gets persisted, a poller picks it up, and it runs. There is no item reader or item writer. There is no chunk size. There is no `JobRepository` in the Spring Batch sense (though Ratchet does persist jobs).

```java
scheduler.enqueueNow(() -> sendWelcomeEmail(userId));
```

That's the whole programming model for fire-and-forget. For recurring jobs, you annotate a method:

```java
@Recurring(cron = "0 0 2 * * ?", name = "Nightly Cleanup")
public void cleanupExpiredSessions() { /* ... */ }
```

For workflows, you chain submissions:

```java
scheduler.enqueue(() -> validateOrder(orderId))
    .thenOnSuccess(() -> fulfillOrder(orderId))
    .thenOnFailure(() -> notifyFailure(orderId))
    .submit();
```

This is the case Ratchet was built for. It is not the case Spring Batch was built for, which is exactly why those comparisons get confused.

## Where they actually overlap

The overlap is batch-style processing inside a Jakarta EE app. If you have a million invoices to process and you do not want to load them into memory, Ratchet has two relevant primitives:

```java
// In-memory batch: parallelize a known collection
scheduler.enqueueBatch("invoices")
    .forEach(invoiceIds, id -> processInvoice(id))
    .submit();

// Streaming batch: chunked iteration over a large dataset
scheduler.streamingBatch("monthly-report")
    .fromStream(invoiceRepository.streamUnprocessed())
    .process(this::generateReportEntry)
    .withChunkSize(1000)
    .start();
```

For a lot of real "process a big dataset" work, these are enough on their own. Chunked iteration, parallel execution across worker JVMs (every item is a persisted child job that any worker can claim), progress tracking, per-item retries, automatic recovery from worker crashes via the `OrphanRecoveryTimer`, persisted state so you can see what's been processed. Don't undersell that. The streaming batch primitive will handle millions of rows of straightforward "read, transform, write" without breaking a sweat.

The genuine cases where Spring Batch's model still wins are covered in the [What Spring Batch does better](#what-spring-batch-does-better) section below: mid-iteration crash recovery (the source read itself crashing), batch-wide skip counters, source-level partition distribution, and the per-item `ItemReadListener` / `ItemProcessListener` / `ItemWriteListener` hooks. For ETL outside those specific cases, Ratchet's primitives are a fair fit and you don't need to bring in a whole batch framework.

## DI and container fit

Spring Batch is part of the Spring portfolio. It assumes Spring. If your app is Spring Boot, it integrates without friction. If your app is Jakarta EE, you can technically run Spring Framework alongside your container, but you end up with two DI models in the same JVM and you fight every wiring decision twice.

Ratchet is the inverse. It is CDI-native, integrates with `@Inject`, captures `CallerPrincipal` through `Instance<SecurityContext>`, and is at home on WildFly, Payara, Open Liberty, and GlassFish. A Spring Boot starter is planned but does not exist yet.

## What Spring Batch does better

- **Mid-iteration crash recovery.** This one is narrower than the marketing suggests. Ratchet's streaming batch persists every item as its own `PENDING` child job *before* any work runs, so a crash after the parent has been built recovers cleanly: completed children stay done, claimed-but-orphaned children are reclaimed by the `OrphanRecoveryTimer` and re-run, pending children are picked up by surviving workers. That's effectively per-item checkpointing, more granular than Spring Batch's per-chunk checkpoint. Where Spring Batch behaves differently is *during* the source iteration itself. If the JVM dies mid-iteration, Spring Batch resumes iterating the source from the last committed offset; Ratchet doesn't have an equivalent because its iteration is a one-shot serial pass on the submitter. For typical ETL where source iteration is fast and processing is slow, this never matters. For workloads where reading the source is itself a long process, Spring Batch's model is the only one that recovers fully.
- **Batch-level skip counters with persisted state.** Spring Batch can say "skip up to 100 `ParseException`s across the whole batch, persist the count across restarts." Ratchet retries each child job per its `RetryPolicy`, but doesn't have a built-in batch-wide skip counter that aborts the batch when a threshold is hit. You can implement this in user code via a streaming batch's `onBatchProgress` hook, but it's a real primitive in Spring Batch that Ratchet doesn't ship.
- **Spring ecosystem fit.** If your app is already Spring Boot, `@EnableBatchProcessing` and you are most of the way there.
- **Maturity.** Spring Batch has been in production for over a decade. Ratchet is alpha.
- **Source-level partition distribution.** Spring Batch's remote partitioning lets each worker JVM iterate its own slice of the source data, which matters when the source iteration itself is expensive (slow reads, very large cursor windows). Ratchet iterates the source on the submitting JVM and persists each item as a child job, which then any worker can claim. For typical "DB query + transform" ETL the difference is invisible; for sources where the read itself is the bottleneck, Spring Batch's model is more elegant.

What Spring Batch does *not* hold over Ratchet (despite the common assumption): **parallel execution of a single batch across multiple JVMs.** Ratchet's streaming batch persists every item as a separately-claimable child job, so N workers across N JVMs naturally process N items in parallel. The parent batch tracks completion and fires `thenOnBatchSuccess`/`thenOnBatchFailure` when all children finish. The mechanism differs from Spring Batch's master/worker partitioning, but the outcome (linear parallelism with worker count) is the same.

The other thing that comes up in production but doesn't show up in feature lists: **automatic recovery from a coordinator crash.** Spring Batch's `JobOperator` is the entry point for starting, stopping, and restarting jobs, and if its JVM dies mid-job, the in-flight job is stranded in `STARTED` status in the `JobRepository` with no coordinator running it. There's no automatic recovery; application code has to detect stranded jobs and call `JobOperator.restart()` for each one, and in practice this is the kind of failure where teams just bounce the JVM. Ratchet's design is the opposite: workers claim jobs with a stale-timeout, the `OrphanRecoveryTimer` continuously reclaims abandoned claims back to `PENDING`, and a worker restart picks up the work without any application intervention. For a piece of software whose job is to be reliable, that's a meaningful difference.

## When to use which

If your problem is **"my source iteration is itself slow and I need to recover mid-iteration after a crash,"** or **"I need a batch-wide skip counter that aborts after N specific exceptions,"** or **"I need source-level partition distribution across worker JVMs,"** use Spring Batch. Or use jBeret if you are on Jakarta EE and need the same model without Spring. These are the cases Spring Batch was purpose-built for, and they're real.

If your problem is **"I have a big dataset, the source iteration is fast, and I need crash recovery for the work itself,"** Ratchet's streaming batch handles it. Every item is a persisted child job; a crash mid-batch picks right back up where it stopped because completed children stay done, orphaned in-flight children get reclaimed, and pending children continue normally. That's more granular than chunk-level checkpointing.

Notably, **if you just need parallel execution of a big batch across multiple JVMs**, you don't need Spring Batch. Ratchet's streaming batch persists each item as its own child job, and any worker on any JVM can claim and process them in parallel. That part of the partitioning story isn't unique to Spring Batch.

If your problem is **"I have a big dataset and I just need to chunk-process it with retries,"** Ratchet's streaming batch is a fair fit. Bringing in Spring Batch for this case is overkill, and the same observation drives the "Spring Batch vs JobRunr for a 10M row ETL job" benchmarks people have been running lately. The threshold for "you actually need Spring Batch" is higher than the marketing material suggests, and a chunked-iterator primitive plus retries covers a lot of real ETL.

If your problem is **"I need to run background jobs, retry them, chain them, and schedule them on a cron,"** use Ratchet. Spring Batch can be coerced into doing this but it is the wrong tool, and the ceremony will show.

If your problem is **"I need both,"** they coexist. Spring Batch jobs and Ratchet jobs do not conflict, and there is no reason a single app cannot use both. We have seen apps use Ratchet for the recurring and event-driven jobs and Spring Batch for the nightly ETL.
