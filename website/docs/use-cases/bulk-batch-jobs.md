---
title: Bulk & Batch Jobs
description: Fan thousands of items out as one tracked batch, watch aggregate progress as it runs, and branch on the success rate when it finishes -- without writing the bookkeeping yourself.
---

# Bulk & Batch Jobs

Someone uploads an album of four thousand photos. Every one of them needs the same treatment: a resized copy, a thumbnail, a virus scan. The work is embarrassingly parallel and none of it should hold up the upload response, so it belongs in the background. The hard part was never running four thousand jobs. It is knowing, at any moment, how many finished, how many failed, and what to do when the album is done.

Roll that yourself and you end up writing the same ledger every time. A counter row. A careful increment after each item so two workers do not clobber each other. A check after every update for "are we there yet." A separate pass to decide whether 4,000-of-4,000 is a success or whether the 60 corrupt files mean someone should look before the album goes live. It is all plumbing, and it is the kind of plumbing that leaks under concurrency.

A batch is Ratchet's name for that ledger. You hand it the items and the work; it enqueues one child job each, counts completions and failures as they land, and hands every progress hook an immutable snapshot. When the batch finishes, you branch on what actually happened.

::: tip Verified
The Java on this page compiles against `ratchet-api` `0.1.2-SNAPSHOT`. It shows real API usage, not pseudocode. The running app needs a Jakarta EE server and a store that advertises the `BatchStore` capability.
:::

## Fan out, track progress, branch on the result

```java
import java.util.List;
import java.util.UUID;

import run.ratchet.api.JobHandle;
import run.ratchet.api.JobSchedulerService;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;

@ApplicationScoped
public class ImageSetService {

  @Inject JobSchedulerService scheduler;

  @Inject AlbumProgress progress;

  @Transactional
  public JobHandle process(UUID albumId, List<String> imageKeys) {
    return scheduler
        .enqueueBatch("album-" + albumId)
        .forEach(imageKeys, key -> renderDerivatives(key))
        .onProgress(ctx -> progress.record(albumId, ctx))
        .thenWhenFailureCount(25, () -> quarantineAlbum(albumId))
        .thenWhenSuccessRate(1.0, () -> publishAlbum(albumId))
        .submit();
  }

  void renderDerivatives(String imageKey) { /* resize, thumbnail, virus-scan */ }

  void quarantineAlbum(UUID albumId) { /* too many bad files; hold for review */ }

  void publishAlbum(UUID albumId) { /* flip the album to visible */ }
}
```

`forEach` enqueues one child job per key. Each runs `renderDerivatives` on a worker, independently, with the store's retry rules behind it. The parent job is the batch itself: it does no image work, it owns the count.

The two `thenWhen` lines are the part you would otherwise hand-roll. `thenWhenFailureCount(25, ...)` schedules `quarantineAlbum` if twenty-five children fail, so a bad upload stops short of publishing. `thenWhenSuccessRate(1.0, ...)` runs `publishAlbum` only when every child succeeded. They are conditions on the final tally, evaluated once, by the batch, not by you polling a counter.

As with every Ratchet job, the lambdas are method references on this CDI bean: only the argument (`albumId`, an image key) is serialized, and the bean is resolved again from CDI when the child runs.

## The progress snapshot

The `onProgress` hook fires after each child completes, and it receives a `BatchContext`: a small immutable record of where the batch stands.

```java
import java.util.UUID;

import run.ratchet.api.BatchContext;

import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class AlbumProgress {

  // Persist a row the UI can poll: "3818 of 4000 done, 60 failed".
  public void record(UUID albumId, BatchContext ctx) {
    int pct = ctx.percentDone();        // 0..100, rounds down
    int ok = ctx.completedItems();
    int failed = ctx.failedItems();
    int total = ctx.totalItems();
    boolean done = ctx.isComplete();    // completed + failed >= total
    double rate = ctx.successRate();    // completed / processed, 0.0..1.0
    // ... write that row somewhere the album page can read it.
  }
}
```

`BatchContext` is computed, not stored by you: `percentDone`, `successRate`, and `isComplete` are derived from the three counts the batch maintains. The hook is where you turn those numbers into something a user can see, a progress bar, a "182 left" badge, a metric, without ever touching the counter that produced them.

## Streaming the items instead of listing them

`forEach` takes a `Collection`, which means the whole list of keys sits in memory at submit time. Four thousand keys is fine. Four million is not. For a dataset too large to hold at once, `streamingBatch` consumes a `Stream` in chunks and persists chunk boundaries as it goes, so a restart picks up where it left off instead of starting over.

```java
public JobHandle reindex(UUID albumId, java.util.stream.Stream<String> imageKeys) {
  return scheduler
      .<String>streamingBatch("reindex-" + albumId)
      .fromStream(imageKeys)
      .process(key -> renderDerivatives(key))
      .withChunkSize(500)
      .onBatchProgress(ctx -> progress.record(albumId, ctx))
      .thenOnBatchSuccess(() -> publishAlbum(albumId))
      .start();
}
```

Same counting, same completion branches; the difference is that the items arrive 500 at a time off a stream you never fully materialize. Note the two progress hooks a streaming batch offers: `onProgress` watches the stream drain locally and is not persisted, while `onBatchProgress` reports execution progress and survives handoff to another node. Reach for `onBatchProgress` when the number has to be right after a restart.

## Honest scope

- Batches are an **optional store capability**. A store that does not advertise `BatchStore` throws `UnsupportedOperationException` from `enqueueBatch` and `streamingBatch`, by design: it refuses to persist a parent job whose children it cannot track, rather than silently dropping the count. The bundled MySQL, PostgreSQL, Oracle, SQL Server, and MongoDB stores all advertise it.
- The items and the per-item action must be `Serializable`; the action is stored and replayed, so capture ids and keys, not live service handles.
- Children are independent and run in whatever order workers claim them. A batch counts completions and failures; it does not sequence one item after another. If item B depends on item A, that is a [workflow](../concepts/workflows.md), not a batch.
- `submit()` runs with the Jakarta transaction attribute `REQUIRED`, so the batch is created inside the caller's transaction. The children become eligible when that transaction commits, then run on the next poll, not the same instant.

## Why reach for a batch

The work was always going to be a few thousand background jobs. What a batch saves you is the ledger around them: the concurrency-safe counting, the "is it finished" check, and the decision about what a partial result means. Those land as `completedItems` and `failedItems` on a snapshot, `isComplete` instead of a polling loop, and `thenWhenSuccessRate` instead of an after-the-fact audit. You describe the items, the work, and the finish conditions; the count is the store's problem.

## Next steps

- [Batches](../concepts/batches.md) -- the model behind parent jobs, child jobs, and aggregate state
- [Workflows](../concepts/workflows.md) -- when items must run in order, or a batch must lead into the next step
- [BatchBuilder](../api-reference/batch-builder.md) -- every builder method, including the `thenWhen` conditions
- [BatchContext](../api-reference/batch-context.md) -- the progress record and its derived accessors
- [Quickstart](../getting-started/quickstart.md) -- get a first job running
