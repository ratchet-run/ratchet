---
title: Human-in-the-Loop Jobs
description: Park a job in a waiting state until a person approves it, then resume exactly where it left off -- holding no thread, surviving restarts, and timing out on its own if nobody answers.
---

# Human-in-the-Loop Jobs

A refund over five hundred dollars should not go out until a person says so. The logic is ready the moment the request arrives: look up the order, check the amount, move the money. What is missing is a decision, and that decision might come in ten seconds or two days, from a button in an admin console, a reply in Slack, or a callback from a finance system.

The clumsy way to wait is to not wait at all: write a `refund_pending` row, drop the in-progress work on the floor, and rebuild it later when the approval shows up. Now the logic lives in two places, the "resume" path drifts from the "start" path, and a half-decided refund is a row somebody has to reconcile by hand. The other clumsy way is to actually block a thread for two days, which is a thread you do not get back.

Ratchet gives the job a waiting state instead. It parks in `WAITING`, holds no thread, and survives a restart as a row in the database. An approval delivers a signal that moves it back to runnable, and the same job body that was about to run the refund now runs it, reading the approver's decision as it goes.

The [durable LLM workflows](./durable-llm-workflows.md#pause-for-a-human-then-resume) page reaches for this same mechanism to gate an agent's actions. This page is the mechanism itself: how a job waits, how a decision reaches it, and what happens when nobody decides.

::: tip Verified
The Java on this page compiles against `ratchet-api` `0.1.2-SNAPSHOT`. It shows real API usage, not pseudocode. The running app needs a Jakarta EE server and a store that advertises the `SignalStore` capability.
:::

## Park, decide, resume

```java
import java.math.BigDecimal;
import java.time.Duration;
import java.util.UUID;

import run.ratchet.api.JobContext;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.SignalDecision;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;

@ApplicationScoped
public class RefundApproval {

  @Inject JobSchedulerService scheduler;

  @Inject RefundStore refunds;

  // 1. The refund job is created already waiting. It will not run until signaled.
  @Transactional
  public UUID requestRefund(UUID refundId) {
    JobHandle handle =
        scheduler
            .enqueue(() -> settleRefund(refundId))
            .awaitSignal("refund-" + refundId, Duration.ofDays(2))
            .submit();
    refunds.recordPendingApproval(refundId, handle.id());
    return handle.id();
  }

  // 2. The approver's action delivers a structured decision to that exact job.
  public boolean approve(UUID jobId, String approver) {
    return scheduler.deliverSignal(jobId, SignalDecision.approved(approver)) == 1;
  }

  public boolean reject(UUID jobId, String reason) {
    return scheduler.deliverSignal(jobId, SignalDecision.rejected(null, reason)) == 1;
  }

  // 3. Once unblocked, the job body reads the decision and acts on it.
  void settleRefund(UUID refundId) {
    SignalDecision decision = JobContext.current().signalPayload(SignalDecision.class);
    if (decision == null || decision.isRejected()) {
      String why = decision == null ? "approval timed out" : decision.rejectionReason();
      refunds.markDenied(refundId, why);
      return;
    }
    String approver = decision.payload(String.class);
    BigDecimal amount = refunds.amountOf(refundId);
    refunds.payOut(refundId, amount, approver);
  }
}
```

`awaitSignal(signalKey, timeout)` is what marks the builder, so `submit()` persists the job in `WAITING` rather than `PENDING`. No worker claims it. It is a durable row holding a place in line, costing nothing but the row, until something wakes it.

What wakes it is a decision, and the decision is structured. `SignalDecision.approved(approver)` and `SignalDecision.rejected(null, reason)` both carry an outcome the scheduler records: a rejection has to include a reason, an approval must not. The job reads that decision back with `signalPayload(SignalDecision.class)`, and the `outcome` and `rejectionReason` come through typed, exactly as sent. The inner payload is the soft spot. The `approver` string survives only in its JSON-native form, so you ask for `String.class`, not for some class of your own. Note too that delivering a decision unblocks the job whichever way it points; paying out an approval or recording a denial is the job body's job, which is why all of it sits in one method.

Delivery is also idempotent, which matters more than it sounds. `deliverSignal(jobId, decision)` returns `1` when it moved a waiting job and `0` when it didn't, so a double-clicked Approve button does no harm: the first call unblocks the job, the second finds it already gone from `WAITING` and reports `0`. The broadcast form, `deliverSignal(signalKey, decision)`, wakes every job waiting on that key and tells you how many it moved. Reach for the `jobId` form to approve one thing and the `signalKey` form to release a whole group at once.

## When nobody decides

A wait cannot be open-ended, which is why `awaitSignal` requires a timeout. A scanner running on the poll tick looks for waiting jobs whose deadline has passed and resolves them so they never sit forever.

What "resolve" means depends on the job's retry budget:

- **No retries (the default).** The wait times out into a permanent failure. The job moves to `FAILED` with a `SignalTimeoutException`, fires a `JobSignalTimedOutEvent`, and travels the normal failure path, including the dead-letter queue, so a refund nobody approved becomes a visible, inspectable failure rather than a silent stall.
- **Retries configured.** A timeout reschedules the job to actually run, without a delivered decision. That is the case the `decision == null` branch above is written for: the body wakes with no approval and denies the refund rather than paying it out. Treat `null` as "no one said yes."

Either way the timeout is the job's own concern. You do not run a reaper, and a process restart mid-wait changes nothing, because the deadline lives on the row, not in memory.

## Honest scope

- Signals are an **optional store capability**. A store that does not advertise `SignalStore` throws `UnsupportedOperationException` from the submission path, refusing to create a `WAITING` job it could neither signal nor time out, rather than stranding it forever. The bundled MySQL, PostgreSQL, Oracle, SQL Server, and MongoDB stores all advertise it.
- A signal payload is **not a typed-bean carrier**. It round-trips as JSON, so it returns as a `String`, a `Number` (a `BigDecimal` under the default serializer), a `Boolean`, a `List`, or a `Map`. Pass an id or an amount and look the rest up; do not try to ship a domain object through the signal and get the same class back.
- `awaitSignal` waits for *a* signal, not a conversation. For multi-step back-and-forth (approve, then later confirm shipment), model each wait as its own job in a [workflow](../concepts/workflows.md), chained on the previous one's result.
- The decision's outcome is **scheduler-visible metadata for audit and events, not enforcement**. An approved decision still just unblocks the job; nothing pays out until your body decides to. Approval and rejection are signals to your code, not actions the scheduler takes on your behalf.

## Why park it in Ratchet

The alternative is two code paths and a reconciliation problem: one path that starts the refund, a `pending` row that throws away the in-progress work, and a second path that has to reconstruct it when approval lands, kept in sync with the first by hand. A waiting job collapses that into one. The work pauses in place and resumes in the same method, the decision arrives as data the body reads, and the case nobody wants to handle, the approval that never comes, is handled by the timeout you were required to set. One method, one row, and a deadline that cannot be forgotten.

## Next steps

- [Job lifecycle](../concepts/job-lifecycle.md) -- where `WAITING` sits among a job's states
- [Workflows](../concepts/workflows.md) -- chaining waits for multi-step approvals
- [JobBuilder](../api-reference/job-builder.md) -- `awaitSignal` and the rest of the builder
- [JobSchedulerService](../api-reference/job-scheduler-service.md) -- the `deliverSignal` delivery contract
- [Durable LLM & agent workflows](./durable-llm-workflows.md) -- the same pause-for-a-human, applied to agents
