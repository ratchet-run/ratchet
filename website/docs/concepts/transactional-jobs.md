---
sidebar_position: 6
title: Transactional Jobs
description: Commit a job's database writes and success record together
---

<!--
Copyright 2026 Ratchet Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Transactional Jobs

Add `@TransactionalJob` to a job method or its class to commit its database writes and its SUCCEEDED record in one transaction:

```java
import run.ratchet.api.TransactionalJob;

@TransactionalJob
public void applyPayment(String paymentId) {
  payments.apply(paymentId);
}
```

This guarantee covers writes through the **same transaction manager and datasource as Ratchet's store**. On Spring, this is the manager Ratchet selects for its store. On Micronaut, it is the datasource configured by `ratchet.jpa.datasource`. Writes through another datasource commit separately. HTTP calls, messages and other external effects also need their own idempotency strategy.

MySQL, PostgreSQL, Oracle and SQL Server support transactional completion. Submission on a store without that capability, including MongoDB, throws `UnsupportedOperationException`. A worker also rejects an annotated job if its store no longer supports the capability.

## Rollbacks and retries

The body runs inside the transaction that records success. If the body fails, or the completion fence finds that cancellation or another worker superseded the claim, the transaction rolls back, including the body's writes.

A transient store failure during completion retries the entire transaction, **including the body**, up to five times with short backoff. These inline retries do not spend a configured job retry attempt. After they are exhausted, ordinary job failure and retry policy applies. Deterministic errors use ordinary failure handling immediately.

If the transaction manager reports a commit failure after the body and completion finished, Ratchet reads the store to resolve the outcome. A committed result runs follow-ups and success callbacks; a rolled-back result uses ordinary failure handling without an inline rerun. If the outcome cannot be read, orphan recovery or restart resolves the job. Without [durable terminal events](../api-reference/event-system.md#delivery-guarantees), a terminal event may be lost when a commit acknowledgement is lost.

Ratchet counts an attempt as successful only when it sees the transaction commit. If the transaction manager returns without committing and without an error, the attempt fails with `IllegalStateException` ("rolled back without an error"), and ordinary job failure and retry policy applies.

The same rule applies when a custom [`ResilienceStrategy`](../advanced/circuit-breakers.md#implementing-a-custom-resiliencestrategy) catches a failure and returns a fallback value. For a transactional job, a fallback never counts as success. The attempt fails with the error that the body or the transaction raised. If the strategy returns without running the job, the attempt fails with `IllegalStateException` ("without a committed transaction").

## A body that leaves its transaction marked for rollback

Do not catch an exception from a nested transactional call and carry on. A nested call that joins the job's transaction, such as a `@Transactional` bean method, can mark the whole transaction for rollback when it throws, before the body sees the exception. After that, nothing in the transaction can commit, even if the body returns normally.

Which exceptions mark the transaction depends on the annotation on the nested method:

- Jakarta Transactions `@Transactional`: by the specification's default, `RuntimeException` and its subclasses mark it and checked exceptions do not. `rollbackOn` adds exceptions and `dontRollbackOn` exempts them.
- Spring `@Transactional`: by default, `RuntimeException` and `Error` mark it and checked exceptions do not. `rollbackFor` adds exceptions and `noRollbackFor` exempts them.
- Micronaut `@Transactional`: by default, every exception marks it, checked exceptions included. `noRollbackFor` exempts exceptions. Once `rollbackFor` is set, only the exceptions it lists mark it. Micronaut Data maps `jakarta.transaction.Transactional` onto this annotation, so on Micronaut the Jakarta annotation follows these rules, not the Jakarta default.

If the exception does not mark the transaction, the body can catch it and the job's transaction still commits.

Before Ratchet records success, it checks whether the transaction is marked for rollback. If it is, the attempt fails with `IllegalStateException` ("transaction is marked for rollback") and the body's writes roll back. Ordinary job failure and retry policy then applies, so a retry runs the body again. Ratchet makes this check itself, so the result does not depend on how the server or framework handles a transaction marked for rollback.

Do one of these instead:

- Let the exception propagate. The attempt fails, its writes roll back, and the job's retry policy decides whether it runs again.
- If the nested work may fail without failing the job, run it in its own transaction (`REQUIRES_NEW`). Its writes commit or roll back separately from the job's, and it holds a second connection while it runs.
- To end the job without a retry, throw an exception whose class is annotated with [`@DoNotRetry`](../api-reference/annotations.md#donotretry).

## Job context

`JobContext.checkpoint(...)`, `runInTransaction(...)` and `requestContinuation(...)` throw `IllegalStateException` for transactional jobs. `lastCheckpoint()` returns empty. Progress remains available and is flushed outside the body's transaction, so it can be visible while the body runs.

A transactional job commits all of its work at once. To commit a long job in steps, leave out the annotation and run each step with [`JobContext.runInTransaction`](../api-reference/job-context.md#saving-the-checkpoint-with-the-work). Each step's writes commit with its checkpoint, and a retry resumes after the last step that committed. The same limits apply: SQL stores only, and only writes through Ratchet's datasource are inside the step's transaction.

## Connections and timeouts

Each running transactional job holds a pooled connection for its whole body. Size the pool for those connections plus heartbeats, completion follow-ups and other reads. Checking whether the attempt is current can need another connection. Keep bodies short and leave pool capacity for the scheduler.

The container's or transaction manager's default transaction timeout applies; the annotation does not set a timeout. Configure job timeouts and transaction timeouts together so the body has time to finish.

## Transaction boundaries inside the body

Submission rejects direct transaction annotations on the method or class with `REQUIRES_NEW`, `NOT_SUPPORTED` or `NEVER`, because they leave the job's transaction. Nested `REQUIRES_NEW` calls inside the body cannot be detected and their writes commit independently.

On MySQL, planning completion for workflow jobs takes shared locks to read current rows. Concurrent completions can deadlock while upgrading those locks; a transient conflict rolls back and retries the entire transaction, including the body.
