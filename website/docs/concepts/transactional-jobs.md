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

## Job context

`JobContext.checkpoint(...)` and `requestContinuation(...)` throw `IllegalStateException` for transactional jobs. `lastCheckpoint()` returns empty. Progress remains available and is flushed outside the body's transaction, so it can be visible while the body runs.

## Connections and timeouts

Each running transactional job holds a pooled connection for its whole body. Size the pool for those connections plus heartbeats, completion follow-ups and other reads. Checking whether the attempt is current can need another connection. Keep bodies short and leave pool capacity for the scheduler.

The container's or transaction manager's default transaction timeout applies; the annotation does not set a timeout. Configure job timeouts and transaction timeouts together so the body has time to finish.

## Transaction boundaries inside the body

Submission rejects direct transaction annotations on the method or class with `REQUIRES_NEW`, `NOT_SUPPORTED` or `NEVER`, because they leave the job's transaction. Nested `REQUIRES_NEW` calls inside the body cannot be detected and their writes commit independently.

On MySQL, planning completion for workflow jobs takes shared locks to read current rows. Concurrent completions can deadlock while upgrading those locks; a transient conflict rolls back and retries the entire transaction, including the body.
