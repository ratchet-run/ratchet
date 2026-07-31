---
title: Offload Work After a Request
description: Persist follow-up work in the same transaction as your business write, return the response immediately, and let Ratchet run the side effects on a worker, durably and with retries.
---

# Offload Work After a Request

A checkout request has one job: take the order and say yes. The receipt email, the inventory hold, the nudge to the warehouse: none of that has to finish before the customer sees a confirmation. Do it inline anyway and you have tied the success of the order to the mood of your SMTP provider. The email times out, the whole request 500s, and the customer is left wondering whether they just bought anything.

The usual escape hatch is a thread pool, or a `@Async` method, or handing the work to a message broker. The thread pool loses everything in flight when the process restarts. The broker is a second system to run, and now you have the dual-write problem: you saved the order to your database and published a message to the broker, and there is a window where one happened and the other did not.

Ratchet closes that window. You enqueue the follow-up work inside the same transaction that writes the order, so the job and the order row commit together. The request returns as soon as the row is durable. The work runs later, on a worker, with retries, and it cannot get lost, because it was written to the same database as the thing it follows up on.

::: tip Verified
The Java on this page compiles against `ratchet-api` `0.3.1-SNAPSHOT`. It shows real API usage, not pseudocode. The running app needs a Jakarta EE server and a configured store.
:::

## Enqueue inside the transaction, return now

```java
import java.time.Duration;
import java.util.UUID;

import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobSchedulerService;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;

@ApplicationScoped
public class CheckoutService {

  @Inject JobSchedulerService scheduler;

  @Inject OrderRepository orders;

  @Transactional
  public OrderConfirmation placeOrder(Cart cart) {
    Order order = orders.save(Order.from(cart));

    scheduler
        .enqueue(() -> sendReceipt(order.id()))
        .withIdempotencyKey("receipt-" + order.id())
        .withTags("email", "receipt")
        .submit();

    scheduler
        .enqueue(() -> reserveInventory(order.id()))
        .withMaxRetries(5)
        .withBackoff(BackoffPolicy.EXPONENTIAL, Duration.ofSeconds(2))
        .withTags("inventory")
        .submit();

    scheduler
        .enqueue(() -> notifyFulfillment(order.id()))
        .withPriority(JobPriority.HIGH)
        .withTags("fulfillment")
        .submit();

    return OrderConfirmation.accepted(order.id());
  }

  void sendReceipt(UUID orderId) { /* render and send the email */ }

  void reserveInventory(UUID orderId) { /* decrement stock, place the hold */ }

  void notifyFulfillment(UUID orderId) { /* hand off to the warehouse */ }
}
```

The handler returns `OrderConfirmation` the moment the order row is committed. Three jobs are now sitting in the database, due immediately, and a worker picks them up on the next poll. The customer never waited on any of them.

The lambdas are method references on this CDI bean. Ratchet serializes only the argument (`order.id()`) and resolves the bean again from CDI when the job runs, so anything injected into `CheckoutService` is live at execution time, not captured at submission.

## Why "before returning" is a real guarantee

The reason this is safe and a thread pool is not comes down to one line in the API contract: `submit()` runs with the Jakarta transaction attribute `REQUIRED`. It persists the job inside whatever transaction is already open: here, the one `@Transactional` started on `placeOrder`.

So the order row and the three job rows are a single commit, with only two ways it can land:

- The transaction commits. The order is saved **and** all three jobs are guaranteed to run.
- Something rolls back: a constraint violation, a thrown exception, a crash mid-method. The order is not saved **and** no jobs exist. Nothing was half-done.

There is no moment where the order persisted but the receipt job evaporated, and none where a job is queued for an order that was rolled back. That is the dual-write problem, and Ratchet sidesteps it by making the queue part of your database instead of a system beside it.

## Idempotency: the double-submit problem

Users double-click. Load balancers retry. A flaky network makes the browser resend a POST that actually succeeded. Without a guard, each of those creates a second order, and a second receipt.

`withIdempotencyKey` is the guard. The key is unique forever, enforced by a database constraint. Submit a job with a key that already exists and the duplicate is silently dropped, not run again.

```java
scheduler
    .enqueue(() -> sendReceipt(orderId))
    .withIdempotencyKey("receipt-" + orderId)
    .submit();
```

Key the job on something stable from the request: an order id, a payment intent id, a webhook delivery id. Now "send the receipt for order 8412" can be attempted any number of times and the email goes out once.

## What happens when the work fails

A worker runs the job, not the request thread, so failure has somewhere to go that is not the customer's screen. `reserveInventory` above gets five attempts with exponential backoff. If the inventory service is briefly down, the retries ride it out. If it stays down past the retry budget, the job lands in the dead-letter queue with its final error attached, ready to inspect and replay. The order was committed and answered long before any of this, so a stuck side effect stays a side effect.

## Honest scope

- The work runs **after** the response, not during it. If the caller needs the result to answer the request, like a computed total or a validation verdict, that is synchronous work and belongs inline, not in a job.
- The three jobs here are independent and run in whatever order workers pick them up. If step B must follow step A, chain them with `then` / `thenOnSuccess` or model it as a [workflow](../concepts/workflows.md); do not rely on submission order.
- Jobs become eligible the instant the transaction commits, but they run on the next poll cycle, not the same millisecond. This is fast background work, not an inline call.

## Why run it through Ratchet

A thread pool is simpler until the process restarts and the in-flight work is gone. A broker is durable but it is a second datastore to run, secure, and keep in sync with your database. Ratchet's pitch is the boring middle: the durability of a persisted queue with none of the dual-write risk, because the queue lives in the database you already committed the order to. The follow-up work is a CDI bean, enqueued in the same transaction, and it survives a restart because it was written down before the response was sent.

## Next steps

- [Job lifecycle](../concepts/job-lifecycle.md) -- how a submitted job moves to running, succeeded, or dead-lettered
- [Persistence](../concepts/persistence.md) -- why the job and your data share one commit
- [Retry strategies](../concepts/retry-strategies.md) -- tune backoff and attempts per job
- [JobBuilder](../api-reference/job-builder.md) -- every option on the builder, including idempotency and tags
- [Quickstart](../getting-started/quickstart.md) -- get a first job running
