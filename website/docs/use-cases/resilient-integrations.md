---
title: Resilient Third-Party Integrations
description: Call flaky external APIs from durable jobs that retry transient failures with backoff and trip a circuit breaker during an outage, so a downstream provider going down does not take your work with it.
---

# Resilient Third-Party Integrations

Sooner or later your code has to call someone else's: a payment gateway, a shipping carrier, an email provider, a partner's REST API. Those calls fail in two different ways, and the two need different answers. Sometimes it is a blip, a 503 or a dropped connection that works on the next try. Sometimes the provider is genuinely down, and every call you make for the next ten minutes is going to fail no matter what.

Retrying handles the blip. It is the wrong tool for the outage: a hundred queued jobs each retrying a dead API eight times is four hundred doomed calls and a thundering herd on a service that is already struggling. A circuit breaker handles the outage. It notices the failure rate, stops sending calls for a while, and lets the provider recover.

Ratchet gives you both, and they stay out of each other's way. The retry budget rides out transient errors. The breaker rides out outages without spending that budget. The external call lives in a durable job, so none of this happens on the request thread, and a provider's bad afternoon never reaches your user as a failed request.

::: tip Verified
The Java on this page compiles against `ratchet-api` `0.2.1`. It shows real API usage, not pseudocode. The running app needs a Jakarta EE server and a configured store. The `@CircuitBreakerProtected` annotation is marked `@Incubating` and may change.
:::

## Layer one: the call as a retrying job

Put the external call in a job, give it a retry budget, and back off between attempts. A transient failure now costs the customer nothing.

```java
import java.time.Duration;
import java.util.UUID;

import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobSchedulerService;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class FulfillmentJobs {

  @Inject JobSchedulerService scheduler;

  @Inject CarrierApi carrier;

  @Inject ShipmentRepository shipments;

  public JobHandle requestLabel(UUID orderId) {
    return scheduler
        .enqueue(() -> bookLabel(orderId))
        .withMaxRetries(8)
        .withBackoff(BackoffPolicy.EXPONENTIAL, Duration.ofSeconds(5))
        .withTimeout(Duration.ofSeconds(30))
        .withIdempotencyKey("label-" + orderId)
        .withTags("shipping", "external")
        .submit();
  }

  public void bookLabel(UUID orderId) {
    String tracking = carrier.createLabel(orderId);
    shipments.attachTracking(orderId, tracking);
  }
}
```

Eight attempts with exponential backoff starting at five seconds spreads the retries out instead of slamming the carrier the instant it stutters. The timeout caps how long a single hung call can sit there. If every attempt fails, the job dead-letters with its last error, ready to inspect and replay.

## Layer two: wrap the client in a circuit breaker

The retry budget is for failures that clear on their own. It does nothing for a carrier that is flat-out down, where retrying is just noise. That is the breaker's job. Annotate the client bean with `@CircuitBreakerProtected` and pick the `EXTERNAL_API` profile, which is tuned for third-party services.

```java
import java.util.UUID;

import run.ratchet.api.CircuitBreakerProfile;
import run.ratchet.api.CircuitBreakerProtected;

import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
@CircuitBreakerProtected(service = "carrier-api", profile = CircuitBreakerProfile.EXTERNAL_API)
public class CarrierApi {

  public String createLabel(UUID orderId) {
    // POST to the carrier; throws on a non-2xx response or a timeout.
    return doHttpCreateLabel(orderId);
  }

  private String doHttpCreateLabel(UUID orderId) { /* the real HTTP client */ }
}
```

A CDI interceptor wraps every call to a method on this bean in a breaker keyed by `service`. The `EXTERNAL_API` profile opens the circuit once 60% of a 50-call sliding window has failed, holds it open for 60 seconds, then lets three trial calls through to test recovery. The `service` name is the unit of sharing: every bean and method that names `"carrier-api"` draws on the same breaker, so failures seen by one method protect all the others.

## How the two layers stay out of each other's way

This is the part that makes the combination work rather than fight. When the breaker is open, a call to `createLabel` does not run and does not fail in the ordinary sense. It throws `CircuitBreakerOpenException`, and Ratchet treats that differently from a normal job failure: the job is **rescheduled, not retried**. It goes back to pending with a delay set to whatever time is left on the breaker's open window, and its attempt counter is left untouched.

So the two budgets never cannibalize each other:

- A real failure from the carrier (a 500, a timeout) consumes one of the eight attempts and backs off. That is the retry budget doing its job.
- An open circuit costs zero attempts. The job parks until the breaker is ready to test recovery, then tries for real.

A ten-minute carrier outage no longer burns through a job's retries and dead-letters it while the carrier is still down. The breaker absorbs the outage; the retry budget is still there, intact, for the genuine attempt once the carrier is back. And because the breaker fails fast while open, a backlog of queued label jobs stops hammering the carrier within a single sliding window instead of grinding against it for as long as it stays down.

## Retries need idempotency

A retried `POST` is only safe if calling it twice does no harm. The first attempt might have reached the carrier and created the label before the response timed out, so attempt two could book a second shipment. Two defenses, used together:

- `withIdempotencyKey` on the Ratchet job (above) stops a double submission of the same logical work from creating two jobs.
- An idempotency key on the outbound request, derived from the order, lets the carrier collapse a duplicated call into the original. Most payment and shipping APIs support one; use it.

## Honest scope

- `@CircuitBreakerProtected` is `@Incubating`. The behavior is stable but the annotation's shape may change before it is finalized.
- The breaker is in-process. Each node keeps its own failure window and trips its own circuit, so on a five-node cluster the provider sees up to five independent breakers, not one shared verdict. That is usually fine, since each node only stops sending its own doomed traffic.
- The profile thresholds are defaults. If `EXTERNAL_API` is too eager or too patient for a particular provider, override the profile per deployment through `RatchetOptions` rather than reaching for a custom annotation.

## Why run it through Ratchet

You could wire up Resilience4j and a thread pool yourself and get to roughly the same place. Folding it into Ratchet means the retrying, the breaker, and the durable record of the work are one mechanism instead of three you have to integrate. The external call is a CDI bean, the breaker is an annotation on it, and the job that drives the call survives a restart because it was persisted before the first attempt. The flaky integration becomes a job like any other, with the failure handling already attached.

## Next steps

- [Circuit breakers](../advanced/circuit-breakers.md) -- profiles, state machine, and configuration overrides in depth
- [Retry strategies](../concepts/retry-strategies.md) -- backoff policies and attempt budgets
- [Error handling](../concepts/error-handling.md) -- how failures move through retries to the dead-letter queue
- [Annotations](../api-reference/annotations.md) -- the full `@CircuitBreakerProtected` reference
- [Quickstart](../getting-started/quickstart.md) -- get a first job running
