---
sidebar_position: 9
title: Cluster Coordinators
description: "First-party push-based cross-node wakeup modules: delivery guarantees, configuration, failure behavior, metrics, and the polling fallback floor"
---

# Cluster Coordinators

A cluster coordinator pushes a "new work is available" hint to the other
nodes so they wake their pollers immediately instead of waiting for the next
poll cycle. Ratchet ships four first-party coordinator modules. You enable one
by adding its dependency; no code or `beans.xml` changes are required.

Coordinators are a **latency optimization, never a correctness mechanism**.
Claim correctness always comes from the store (`FOR UPDATE SKIP LOCKED` on SQL,
the equivalent guarded find-and-modify on MongoDB). A coordinator only shortens
the gap between "work submitted on node A" and "node B notices." If you can
tolerate poll-interval latency for cross-node wakeups, you do not need one (see
[When you need a coordinator](#when-you-need-a-coordinator)).

## When you need a coordinator

Out of the box Ratchet uses `NoOpClusterCoordinator`: no cross-node push,
correctness from the store, wakeup latency bounded by each node's poll interval.
That is the right choice when poll-interval latency is acceptable, which covers
most deployments.

Reach for a coordinator when **both** of these hold:

- You run more than one node against the same store.
- You submit time-sensitive work (`CRITICAL` priority or `.immediate()` jobs)
  whose first execution should start in well under a poll interval, even when it
  lands on a node other than the one that polls next.

A single-node deployment never needs a coordinator. Neither does a multi-node
deployment whose latency budget already fits inside the poll interval.

Not every submission triggers a wakeup even with a coordinator installed. The
scheduler only publishes a hint for `CRITICAL`-priority jobs, zero-delay
single-job submissions, and batch-parent jobs (so child distribution starts
quickly). Normal and low-priority jobs wait for the next poll regardless.

## The four modules

| Module | Transport | Best fit |
| --- | --- | --- |
| `ratchet-coordinator-postgresql` | PostgreSQL `LISTEN`/`NOTIFY` | A PostgreSQL-backed deployment (no extra infrastructure) |
| `ratchet-coordinator-jms` | Jakarta Messaging topic | You already run a broker (ActiveMQ Artemis, IBM MQ, …) or want an EE-native transport |
| `ratchet-coordinator-hazelcast` | Hazelcast `ITopic` | Payara (bundles Hazelcast), or any deployment already using Hazelcast |
| `ratchet-coordinator-infinispan` | Infinispan / JGroups | WildFly, or a stack already running Infinispan + JGroups |

All four share `ratchet-coordinator-common` (pulled in transitively) and the
same delivery, self-suppression, failure, and metrics contracts described below.

### Choosing one

Pick the transport you already operate. If you have no preference, the
PostgreSQL coordinator is the lowest-friction option for a PostgreSQL store: it
needs no broker, no cache grid, and no extra ports: just the database you are
already connected to.

## Enabling a coordinator

Add **exactly one** coordinator module to your deployment. With the
[`ratchet-bom`](/getting-started/installation) imported, the dependency is
versionless:

```xml
<dependency>
  <groupId>run.ratchet</groupId>
  <artifactId>ratchet-coordinator-postgresql</artifactId>
</dependency>
```

That is the whole opt-in. Each coordinator is a CDI `@Alternative` annotated
with `@Priority`, so per CDI 4.0 it is selected globally across all archives and
replaces `NoOpClusterCoordinator` automatically. You do not edit application
`beans.xml`.

Confirm activation from the startup log. With no coordinator on the classpath
you will see:

```
Ratchet cluster coordination: NoOp (no cross-node wakeups). Add a ratchet-coordinator-* module to enable push-based wakeups.
```

Once a module is present, that line is gone and the chosen coordinator logs its
own initialization instead.

::: warning Install exactly one
Putting two coordinator modules on the classpath is a misconfiguration. The
`@Priority` values (`postgresql` > `jms` > `hazelcast` > `infinispan`) only exist
so that an accidental transitive double-pull resolves deterministically to one
coordinator rather than failing deployment. They are not a way to run two
transports at once.
:::

## Configuration

Every coordinator reads a configuration record and falls back to its
`defaults()` when you provide no override. To pin or change settings, produce the
record as a CDI bean:

```java
@ApplicationScoped
public class CoordinatorConfigProducer {
  @Produces
  PostgresqlCoordinatorConfig config() {
    return PostgresqlCoordinatorConfig.defaults();
  }
}
```

The defaults are tuned for the common case; most deployments change nothing.

**PostgreSQL** (`PostgresqlCoordinatorConfig`)

| Setting | Default | Purpose |
| --- | --- | --- |
| `channel` | `ratchet_wakeup` | Base `NOTIFY` channel. Must match `^[A-Za-z_][A-Za-z0-9_]*$`; the effective name is truncated to PostgreSQL's 63-byte identifier limit |
| `cellId` | _(none)_ | Optional per-cell suffix; see [Multi-cell isolation](#multi-cell-isolation) |
| `receiveTimeoutMs` | `1000` | `getNotifications()` wait timeout on the LISTEN connection |
| `reconnectBackoffInitialMs` | `200` | Delay after the first reconnect failure; doubles per retry |
| `reconnectBackoffMaxMs` | `30000` | Cap on the doubled reconnect delay |
| `maxInboundPayloadChars` | `16384` | Hard cap on an inbound envelope (envelopes are ~80 chars) |
| `listenerExecutorThreads` | `1` | Dispatch-pool worker count |
| `listenerExecutorQueueCapacity` | `1024` | Dispatch-pool queue bound; oldest is dropped when full |
| `shutdownGraceMs` | `5000` | Max wait for the LISTEN thread and dispatch pool to drain on `close()` |

The publish path borrows short-lived connections from your configured
`DataSource` for `pg_notify`, so publishing never stalls behind the dedicated
(autocommit, non-pooled) LISTEN connection.

**JMS** (`JmsCoordinatorConfig`)

| Setting | Default | Purpose |
| --- | --- | --- |
| `connectionFactoryJndi` | `java:comp/DefaultJMSConnectionFactory` | Connection factory lookup name |
| `topicJndi` | `java:comp/Ratchet/Wakeup` | Wakeup topic lookup name |
| `cellId` | _(none)_ | Operator hygiene only; JMS cell isolation needs separate physical topics |
| `brokerSideSelfFilter` | `true` | Add a JMS selector so the broker drops a node's own messages (receive-side filtering is always on regardless) |
| `reconnectBackoffInitialMs` | `200` | Reconnect delay after the `ExceptionListener` fires; doubles per retry |
| `reconnectBackoffMaxMs` | `30000` | Cap on the doubled reconnect delay |
| `maxInboundPayloadChars` | `16384` | Hard cap on an inbound `TextMessage` body |
| `listenerExecutorThreads` | `2` | Dispatch-pool worker count |
| `listenerExecutorQueueCapacity` | `1024` | Dispatch-pool queue bound; oldest is dropped when full |
| `shutdownGraceMs` | `5000` | Max wait for in-flight callbacks on `close()` |

**Hazelcast** (`HazelcastCoordinatorConfig`)

| Setting | Default | Purpose |
| --- | --- | --- |
| `topicName` | `ratchet-wakeup` | `ITopic` name; the cell suffix is appended when set |
| `cellId` | _(none)_ | Optional per-cell suffix |
| `maxInboundPayloadChars` | `16384` | Hard cap on an inbound payload |
| `listenerExecutorThreads` | `2` | Dispatch-pool worker count |
| `listenerExecutorQueueCapacity` | `1024` | Dispatch-pool queue bound; oldest is dropped when full |
| `shutdownGraceMs` | `5000` | Max wait for listener removal on `close()` |

Publishing uses `ITopic.publishAsync`, so `notifyNewWork` never blocks on a
client-mode network round-trip.

**Infinispan** (`InfinispanCoordinatorConfig`)

| Setting | Default | Purpose |
| --- | --- | --- |
| `cacheName` | `wakeup` | Wakeup cache name; the cell suffix is appended when set |
| `cellId` | _(none)_ | Optional per-cell suffix |
| `wakeupTtlSeconds` | `60` | Per-entry TTL applied to every put, so wakeup entries always expire |
| `maxInboundPayloadChars` | `16384` | Hard cap on an inbound value |
| `listenerExecutorThreads` | `2` | Dispatch-pool worker count |
| `listenerExecutorQueueCapacity` | `1024` | Dispatch-pool queue bound; oldest is dropped when full |
| `shutdownGraceMs` | `5000` | Max wait for listener removal on `close()` |

The JMS connection factory and topic, and the PostgreSQL `DataSource`, are
resolved from CDI. Provide them the same way you already wire your store
resources; the coordinators look them up by the JNDI names above unless you
override the config.

## Delivery guarantees

Wakeups are **best-effort, at-most-once hints**. There is no delivery receipt,
no redelivery, and no ordering guarantee. A wakeup carries three things: the
priority of the new work, the identity of the origin node, and an optional
execution-target label.

The execution target is **informational only**. A receiving node wakes its
poller unconditionally; the claim-side filter on each node then decides which
pool actually drains. A wakeup never routes a job to a particular node: it only
prompts nodes to look.

Publishing never blocks the scheduler and never throws into it. A transport
error during publish is logged and counted, then swallowed. The submitting
transaction has already committed by the time the hint goes out, so a dropped
hint costs latency, not work.

## Fallback semantics: the polling floor

This is the guarantee that lets you treat coordinators as optional:

> **Polling-based claim correctness is the floor, regardless of coordinator.**
> A coordinator wakeup is a latency optimization layered on top. It never
> affects which node claims a job, and it can never weaken claim safety.

Concretely, the scheduler behaves correctly in every degraded case:

- **Push delivery fails** (broker down, channel error, connection lost): the
  hint is dropped, and each node's adaptive poll loop still drains the queue at
  poll-interval latency.
- **A node is not subscribed yet** (still starting, mid-reconnect): it misses
  hints during that window and falls back to polling until it re-subscribes.
- **The coordinator is removed entirely**: the deployment reverts to the
  `NoOp` behavior with no correctness change, only higher wakeup latency.

No coordinator implementation may break this contract. The `ClusterCoordinator`
SPI documents wakeups as best-effort hints with "the local poll loop remains the
source of truth," and the poller's wakeup listener treats both registration
failures and delivery failures as non-fatal.

## Self-notification handling

On a broadcast transport a node receives its own published wakeups. Every
coordinator drops these receive-side by comparing the wakeup's origin
`NodeIdentity` against the local node identity; a self-match is discarded and
counted with the `ignored_self` outcome.

The JMS coordinator adds a second layer when `brokerSideSelfFilter` is on (the
default): a JMS selector asks the broker not to deliver a node's own messages in
the first place. The receive-side check still runs underneath as defense in
depth.

## Failure and reconnect behavior

- **Transport errors on publish** are logged, counted, and never propagated to
  the caller of `notifyNewWork`.
- **Connection loss** on the PostgreSQL and JMS coordinators triggers a bounded
  exponential-backoff reconnect loop (`reconnectBackoffInitialMs` doubling up to
  `reconnectBackoffMaxMs`). The Infinispan and Hazelcast transports manage their
  own cluster membership and reconnection; the coordinator does not.
- **Back-pressure** is bounded. The listener dispatch pool has a fixed queue
  capacity and a discard-oldest policy, so a sustained wakeup storm drops the
  oldest hints rather than exhausting memory. Losing a hint only costs latency.
- **Shutdown** is clean and idempotent. `close()` can be called twice safely,
  loop threads are interrupt-aware, and the coordinator waits up to
  `shutdownGraceMs` for threads and the dispatch pool to drain.

In a Jakarta EE container the coordinators run their threads on the
container-managed thread factory (`java:comp/DefaultManagedThreadFactory`). If
that binding is missing they fail loudly at startup rather than silently falling
back to unmanaged threads.

## Metrics

Coordinators emit two counters through the `MetricsCollector` SPI:

- `clusterWakeupPublished(transport, outcome)` -- one per publish attempt.
- `clusterWakeupReceived(transport, outcome)` -- one per inbound wakeup.

The `transport` label is the coordinator kind: `postgresql`, `jms`,
`infinispan`, or `hazelcast`. The `outcome` label is bounded:

| Direction | Outcomes |
| --- | --- |
| Published | `success`, `failure` |
| Received | `delivered`, `ignored_self`, `parse_failure`, `transport_failure`, `pre_registration_overflow`, `listener_failure` |

Node identity, job ids, and workflow ids are deliberately excluded from metric
labels because they are unbounded and would blow up cardinality. Watch
`ignored_self` to confirm self-suppression is working, and `transport_failure` /
`failure` to spot a degraded transport (the scheduler keeps working through the
polling floor while you investigate).

## Multi-cell isolation

If you run several independent Ratchet cells that share a transport, set a
`cellId` to keep their wakeup traffic apart. The cell id is appended to the
channel, topic, or cache name with an underscore separator (`ratchet_wakeup`
becomes `ratchet_wakeup_orders` for `cellId = "orders"`). Underscore is the one
separator legal in an unquoted PostgreSQL identifier that is also valid as a
Hazelcast topic and an Infinispan cache name.

The JMS coordinator is the exception: a `cellId` there is operator hygiene only.
True JMS isolation requires provisioning separate physical topics with distinct
JNDI bindings per cell.

## See also

- [Clustering](/deployment/clustering) -- claim-based execution, heartbeats, and
  where wakeup notifications fit
- [Cluster Configuration](/deployment/cluster-configuration) -- node identity,
  distributed locks, and Kubernetes patterns
- [Monitoring](/deployment/monitoring) -- wiring up the `MetricsCollector`
- [Clustering concepts](/concepts/clustering) -- the `ClusterCoordinator` SPI and
  how to implement a custom transport
