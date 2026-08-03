---
title: Spring Boot Configuration
description: "Complete Ratchet property reference and supported override points for Spring Boot applications."
---

# Spring Boot Configuration

Ratchet reads its canonical `ratchet.*` settings through Spring Boot's relaxed property binding, so
they can live in `application.properties`, YAML, environment variables, or another Spring property
source. The tables below cover every property published by the Spring Boot integration's additional
configuration metadata.

Defaults are shown where Ratchet's typed configuration catalog defines one. The two lifecycle
defaults come directly from Spring's metadata. Two Spring-only bootstrap properties sit outside the
additional metadata: `ratchet.enabled` defaults to `true`, and
`ratchet.transaction-manager-bean-name` is covered under
[Multiple transaction managers](#multiple-transaction-managers).

## Lifecycle

| Property | Default | Description |
|---|---|---|
| `ratchet.lifecycle.defer-auto-start` | `false` | When true, Ratchet's Spring lifecycle bean does not auto-start the runtime; a consumer must call start() manually. |
| `ratchet.lifecycle.drain-timeout` | `PT30S` | Maximum time to wait for in-flight Ratchet jobs during Spring context shutdown. |

## Poller

| Property | Default | Description |
|---|---|---|
| `ratchet.poller.batch-size` | `50` | Configures Ratchet poller batch size. |
| `ratchet.poller.burst-delay-ms` | `500` | Configures Ratchet poller burst delay ms. |
| `ratchet.poller.min-delay-ms` | `2000` | Configures Ratchet poller min delay ms. |
| `ratchet.poller.max-delay-ms` | `10000` | Configures Ratchet poller max delay ms. |
| `ratchet.poller.deep-idle-delay-ms` | `30000` | Configures Ratchet poller deep idle delay ms. |
| `ratchet.poller.deep-idle-threshold-ms` | `60000` | Configures Ratchet poller deep idle threshold ms. |
| `ratchet.poller.idle-threshold` | `3` | Configures Ratchet poller idle threshold. |
| `ratchet.poller.claim-headroom-factor` | `0` | Configures Ratchet poller claim headroom factor. |

## Worker and threading

| Property | Default | Description |
|---|---|---|
| `ratchet.worker.default-threading-mode` | `PLATFORM` | Configures Ratchet worker default threading mode. |
| `ratchet.worker.job-executor-jndi` | `java:comp/DefaultManagedExecutorService` | Configures Ratchet worker job executor jndi. |
| `ratchet.worker.scheduled-executor-jndi` | `java:comp/DefaultManagedScheduledExecutorService` | Configures Ratchet worker scheduled executor jndi. |
| `ratchet.coordinator.thread-factory-jndi` | `java:comp/DefaultManagedThreadFactory` | Configures Ratchet coordinator thread factory jndi. |
| `ratchet.worker.virtual-executor-jndi` | Empty | Configures Ratchet worker virtual executor jndi. |
| `ratchet.worker.virtual-counter-accounting` | `false` | Configures Ratchet worker virtual counter accounting. |

## Thread pools

| Property | Default | Description |
|---|---|---|
| `ratchet.thread-pool.queue-size` | `100` | Configures Ratchet thread pool queue size. |
| `ratchet.thread-pool.size.single` | `20` | Configures Ratchet thread pool size single. |
| `ratchet.thread-pool.size.recurring` | `5` | Configures Ratchet thread pool size recurring. |
| `ratchet.thread-pool.size.batch-child` | `30` | Configures Ratchet thread pool size batch child. |
| `ratchet.thread-pool.size.batch-parent` | `2` | Configures Ratchet thread pool size batch parent. |
| `ratchet.thread-pool.size.chain-step` | `10` | Configures Ratchet thread pool size chain step. |
| `ratchet.thread-pool.size.workflow-branch` | `10` | Configures Ratchet thread pool size workflow branch. |
| `ratchet.thread-pool.size.workflow-join` | `10` | Configures Ratchet thread pool size workflow join. |

## Node

| Property | Default | Description |
|---|---|---|
| `ratchet.node.id` | Empty | Configures Ratchet node id. |
| `ratchet.node.heartbeat-interval-seconds` | `10` | Configures Ratchet node heartbeat interval seconds. |
| `ratchet.node.orphan-grace-seconds` | `60` | Configures Ratchet node orphan grace seconds. |
| `ratchet.node.orphan-scan-interval-minutes` | `5` | Configures Ratchet node orphan scan interval minutes. |
| `ratchet.node.dynamic-heartbeat-enabled` | `true` | Configures Ratchet node dynamic heartbeat enabled. |

## Recurring jobs

| Property | Default | Description |
|---|---|---|
| `ratchet.recurring.batch-limit` | `20` | Configures Ratchet recurring batch limit. |
| `ratchet.recurring.poll-ms` | `1000` | Configures Ratchet recurring poll ms. |
| `ratchet.recurring.max-poll-ms` | `60000` | Configures Ratchet recurring max poll ms. |
| `ratchet.recurring.startup-grace-seconds` | `60` | Configures Ratchet recurring startup grace seconds. |
| `ratchet.recurring.convergence-window-seconds` | `0` | Configures Ratchet recurring convergence window seconds. |

## Timeouts and retry buffer

| Property | Default | Description |
|---|---|---|
| `ratchet.retry-buffer.drain-interval-ms` | `1000` | Configures Ratchet retry buffer drain interval ms. |
| `ratchet.timeout.soft-timeout-percent` | `80` | Configures Ratchet timeout soft timeout percent. |
| `ratchet.timeout.default-sla-seconds` | `1800` | Configures Ratchet timeout default sla seconds. |
| `ratchet.timeout.signal-timeout-batch-size` | `500` | Configures Ratchet timeout signal timeout batch size. |

## Dead-letter queue

| Property | Default | Description |
|---|---|---|
| `ratchet.dlq.purge-enabled` | `true` | Configures Ratchet dlq purge enabled. |
| `ratchet.dlq.purge-cron` | `0 0 2 * * ?` | Configures Ratchet dlq purge cron. |
| `ratchet.dlq.purge-days` | `90` | Configures Ratchet dlq purge days. |

## Job and log retention

| Property | Default | Description |
|---|---|---|
| `ratchet.jobs.archive-enabled` | `true` | Configures Ratchet jobs archive enabled. |
| `ratchet.jobs.archive-cron` | `0 0 1 * * ?` | Configures Ratchet jobs archive cron. |
| `ratchet.jobs.retention-days` | `90` | Configures Ratchet jobs retention days. |
| `ratchet.jobs.archive-batch-size` | `1000` | Configures Ratchet jobs archive batch size. |
| `ratchet.logs.purge-enabled` | `true` | Configures Ratchet logs purge enabled. |
| `ratchet.logs.purge-cron` | `0 30 2 * * ?` | Configures Ratchet logs purge cron. |
| `ratchet.logs.retention-days` | `30` | Configures Ratchet logs retention days. |

## Schema

| Property | Default | Description |
|---|---|---|
| `ratchet.schema.auto-migrate` | `false` | Configures Ratchet schema auto migrate. |
| `ratchet.schema.migration-dialect` | Empty | Configures Ratchet schema migration dialect. |
| `ratchet.schema.migration-prefix` | `ddl/migrations` | Configures Ratchet schema migration prefix. |

## Payload and results

| Property | Default | Description |
|---|---|---|
| `ratchet.payload.max-payload-kb` | `100` | Configures Ratchet payload max payload kb. |
| `ratchet.jobs.max-result-bytes` | `65536` | Configures Ratchet jobs max result bytes. |

## Class policy

| Property | Default | Description |
|---|---|---|
| `ratchet.allow-empty-class-policy` | `false` | Configures Ratchet allow empty class policy. |
| `ratchet.class-policy.allowed-packages` | Empty | Configures Ratchet class policy allowed packages. |
| `ratchet.class-policy.allowed-result-type-packages` | Empty | Configures Ratchet class policy allowed result type packages. |

## Security

These are Ratchet runtime data-handling settings. They do not add Spring Security integration.

| Property | Default | Description |
|---|---|---|
| `ratchet.security.redact-emails` | `true` | Configures Ratchet security redact emails. |
| `ratchet.security.mask-payloads` | `false` | Configures Ratchet security mask payloads. |

## Isolation and priority

| Property | Default | Description |
|---|---|---|
| `ratchet.isolation-check` | `FAIL` | Configures Ratchet isolation check. |
| `ratchet.priority-boost-interval-minutes` | `15` | Configures Ratchet priority boost interval minutes. |

## Circuit breakers

| Property | Default | Description |
|---|---|---|
| `ratchet.circuit-breaker.enabled` | `true` | Configures Ratchet circuit breaker enabled. |
| `ratchet.circuit-breaker.default.failure-rate` | `50.0` | Configures Ratchet circuit breaker default failure rate. |
| `ratchet.circuit-breaker.default.window-size` | `100` | Configures Ratchet circuit breaker default window size. |
| `ratchet.circuit-breaker.default.wait-ms` | `30000` | Configures Ratchet circuit breaker default wait ms. |
| `ratchet.circuit-breaker.default.half-open-calls` | `3` | Configures Ratchet circuit breaker default half open calls. |
| `ratchet.circuit-breaker.default.minimum-calls` | `5` | Configures Ratchet circuit breaker default minimum calls. |
| `ratchet.circuit-breaker.fast.failure-rate` | `50.0` | Configures Ratchet circuit breaker fast failure rate. |
| `ratchet.circuit-breaker.fast.window-size` | `20` | Configures Ratchet circuit breaker fast window size. |
| `ratchet.circuit-breaker.fast.wait-ms` | `10000` | Configures Ratchet circuit breaker fast wait ms. |
| `ratchet.circuit-breaker.fast.half-open-calls` | `2` | Configures Ratchet circuit breaker fast half open calls. |
| `ratchet.circuit-breaker.fast.minimum-calls` | `3` | Configures Ratchet circuit breaker fast minimum calls. |
| `ratchet.circuit-breaker.critical.failure-rate` | `75.0` | Configures Ratchet circuit breaker critical failure rate. |
| `ratchet.circuit-breaker.critical.window-size` | `200` | Configures Ratchet circuit breaker critical window size. |
| `ratchet.circuit-breaker.critical.wait-ms` | `60000` | Configures Ratchet circuit breaker critical wait ms. |
| `ratchet.circuit-breaker.critical.half-open-calls` | `5` | Configures Ratchet circuit breaker critical half open calls. |
| `ratchet.circuit-breaker.critical.minimum-calls` | `10` | Configures Ratchet circuit breaker critical minimum calls. |
| `ratchet.circuit-breaker.external-api.failure-rate` | `60.0` | Configures Ratchet circuit breaker external api failure rate. |
| `ratchet.circuit-breaker.external-api.window-size` | `50` | Configures Ratchet circuit breaker external api window size. |
| `ratchet.circuit-breaker.external-api.wait-ms` | `60000` | Configures Ratchet circuit breaker external api wait ms. |
| `ratchet.circuit-breaker.external-api.half-open-calls` | `3` | Configures Ratchet circuit breaker external api half open calls. |
| `ratchet.circuit-breaker.external-api.minimum-calls` | `5` | Configures Ratchet circuit breaker external api minimum calls. |
| `ratchet.circuit-breaker.claim-path.failure-rate` | `50.0` | Configures Ratchet circuit breaker claim path failure rate. |
| `ratchet.circuit-breaker.claim-path.window-size` | `20` | Configures Ratchet circuit breaker claim path window size. |
| `ratchet.circuit-breaker.claim-path.wait-ms` | `5000` | Configures Ratchet circuit breaker claim path wait ms. |
| `ratchet.circuit-breaker.claim-path.half-open-calls` | `1` | Configures Ratchet circuit breaker claim path half open calls. |
| `ratchet.circuit-breaker.claim-path.minimum-calls` | `5` | Configures Ratchet circuit breaker claim path minimum calls. |

## Virtual-thread and rate limits

Each execution type has an independent virtual-thread concurrency limit and per-minute rate limit.
A value of `0` leaves that limit unset.

| Property | Default | Description |
|---|---|---|
| `ratchet.virtual-thread-limit.single` | `0` | Configures Ratchet virtual thread limit single. |
| `ratchet.rate-limit-per-minute.single` | `0` | Configures Ratchet rate limit per minute single. |
| `ratchet.virtual-thread-limit.recurring` | `0` | Configures Ratchet virtual thread limit recurring. |
| `ratchet.rate-limit-per-minute.recurring` | `0` | Configures Ratchet rate limit per minute recurring. |
| `ratchet.virtual-thread-limit.batch-child` | `0` | Configures Ratchet virtual thread limit batch child. |
| `ratchet.rate-limit-per-minute.batch-child` | `0` | Configures Ratchet rate limit per minute batch child. |
| `ratchet.virtual-thread-limit.batch-parent` | `0` | Configures Ratchet virtual thread limit batch parent. |
| `ratchet.rate-limit-per-minute.batch-parent` | `0` | Configures Ratchet rate limit per minute batch parent. |
| `ratchet.virtual-thread-limit.chain-step` | `0` | Configures Ratchet virtual thread limit chain step. |
| `ratchet.rate-limit-per-minute.chain-step` | `0` | Configures Ratchet rate limit per minute chain step. |
| `ratchet.virtual-thread-limit.workflow-branch` | `0` | Configures Ratchet virtual thread limit workflow branch. |
| `ratchet.rate-limit-per-minute.workflow-branch` | `0` | Configures Ratchet rate limit per minute workflow branch. |
| `ratchet.virtual-thread-limit.workflow-join` | `0` | Configures Ratchet virtual thread limit workflow join. |
| `ratchet.rate-limit-per-minute.workflow-join` | `0` | Configures Ratchet rate limit per minute workflow join. |

## MongoDB

The MongoDB settings have no declared defaults. When `connection-string` is configured, `database`
must also be set.

| Property | Default | Description |
|---|---|---|
| `ratchet.mongodb.connection-string` | None | MongoDB connection string used to create Ratchet's isolated MongoDB client. |
| `ratchet.mongodb.database` | None | MongoDB database containing Ratchet collections. |

## Multiple transaction managers

The JPA starter fails at startup when more than one SQL store implementation is on the classpath.
For example, PostgreSQL and MySQL together produce this `IllegalStateException`:

```text
Ratchet JPA auto-configuration found multiple ratchet-store-* dependencies on the classpath: [ratchet-store-postgresql, ratchet-store-mysql]. Keep exactly one ratchet-store-* dependency.
```

Remove or exclude the extra store jars so exactly one `ratchet-store-*` dependency remains.

Ratchet's supported JPA model has one application-owned, Boot-managed `EntityManagerFactory`. It
selects a `JpaTransactionManager` that owns that factory. When several transaction-manager beans are
present and there is no unique primary candidate, startup fails with this form:

```text
Ratchet JPA could not select a JpaTransactionManager bean from candidates [firstTransactionManager, secondTransactionManager]. Mark exactly one JpaTransactionManager bean @Primary or set 'ratchet.transaction-manager-bean-name' to the desired bean name.
```

Mark the intended `JpaTransactionManager` bean `@Primary`, or set
`ratchet.transaction-manager-bean-name` to its bean name. The selected transaction manager must own
the single application `EntityManagerFactory`; choosing a manager does not make multiple persistence
units supported. Ratchet's JPA entities join that application persistence unit.

## Supported overrides

The default `RatchetOptions` bean is seeded from the Spring `Environment` through
`RatchetOptionsFactory.builderFromEnvironment(...)`. If the application supplies its own
`RatchetOptions` bean, auto-configuration backs off. The public environment-seeded builder lets code
pin selected values with typed setters; those setters win over values read while seeding:

```java
package com.example;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import run.ratchet.api.RatchetOptions;
import run.ratchet.api.RatchetOptionsFactory;

@Configuration(proxyBeanMethods = false)
public class RatchetConfiguration {
  @Bean
  RatchetOptions ratchetOptions() {
    return RatchetOptionsFactory.builderFromEnvironment()
        .schema(schema -> schema.autoMigrate(false))
        .build();
  }
}
```

With no additional source, the public builder reads environment variables, system properties, and
MicroProfile Config when present. Replacing the auto-configured bean this way does not automatically
replay arbitrary Spring `application.properties` or YAML property sources; use it when that source
behavior is intentional.

The integration also supports these narrow application overrides:

- Define a `Jsonb` bean to make the default payload serializer borrow that instance. If none exists,
  Ratchet creates and owns its JSON-B instance.
- Define a `Supplier<ScheduledExecutorService>` bean to replace the default scheduled-executor
  supplier.
- Configure the default `ClassPolicy` with `ratchet.class-policy.allowed-packages` and
  `ratchet.class-policy.allowed-result-type-packages`. A custom `ClassPolicy` bean also makes the
  default policy bean back off.

Actuator endpoints, Spring Security integration, a UI, and cluster coordinators are not part of the
Spring Boot integration scope.
