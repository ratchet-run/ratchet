# Ratchet Spring Boot Extension Feasibility

Status: **CONDITIONAL GO**

Date: 2026-07-29

Baseline: `origin/main` at `f3ea377adb4bc829c4126d10976d9cd707ca0b9a`

Primary target tested: Spring Boot 4.1.0

Compatibility target tested: Spring Boot 3.5.16

## Decision

A dependency-first Spring Boot extension is feasible, including Ratchet and
application entities in one Boot-managed JPA persistence unit. The target
developer experience, with `ratchet-bom` imported as in the existing
quickstart, should be:

```xml
<dependency>
  <groupId>run.ratchet</groupId>
  <artifactId>ratchet-spring-boot-starter</artifactId>
</dependency>
<dependency>
  <groupId>run.ratchet</groupId>
  <artifactId>ratchet-store-postgresql</artifactId>
</dependency>
```

With an ordinary Boot datasource, the application should then inject
`JobSchedulerService`; it should not add `persistence.xml`, `@EntityScan`, a
second `EntityManagerFactory`, lifecycle glue, JNDI, or a CDI implementation.
A separate `ratchet-spring-boot-starter-mongodb` must keep JPA and Hibernate
absent.

This is not feasible as a thin auto-configuration wrapper around current
internals. Ratchet first needs a supported, container-neutral runtime handle
and small transaction/recurring/store-construction seams. Reaching into
unexported RI packages, invoking CDI observers, or embedding Weld would create
an extension that is fragile on the JVM and untenable in a native image.

No JDBC storage comparison was created or run. JPA passed its simplicity gate,
so JPA remains the selected SQL path.

## Evidence Summary

| Spike | Result | Evidence |
|---|---|---|
| Spring transaction truth | PARTIAL | Six real PostgreSQL tests passed for commit, rollback, `REQUIRES_NEW`, `NOT_SUPPORTED`, self-invocation, and wakeups. Spring honors Jakarta transaction annotations, but Ratchet's missing-JNDI fallback publishes before rollback. |
| Spring-native bootstrap/lifecycle | PARTIAL | Six tests and an executable Boot jar proved Spring bean resolution, lifecycle gating, ordered shutdown, and no thread leak. Current Ratchet exposes no supported bootstrap and initializes its node before migration. |
| Persistence boundary | PASS | Two JPA tests used PostgreSQL 16 and one Boot-managed EMF/`JpaTransactionManager` for application plus Ratchet entities. One Mongo-only test and its dependency tree contained no JPA or Hibernate. |
| Fresh-project developer experience | PARTIAL | The same starter jar passed five tests on Boot 4.1.0 and five on Boot 3.5.16, including typed metadata, user-bean backoff, service resolution, disablement, and actionable transaction-manager ambiguity. It could not start the real scheduler because the bootstrap seam is missing. |
| AOT/native payload | PARTIAL | A real Linux/ARM64 Boot 4.1 native image compiled with Liberica NIK 25.0.3 and ran. Both ClassPolicy denials passed. Direct invocation lacked a reflection hint; both lambda forms lacked generated `writeReplace()` registration. Persistence/claim/retry remained outside the supported surface. |

The disposable probe ledger under `.planning/spikes/` contains the exact
commands, intermediate failures, environment versions, and per-scenario
results.

## What Is Simple

### JPA coexistence

Boot 4.1 discovered Ratchet's dependency-jar `META-INF/orm.xml` without:

- a consumer `persistence.xml`;
- `spring.jpa.mapping-resources`;
- Ratchet-specific `@EntityScan`;
- a custom or second EMF; or
- a custom transaction manager.

The single metamodel contained the application's entity plus Ratchet's
`JobEntity` and `NodeEntity`. Application and Ratchet converters coexisted.
Twelve operations on four worker threads committed or rolled back application
and Ratchet rows together against PostgreSQL 16.

This is the right Spring design. A separate Ratchet persistence unit would
make application entity configuration harder and would discard the strongest
positive result from the spike.

### Spring transactions

Spring Tx 7.0.8 recognized Ratchet's Jakarta `@Transactional` annotations and
propagation. The required replacement is narrow:

- an exported tri-state after-commit registrar;
- the current Jakarta TSR implementation behind that registrar; and
- a Spring implementation using both
  `TransactionSynchronizationManager.isActualTransactionActive()` and
  `isSynchronizationActive()`.

The tri-state behavior must remain exact: no transaction may run immediately;
a registered action runs only after commit; an active transaction whose
registration failed must suppress the action.

### Starter conventions

Boot's normal extension mechanisms worked:

- `AutoConfiguration.imports`;
- typed `@ConfigurationProperties`;
- generated configuration and condition metadata;
- `@ConditionalOnMissingBean` user overrides;
- `ratchet.enabled=false`; and
- explicit remediation when multiple transaction managers exist.

Ratchet's parent sets `maven.compiler.proc=none`, so the production
auto-configuration module must explicitly enable Boot's two annotation
processors.

## What Requires Core Work

### Runtime bootstrap and lifecycle

The `run.ratchet.ri` module exports no packages. Scheduler startup and
recurring registration are package-private CDI observer methods.

The supported replacement should be one exported, incubating runtime handle
with idempotent `start()` and `stop()`. CDI and Spring become thin lifecycle
adapters around the same orchestration. The required order is:

1. schema/before-start hooks;
2. recurring registration;
3. initial node heartbeat and recovery;
4. poller, recurring scheduler, timers, and wakeup listener;
5. after-start hooks.

`RatchetProducer.nodeIdentityProvider()` currently calls `init()` while
producing the bean, before the later migration hook. That side effect must
move into the common runtime start sequence.

The spike proved the lifecycle primitives, but its
`ApplicationReadyEvent` trigger is too late for production because Boot
runners have already begun by then. The production adapter must start during
container lifecycle processing, after singleton creation and before
`ApplicationRunner`/`CommandLineRunner` and readiness. A high-phase
auto-start `SmartLifecycle` is the planned proof point; shutdown must finish
while the EMF and datasource remain available.

### Runtime graph construction

Spring cannot construct the current graph through supported APIs:

- CDI producers create configured components;
- many optional capabilities are injected as CDI `Instance<T>`;
- internal event delivery contains a CDI event bridge; and
- the fallback caller-principal provider depends directly on Jakarta Security
  and CDI `Instance<SecurityContext>`; and
- public RI classes live in unexported packages.

The portability work should use explicit dependencies, lists, optionals, and
the existing `JobStore.capability(...)` contract—not a generic imitation of a
DI container. Transactionally annotated core services must still be created as
container-managed beans; a constructor-only common assembler would bypass
Spring AOP and is not acceptable. Use a common Ratchet component catalog plus
common runtime orchestration, with thin CDI and Spring managed-construction
adapters and cross-container graph drift tests. The Spring adapter should register
`ApplicationEventPublisher.publishEvent` through the existing programmatic
listener path and remove it on shutdown. Initial Spring Security integration
is out of scope: the default fallback principal is empty, while the existing
`CallerPrincipalResolver` option remains the explicit application seam.

The source audit found two concrete proxy repairs that the plan must implement,
not merely test around: `DefaultJobSchedulerService` injects the concrete,
interface-proxyable `DefaultJobCreationService`, and `BatchService` falls back
to raw `this` for a `NOT_SUPPORTED` to `REQUIRES_NEW` call outside CDI. Use
proxy-safe interfaces and a separately managed transactional collaborator,
then inventory every other transactional graph edge under both Spring proxy
settings.

Spring also cannot name the existing default
`run.ratchet.ri.security.PackagePrefixClassPolicy` without crossing the
unexported-package boundary. The narrow exported runtime package must provide
a default factory returning the `ClassPolicy` SPI. Canonical
`RatchetConfigKeys` and `RatchetOptions.SecurityOptions` values—not
Spring-only configuration—must keep invocation and result-type package
allowlists separate, preserve prefix/gadget validation, and fail startup on an
empty policy unless the existing explicit test-only opt-in is set. Its default
`ExecutorProvider` must own non-JNDI executors with bounded shutdown and back
off for an application provider.

### Payload serialization

The disposable probes did not exercise Ratchet's default
`PayloadSerializer`. Current default construction is CDI-internal and relies
on JSON-B, while Boot 3.5 and 4.1 use different preferred Jackson generations.
The common starter must therefore not bind to either Jackson API.

Both Boot generations document support for auto-configuring a
`jakarta.json.bind.Jsonb` bean when a provider such as Yasson is present. The
planned default is a small JSON-B `PayloadSerializer` adapter with Yasson
provided by the starter and normal user-bean backoff. This remains an
implementation proof obligation: JVM matrix tests must round-trip
representative payloads, and the real native gate must prove JSON-B
serialization and restoration without application hints. The adapter must not
change Boot's preferred HTTP JSON mapper or own/close a user-provided `Jsonb`
bean.

### Full configuration surface

The fresh-project probe covered only three Spring-specific properties; it did
not bind every `RatchetOptions` key. Production code should adapt Spring's
relaxed `Binder` to `RatchetConfigSource` and continue through
`RatchetOptionsFactory`, rather than duplicate parsing/default behavior in a
second options builder. Expose a read-only catalog of Ratchet's canonical
configuration descriptors and compare it with Spring's additional
configuration metadata so names, types, and defaults cannot drift.

### Store construction

PostgreSQL, MySQL, Oracle, SQL Server, and Mongo expose public store interfaces
but package-private implementation classes and constructors. Keep those
implementations encapsulated. Add a small public factory in each store package
that returns its public store interface from explicit inputs.

The first production JPA slice must prove that a factory-created store retains
Spring transaction interception and lifecycle callbacks. Mapping coexistence
alone does not prove that final wiring.

### Recurring jobs

Current recurring discovery uses CDI `BeanManager`; invocation uses CDI
`Instance<Object>` and destroys dependent instances. The neutral design needs:

- a container-specific discovery adapter producing method descriptors;
- registration independent of CDI;
- invocation through `BeanResolver`; and
- a managed resolver handle so CDI dependent and Spring prototype targets can
  be released correctly.

The persisted dispatch target currently names
`run.ratchet.ri.cdi.RecurringMethodInvoker`. Its fully qualified name or a
backward-compatible alias must remain valid for already-persisted recurring
jobs.

### Native payload reachability

Ordinary Spring bean AOT reachability did not cover Ratchet's dynamic
invocation:

- `ProbeJobService.executeDirect()` needed explicit invocation reflection;
- a bound method reference needed registration of its generated serializable
  lambda; and
- an inline wrapper needed the same lambda registration before Ratchet could
  reach its additional caller-class resource/ASM path.

Spring Framework 7.0.8 has `LambdaHint` and
`ReflectionHints.registerLambda(...)`; Spring Framework 6.2.19 does not. The
same starter jar is viable for the tested JVM surface, but cross-line native
support is not established. Initial native support should target Boot 4.1
only, isolate Spring 7 AOT code from Boot 3.5 JVM startup, and add Boot 3.5
native only after an independent executable gate passes.

The simplest honest compatibility boundary is one explicit
`ratchet-spring-boot-aot-spring7` build dependency for Boot 4 native
consumers. Ordinary JVM starters must not depend on it. The AOT processor must
also derive bounded JSON-B reflection hints from eligible job
parameter/result types; the preflight must prove a DTO round-trip before the
real native scheduler gate.

If wrapper-lambda hints cannot be generated from supported Spring AOT inputs,
native wrappers must fail at submission with an actionable message. They must
not depend on undocumented consumer hint files.

## Compatibility Contract to Build Toward

- Boot 4.1 is the primary development and native line.
- The same core starter artifact should continue to run on the latest Boot
  3.5 patch on the JVM while the compatibility matrix remains green.
- Compile shared runtime auto-configuration against only the Boot 3.5/4.1 API
  intersection. Keep Boot 4-only AOT linkage isolated.
- Use the common Jakarta JSON-B API for the default payload serializer rather
  than coupling the shared artifact to Jackson 2 or Jackson 3.
- Emit Java 17 bytecode, matching Ratchet's baseline. Use a Boot-supported
  GraalVM 25+ toolchain for Boot 4.1 native gates.
- Support PostgreSQL first, then run the same JPA TCK on MySQL, Oracle, and SQL
  Server before a general release.
- Keep SQL/JPA and Mongo dependency graphs separate from the first production
  commit.
- Preserve the clean-room boundary: do not inspect or imitate JobRunr source,
  documentation, or API shapes.

## Lessons Applied from the Quarkus Project

The Quarkus branch history makes the sequencing mistakes concrete:

- The initial extension landed on 2026-06-19 (`013f7a262`) without tests; test
  and CI productization arrived on 2026-07-24 (`b8170dc0f`).
- Persistence-agnostic core and Mongo flavors were split on 2026-07-27
  (`b0f38b426`, `5f4c59bc9`), after the original JPA shape existed.
- Native CI arrived late (`34256b4ad`), followed by native-only fixes for
  migration/configuration and job target registration (`664c31aec`,
  `9d5e0f1ba`).
- TCK wrapper lambdas had to become method references (`4725e7832`).
- Hundreds of lines of TCK fixtures were forked into the extension and later
  deduplicated (`fe696d7db`).
- The extension remains a standalone reactor without root release/BOM wiring
  at the inspected head.

The Spring project therefore starts with these guardrails:

1. Add root reactor, publication topology, version-sync, license, and CI
   wiring with the non-publishing skeleton. Record vertical-slice
   qualification separately; add artifacts to the BOM, Central eligibility,
   and release inventory only at the complete release-candidate gate.
2. Create common, JPA, and Mongo module boundaries before implementing a
   flavor.
3. Reuse `ratchet-tck-api`, `ratchet-tck-store`, and shared fixtures; never
   copy them.
4. Add a real PostgreSQL vertical slice and transaction tests with the first
   starter code, not after feature implementation.
5. Make a native claim only in the same change that adds a real per-flavor
   native CI gate.
6. Test method references, wrappers, and true ClassPolicy rejection as
   separate cases.
7. Pin Testcontainers 2.0.5 while Docker 29 compatibility requires it.
8. Use one canonical list or a drift test anywhere XML mappings,
   configuration keys, native types, or CI matrices must be repeated.
9. Use complete reactor builds or a worktree-local Maven repository so
   snapshot artifacts cannot come from another worktree.
10. Keep optional security and observability adapters outside the
    persistence-neutral core.

## Hard Stop Gates

Implementation must stop for review rather than widening scope if any of these
occurs:

- The first real PostgreSQL scheduler slice requires a consumer
  `persistence.xml`, a second EMF/persistence unit, global mapping overrides,
  or manual transaction/lifecycle glue.
- A factory-created JPA store cannot retain correct Spring transaction
  semantics without exposing or duplicating store internals.
- The common runtime handle cannot preserve the existing Jakarta lifecycle and
  transaction TCK.
- Spring cannot create every transactionally annotated core service as a
  managed, advised bean without direct application access to RI internals or a
  duplicated/forked service graph.
- Spring cannot construct secure ClassPolicy/executor defaults through the
  narrow exported runtime boundary without importing RI internals.
- Boot 3.5 support requires runtime version checks, reflective compatibility
  shims, or divergent behavior in the shared JVM artifact.
- Native execution needs tracing-agent output or unexplained consumer-local
  reachability JSON.
- Mongo brings JPA or Hibernate into its resolved runtime graph.
- A partial Spring product can enter snapshots, Central, the public BOM, or
  release inventory before the complete release-candidate gate.

None of these gates authorizes a JDBC fallback. A failed JPA gate returns to a
design discussion.

## Feasibility Verdict

Proceed, but make the portable runtime seam the first implementation phase.
The evidence supports the desired developer experience and removes JPA as the
primary risk. The remaining work is substantial but well bounded: transaction
completion, runtime lifecycle, recurring resolution, store factories, and AOT
metadata.

Do not make any Spring artifact release eligible or advertise native support
until all public-API, real-database, shutdown, dialect, native executable, and
release-candidate gates pass.
