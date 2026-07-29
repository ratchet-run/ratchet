# Ratchet Spring Boot Extension Implementation Plan

Status: **READY FOR IMPLEMENTATION REVIEW**

Date: 2026-07-29

Baseline: `origin/main` at `c49664add6b125f8b31670eefc71ae2546a5bfcf`
(refreshed 2026-07-29 after Quarkus Support #148 merged; the feasibility
spikes ran against `f3ea377adb4bc829c4126d10976d9cd707ca0b9a`)

Decision source: `integrations/ratchet-spring-boot/FEASIBILITY.md`

Baseline-refresh deltas already on `main` that this plan absorbs:

- `RatchetProducer.nodeIdentityProvider()` no longer calls `init()`; the
  deferred-start seam (`RatchetRuntimeStart`, `ratchet.lifecycle.defer-auto-start`)
  now exists for build-time-CDI runtimes. PR 3 hardens ordering on top of
  this instead of introducing the removal.
- The caller-principal fallback is now the `run.ratchet.spi.PrincipalSource`
  SPI with the Jakarta Security implementation isolated in the published
  `ratchet-security-jakarta` module; `Instance<SecurityContext>` is gone
  from the RI. PR 7's container-fallback step binds Spring to
  `PrincipalSource`, not to a Jakarta Security shim.
- `ListenerProbe`, `RatchetTckRuntimeSupport`, and the clocked in-memory
  store now live in shared `ratchet-tck-api`/`ratchet-tck-store` (test-jar
  BOM-managed). PR 8 reuses these; it does not recreate them.
- SQL stores ship `ddl/migrations/index.txt` and `SchemaMigrator` reads it,
  so native migration discovery is index-driven. PR 10/11 register those
  index and SQL resources instead of inventing a discovery mechanism.
- `ci-required.needs` now also contains `quarkus-extension-verify` and
  `quarkus-extension-native`; the Spring job additions append to that list.
- `integrations/ratchet-quarkus` remains a standalone reactor; the Spring
  modules deliberately join the root reactor instead (guardrail 1), which
  also keeps them inside `versions:set`/`sync-version.sh` coverage.

This document is the current deliverable. No implementation PR begins until
the user reviews and approves it.

## Goal

Deliver an official Spring Boot integration with a dependency-first developer
experience after importing the existing `ratchet-bom`:

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

With an ordinary Boot datasource and a package allowlist, the application can
inject `JobSchedulerService`. It does not add `persistence.xml`,
`@EntityScan`, a second `EntityManagerFactory`, JNDI, Weld, or application
lifecycle code.

The same shared starter jars run on pinned Boot 3.5.16 and 4.1.0 JVM
consumers. Native support begins on Boot 4.1 only and is published only after a
real no-fallback PostgreSQL scheduler executable passes.

Boot 4 native consumers add one explicitly managed build-time dependency:

```xml
<dependency>
  <groupId>run.ratchet</groupId>
  <artifactId>ratchet-spring-boot-aot-spring7</artifactId>
</dependency>
```

Ordinary JVM consumers do not add that artifact. This one-dependency native
overlay is the explicit cost of keeping Spring 7 linkage off the Boot 3.5
runtime classpath.

## Locked Decisions

1. JPA is the SQL persistence strategy. There is no JDBC comparison,
   prototype, benchmark, or fallback in this work.
2. Application and Ratchet entities use one Boot-managed persistence unit and
   one `JpaTransactionManager`.
3. PostgreSQL is the first SQL vertical slice. MongoDB is the second,
   dependency-isolated flavor.
4. CDI/Jakarta behavior is a regression gate for every core portability
   change.
5. Store implementations remain package-private. A public factory is added
   only when that store's real Spring slice is implemented.
6. The persisted recurring target
   `run.ratchet.ri.cdi.RecurringMethodInvoker` remains resolvable.
7. The shared JVM artifacts compile against the Boot 3.5/Spring 6.2
   intersection and emit Java 17 bytecode.
8. Spring Framework 7-only AOT code is isolated in
   `ratchet-spring-boot-aot-spring7`; ordinary starters do not depend on it.
9. TCK contracts and fixtures are extended or moved, never copied.
10. Actuator endpoints, dashboards, a UI, Spring Security integration,
    observability adapters, and alternate coordinators are outside the first
    release.
11. The starter uses Boot's JSON-B support and Yasson for its default
    `PayloadSerializer`; common code does not bind to Jackson 2 or Jackson 3.
12. Public Ratchet events bridge synchronously to
    `ApplicationEventPublisher`. The initial principal fallback is empty;
    applications can use the existing `CallerPrincipalResolver`.
13. The clean-room boundary is absolute: do not inspect or imitate JobRunr
    source, documentation, tests, artifacts, or API shapes.
14. Technical qualification and release eligibility are separate states.
    Every Spring artifact remains deploy-skipped, Central-excluded, absent
    from the public BOM, and absent from release inventory until the final
    release-candidate gate.

## Stop-and-Discuss Gates

When a gate trips, stop that PR and every dependent PR. Record the exact
evidence and return to the user. A gate never authorizes JDBC work.

| Gate | First owner | Stop condition |
|---|---|---|
| JPA simplicity | PR 6 and PR 8 | Consumer `persistence.xml`, Ratchet `@EntityScan`, mapping override, second EMF/PU/TM, application transaction/lifecycle glue, or exposed/duplicated store internals are required. |
| Store management | PR 6 | A factory-created store cannot be initialized exactly once and receive Spring transaction advice through normal bean lifecycle. |
| Managed core graph | PR 2 and PR 7 | A core transactional service cannot be a normally advised Spring bean without scanning/importing unexported RI packages, retaining raw collaborators, changing global AOP strategy, duplicating the graph, or adding a generic DI facade to core. |
| Runtime defaults | PR 7 | Spring cannot construct the secure default `ClassPolicy`, executor ownership, or another required default through a narrow exported runtime factory without naming an unexported RI implementation. |
| Jakarta parity | Every core PR | The common runtime/transaction/recurring seam cannot preserve the existing Jakarta testsuite and JPMS behavior. |
| Boot compatibility | Every JVM matrix PR | Boot 3.5 requires runtime version checks, reflection-based compatibility shims, a separately compiled shared jar, or behavior different from Boot 4.1. |
| Native metadata | PR 10 and PR 11 | Native execution needs tracing-agent output, hand-authored consumer reachability JSON, unexplained consumer hints, or an unisolated Spring 7 linkage. |
| Mongo isolation | PR 9 | Mongo resolves JPA, Hibernate, EclipseLink, Spring JDBC, a SQL store/driver, or another CDI implementation. |
| Release topology | PR 15 | A production Spring artifact can reach snapshots, Central, the public BOM, or release inventory before the complete release-candidate manifest is approved. |

## Architecture Contract

Dependencies point inward:

```text
Spring configuration and lifecycle adapters
                    |
                    v
small portable SPIs + component catalog + runtime orchestration
                    ^
                    |
              Jakarta/CDI adapters
```

The boundaries enforce SOLID properties:

- Transaction completion, bean lifetime, recurring discovery, managed
  component registration, runtime lifecycle, persistence construction, and
  AOT are separate responsibilities.
- Core services depend on narrow interfaces, typed lists/optionals, and
  `JobStore.capability(...)`, not Spring, JNDI, `BeanManager`, or a service
  locator.
- Every dependency between advised components is proxy-safe. Inject an
  interface where a JDK proxy is possible; move a self-invoked transaction
  boundary into a separately managed collaborator instead of retaining a raw
  `this` fallback.
- CDI and Spring run the same behavioral contracts.
- Only `run.ratchet.ri.runtime` is newly exported from `run.ratchet.ri`.
  It contains the incubating runtime/catalog contracts and a narrow
  `RatchetRuntimeDefaults` factory for secure existing defaults such as the
  package-prefix `ClassPolicy`. Existing `cdi`, `core`, `security`, and
  `internal` packages remain unexported.

Container-managed construction is mandatory. Ratchet has many Jakarta
`@Transactional` core services. A factory that directly constructs the whole
graph would bypass Spring AOP. A common `RatchetRuntimeComponentCatalog`
therefore describes Ratchet-owned component types, CDI discovers them through
its normal mechanism, and Spring registers them as normal bean definitions.
The common runtime orchestrates already-managed components.

The catalog must describe enough constructor and role metadata for the
container to create each target and apply post-processors; it must not return
an already-wired raw graph. The graph conversion explicitly repairs known
proxy violations: concrete `DefaultJobCreationService` dependencies become
proxy-safe interface dependencies, and `BatchService` recovery's
`NOT_SUPPORTED` → `REQUIRES_NEW` self-call moves to a separately managed
transactional collaborator. A source-derived test inventories every
catalogued `@Transactional` type, injection edge, and self-call so this is a
closed repair list rather than a one-time audit.

## Module and Publication Topology

Create all boundaries in PR 1, but publish none of them. The topology is
machine-checked by
`integrations/ratchet-spring-boot/publication-topology.json`.

| Path | Artifact ID | Packaging | Pre-RC release state | Technical qualification |
|---|---|---:|---|---|
| `integrations/ratchet-spring-boot` | `ratchet-spring-boot-parent` | `pom` | deploy skipped; Central excluded; no public BOM/release entry | PR 8 |
| `ratchet-spring-boot-autoconfigure` | same as directory | `jar` | same | PR 8 |
| `ratchet-spring-boot-autoconfigure-jpa` | same as directory | `jar` | same | PR 8 |
| `ratchet-spring-boot-starter` | same as directory | `jar` | same | PR 8 |
| `ratchet-spring-boot-autoconfigure-mongodb` | same as directory | `jar` | same | PR 9 |
| `ratchet-spring-boot-starter-mongodb` | same as directory | `jar` | same | PR 9 |
| `ratchet-spring-boot-aot-spring7` | same as directory | `jar` | same | PR 11 |
| `integration-tests` and every child | exact IDs below | `pom`/`jar` | permanently deploy skipped and Central excluded | never |

Technical qualification changes only the manifest's evidence state. PR 15 is
the sole release unlock: it atomically enables the seven production
coordinates, removes their Central exclusions, and adds the six jars to the
public BOM/release inventory. An ordinary Ratchet release before PR 15 cannot
publish or reference a partial Spring product.

The permanent non-published/Central-excluded artifact IDs are:

- `ratchet-spring-boot-integration-tests`;
- `ratchet-spring-boot-it-compatibility`;
- `ratchet-spring-boot-it-postgresql`;
- `ratchet-spring-boot-it-mongodb`;
- `ratchet-spring-boot-it-native-postgresql`;
- `ratchet-spring-boot-it-mysql`;
- `ratchet-spring-boot-it-oracle`; and
- `ratchet-spring-boot-it-sqlserver`.

All coordinates use group `run.ratchet` and the reactor version. The Spring
parent inherits `run.ratchet:ratchet-parent` with
`<relativePath>../../pom.xml</relativePath>`. Each production child inherits
`ratchet-spring-boot-parent` with `<relativePath>../pom.xml</relativePath>`;
the integration-test aggregator does the same, and each test child inherits
that aggregator with `<relativePath>../pom.xml</relativePath>`. The Spring
parent manages all six production sibling jars at `${project.version}` and
the topology test checks every parent, relative path, and managed version.
The AOT child alone imports the pinned Boot 4.1/Spring 7 build baseline; that
management cannot leak into an ordinary starter.

The SQL starter supplies Boot's standard JPA starter but no dialect store.
The application adds exactly one `ratchet-store-*` dependency. The Mongo
starter supplies Boot's standard Mongo starter and `ratchet-store-mongodb`.
These Boot starter dependencies arrive with their behavior, not with the
skeleton: PR 6 adds `spring-boot-starter-data-jpa` to the SQL starter and
PR 9 adds the Mongo starter dependency. The PR 1 starter jars carry no
Boot starter dependency, so an empty-skeleton consumer triggers no JPA or
datasource auto-configuration.

The parent owns pinned Boot 3.5.16 and 4.1.0 test properties. Shared production
code compiles only against 3.5.16. Testcontainers remains owned by the root
`testcontainers.version` property at 2.0.5.

## Canonical Automation Files

- `publication-topology.json` is the source checked against module POM
  packaging/parents, deploy state, `ratchet-bom`, Central exclusions, release
  jar/SBOM collection, and snapshot eligibility. Each production coordinate
  carries `requiredQualificationScenarios`, `snapshotEligible`,
  `centralEligible`, `bomManaged`, and `releaseInventory` fields plus one
  repository-wide `releaseReady`.
- Qualification is derived, never a hand-maintained boolean.
  `verify.sh` emits a same-run `qualification-attestation.json` containing the
  commit SHA, reproducible production-jar hashes, managed runtime-dependency
  hashes, scenario/report digests, and toolchain identity. A changed artifact,
  POM, or dependency makes the attestation inapplicable. The topology and
  release-candidate checks reject stale/missing attestations or any
  eligibility flag before `releaseReady`.
- `integration-tests/compatibility-matrix.json` owns Boot versions, flavors,
  Java runtime, and expected same-jar hashes.
- `integration-tests/scenario-manifest.json` maps every verification scenario
  to its Boot lanes, database, shared TCK contracts, expected report classes,
  source sentinels, exact conformance-artifact path/name, and required CI job.
- `integration-tests/tck-coverage.json` maps each applicable shared
  `ratchet-tck-{api,store,util}` contract to every flavor. A package/source
  duplication sentinel rejects copied TCK fixtures or assertions.
- `integration-tests/native-postgresql/native-toolchain.properties` pins the
  resolved Paketo builder/run image digests and required Liberica NIK 25.x
  version beginning in PR 10. Do not use a floating image tag.
- `integration-tests/verify.sh <scenario>` creates one temporary Maven
  repository, traps its cleanup, passes its absolute path through every Maven
  and child-script invocation, runs a complete root
  `-DskipTests clean install`, and then executes the scenario from the
  manifest without `-am`. It rejects the shared user repository and asserts
  the expected Surefire/Failsafe reports.
- `integration-tests/verify-jvm-matrix.sh` builds production jars once into a
  repository supplied by `verify.sh`, hashes them, and runs consumers without
  `-am`.
- `scripts/test-spring-boot-publication-topology.sh` fails closed on drift.
  It also enumerates the actual module directories under
  `integrations/ratchet-spring-boot` and fails when any Spring module on
  disk lacks a `publication-topology.json` entry, so a future module (for
  example the PR 12–14 dialect test children) cannot exist unguarded.
  Each of PR 12, 13, and 14 extends the manifest for its new module in the
  same PR; the directory-enumeration check enforces this.
- `scripts/verify-spring-boot-release-candidate.sh` uses a temporary checkout,
  temporary Maven/deployment repositories, and an ephemeral signing key. It
  exercises `versions:set`, stages without the Central profile/API, and
  compares every expected POM, jar, sources jar, Javadoc jar, SBOM, signature,
  BOM entry, exclusion, and release asset with `publication-topology.json`.

Add every Spring Maven directory to `.github/dependabot.yml`. Add an Enforcer
or dependency-tree assertion that the Spring test graph resolves
Testcontainers 2.0.5.

Every command below goes through `integration-tests/verify.sh`; CI invokes the
same scenario. No command may accept a matching snapshot from the shared user
repository or another worktree.

## Required CI Jobs

Add these jobs to `.github/workflows/ci.yml` as their owning PR lands, and add
each exact job ID to `ci-required.needs` in the same PR:

| Job ID | Runtime | Exact scenario | First owner |
|---|---|---|---|
| `spring-boot-topology` | Temurin 17 | `topology` | PR 1 |
| `spring-boot-core-compat` | Boot 3.5.16/4.1.0, Temurin 17, identical jars; PostgreSQL 16 from PR 4 | `core-compat` | PR 2 |
| `spring-boot-postgresql-jvm` | Boot 3.5.16/4.1.0, Temurin 17, PostgreSQL 16 | `postgresql-store`, expanded to `postgresql-runtime` | PR 6 |
| `spring-boot-mongodb-jvm` | Boot 3.5.16/4.1.0, Temurin 17 | `mongodb-runtime` | PR 9 |
| `spring-boot-aot` | Boot 4.1.0, pinned NIK 25.x | `aot-preflight` | PR 10 |
| `spring-boot-native-postgresql` | Linux, pinned NIK 25.x | `native-postgresql` | PR 11 |
| `spring-boot-sql-dialects-jvm` | dialect × Boot version, Temurin 17 | `dialect-*` | PR 12 |
| `spring-boot-release-candidate` | Temurin 17 plus ephemeral GPG key | `release-candidate` | PR 15 |

Jobs must exist on every CI event and produce an explicit no-op success when
path filtering says their expensive body is unnecessary; the required
aggregate must not become skipped through a skipped dependency.

The `changes` job gains one `spring-boot` output covering the Spring tree plus
core, stores, TCK, root POM, release, and CI inputs. Each required job itself
stays present and returns an explicit no-op success when that output says its
expensive body is unnecessary.

## Delivery Sequence

| PR | Increment | Publication |
|---:|---|---|
| 1 | Non-publishing product skeleton and topology checks | none |
| 2 | Minimal runtime/component-catalog seam and advised-bean proof | none |
| 3 | Common lifecycle orchestration and CDI parity | none |
| 4 | Portable transaction contracts and after-commit seam | none |
| 5 | Managed bean lifetime and recurring portability | none |
| 6 | PostgreSQL factory and one-EMF store slice | none |
| 7 | Spring defaults, configuration, serialization, and full managed graph | none |
| 8 | Full PostgreSQL scheduler/TCK/Boot matrix | mark SQL artifacts technically qualified only |
| 9 | Full isolated Mongo flavor/matrix | mark Mongo artifacts technically qualified only |
| 10 | Spring 7 AOT discovery and payload preflight | none |
| 11 | Real Boot 4 PostgreSQL native scheduler | mark Spring 7 AOT artifact technically qualified only |
| 12 | MySQL real scheduler matrix | existing Spring artifacts unchanged |
| 13 | Oracle real scheduler matrix | existing Spring artifacts unchanged |
| 14 | SQL Server real scheduler matrix | existing Spring artifacts unchanged |
| 15 | Documentation and release-candidate audit | atomically enable all qualified production artifacts |

Each PR has at most three task groups below. Tests and CI for its behavior land
with that behavior.

## PR 1 — Non-Publishing Product Skeleton

### Tasks

1. Add the parent, six production modules, compatibility/PostgreSQL/Mongo/native
   test children, root reactor entry, Java 17 compile baseline, annotation
   processors, `AutoConfiguration.imports`, and empty context smoke tests.
2. Add `publication-topology.json`, the topology test, permanent Central
   exclusions for test artifacts, temporary exclusions/deploy skips for every
   production artifact, and all Maven directories to Dependabot. Do not add a
   Spring artifact to the BOM or release jar inventory.
3. Add `verify.sh`, `scenario-manifest.json`, `tck-coverage.json`,
   `compatibility-matrix.json` (both Boot lanes with expected same-jar
   hashes), `verify-jvm-matrix.sh`, the pinned Boot profiles,
   metadata-presence checks, report-count and Testcontainers resolution
   assertions, and `spring-boot-topology`.

### Acceptance

- A root reactor install reaches every new POM.
- Snapshot/Central dry-run topology proves no Spring artifact can deploy.
- Both consumers load the same empty auto-configuration jar.
- Configuration and condition metadata files exist.
- No behavior, database, or native claim exists.

### Verification

```bash
integrations/ratchet-spring-boot/integration-tests/verify.sh topology
```

## PR 2 — Runtime Catalog and Managed-Bean Proof

### Tasks

1. Add a narrowly exported incubating `RatchetRuntime` handle and
   `RatchetRuntimeComponentCatalog`; add JPMS/architecture tests that prevent
   exporting or naming existing RI internals.
2. Add deterministic `RatchetBeanDefinitionRegistrar` consumption of a
   representative catalog entry. Prove one class-level transactional service,
   one `REQUIRES_NEW` method, and one downstream collaborator receive advised
   references with both Spring proxy strategies.
3. Add the same representative component to CDI discovery and a drift test
   comparing CDI/Spring component sets to the catalog. Make `core-compat` run
   identical installed hashes on Boot 3.5.16 and 4.1.0, then add
   `spring-boot-core-compat` to `ci-required.needs`.

### Stop

Apply the managed-core-graph, Jakarta-parity, and Boot-compatibility gates.

### Acceptance

- Core has no Spring dependency and Spring sources do not import unexported RI
  packages.
- Managed advice and propagation work with
  `spring.aop.proxy-target-class=true` and `false`.
- No downstream object retains the raw target.
- Both Boot lines pass the managed-bean scenario from byte-identical
  production jars on Java 17.
- Existing Jakarta and JPMS suites pass.

### Verification

```bash
integrations/ratchet-spring-boot/integration-tests/verify.sh core-compat
```

## PR 3 — Common Lifecycle Orchestration

### Tasks

1. Move startup/stop orchestration into the catalogued `RatchetRuntime`
   implementation. Accept already-managed hooks/components and a narrow
   recurring-registration callback; do not construct transactional services.
2. Building on main's producer no longer calling
   `DefaultNodeIdentityProvider.init()` and the existing
   `RatchetRuntimeStart` deferred-start seam, enforce in the common
   runtime: migration/before hooks → recurring reconciliation → node
   initialization/recovery → workers/timers/listener → after hooks. Reverse
   owned work on bounded, idempotent shutdown.
3. Move serializer/encryption/masking holder installation into runtime
   lifecycle with owner tokens. Sequential contexts must not leak state;
   simultaneous runtimes in one classloader fail instead of overwriting state.
   Make CDI observers thin delegates and extend the required `core-compat`
   scenario with the lifecycle contracts.

### Stop

Apply the Jakarta-parity gate before PR 4 begins.

### Acceptance

- Migration failure creates no node row or worker thread.
- Hook priority/pairing, dependent destruction, worker drain, and schema
  failure semantics remain unchanged.
- Repeated start/stop is safe and no Ratchet thread survives.
- Existing Jakarta and JPMS suites pass.

### Verification

```bash
integrations/ratchet-spring-boot/integration-tests/verify.sh core-compat
```

## PR 4 — Portable Transaction Semantics

### Tasks

1. Move the shared assertions from
   `testing/ratchet-tck/jakarta` transaction contracts into
   `testing/ratchet-tck/api/.../transaction`, centered on a
   `RatchetTransactionDriver`. Keep thin Jakarta `UserTransaction` and Spring
   `TransactionTemplate` drivers; do not copy assertions.
2. Add exported `run.ratchet.spi.AfterCommitRegistrar` with exact
   `NO_ACTIVE_TRANSACTION`, `REGISTERED`, and
   `ACTIVE_TRANSACTION_REGISTRATION_FAILED` outcomes. Keep TSR/JNDI in one
   Jakarta adapter and use Spring synchronization state in one Spring adapter.
3. Inject the registrar into `JobWakeupService`,
   `DefaultJobCreationService`, `DefaultJobSchedulerService`, `BatchService`,
   `ChainScheduler`, `DeadLetterService`, `JobCascadeService`, and
   `JobTimeoutHandler`. Add a source sentinel limiting TSR lookup and
   interposed synchronization to the Jakarta adapter, update
   `tck-coverage.json`, and extend `core-compat` with both Boot transaction
   drivers.

### Stop

Apply the Jakarta-parity and Boot-compatibility gates.

### Acceptance

- Commit publishes only after the row is visible; rollback and failed
  registration publish nothing.
- `REQUIRES_NEW`, `NOT_SUPPORTED`, `SUPPORTS`, required, and self-invocation
  controls have explicit shared assertions.
- Failure-before-DLQ event order is preserved.

### Verification

```bash
integrations/ratchet-spring-boot/integration-tests/verify.sh core-compat
```

No targeted command disables fail-on-no-tests. CI additionally asserts the
expected transaction report classes exist.

## PR 5 — Managed Bean Lifetime and Recurring Portability

### Tasks

1. Keep `BeanResolver.resolve(Class<T>)` as the single abstract method and add
   a default closeable managed handle. CDI destroys dependent handles; Spring
   destroys prototypes and leaves singletons alive.
2. Split recurring discovery from validation/registration. CDI adapts its
   extension/`BeanManager`; Spring uses bean definitions and stable user
   classes. Use `JobStore.capability(RecurringJobStore.class)`.
3. Keep `run.ratchet.ri.cdi.RecurringMethodInvoker` as the persisted shim,
   delegate portable invocation through the managed handle, and test CGLIB,
   JDK proxies, inherited methods, idempotent reconciliation, ClassPolicy, and
   cleanup on success/failure. Extend the required `core-compat` scenario with
   these contracts on both Boot lines.

### Stop

Apply the Jakarta-parity, managed-core-graph, and Boot-compatibility gates.

### Verification

```bash
integrations/ratchet-spring-boot/integration-tests/verify.sh core-compat
```

## PR 6 — PostgreSQL Factory and JPA Truth

### Tasks

1. Add only
   `run.ratchet.store.postgresql.PostgresqlJobStoreFactory`. It returns the
   public interface and a ready store; make existing initialization idempotent
   so standalone factory and Spring lifecycle cannot double-initialize.
2. Add isolated PostgreSQL JPA auto-configuration. Adapt the EMF with
   `SharedEntityManagerCreator`, create the factory result as a normal Spring
   bean, infer PostgreSQL migration dialect when unset, and reject conflicts.
3. Run Boot 3.5.16 and 4.1.0 consumers—with and without application entities—
   from the same installed jars. Assert one EMF/TM, dependency-jar
   `META-INF/orm.xml`, lifecycle once, transaction proxying, and concurrent
   commit/rollback. Introduce `spring-boot-postgresql-jvm` with the
   `postgresql-store` scenario and add it to `ci-required.needs`.

### Stop

Evaluate the JPA-simplicity, store-management, and Boot-compatibility gates
before approval. If any fail, discuss with the user. Do not begin JDBC work.

### Verification

```bash
integrations/ratchet-spring-boot/integration-tests/verify.sh postgresql-store
```

## PR 7 — Spring Defaults and Full Managed Graph

### Tasks

1. Add public `RatchetConfigCatalog`; bind ordinary keys through a Spring
   `Binder`-backed `RatchetConfigSource` and `RatchetOptionsFactory`. Keep only
   `ratchet.enabled` and
   `ratchet.transaction-manager-bean-name` in Spring-specific
   `@ConfigurationProperties`. Add
   `ratchet.class-policy.allowed-packages` and
   `ratchet.class-policy.allowed-result-type-packages` as canonical
   `RatchetConfigKeys`/`RatchetOptions.SecurityOptions` values used by both
   containers. Include fixed keys and generated execution-type/profile
   descriptors. The metadata drift test compares against the union of the
   canonical catalog and exactly those two Spring-only descriptors. Back off
   for user `RatchetOptions` and implement unique, `@Primary`, or explicit
   transaction-manager selection with actionable
   missing/ambiguous/wrong-EMF failures.
2. Add the JSON-B/Yasson serializer and use exported
   `RatchetRuntimeDefaults` to create the default `ClassPolicy` from
   `ratchet.class-policy.allowed-packages` and
   `ratchet.class-policy.allowed-result-type-packages`. Add an owned
   non-JNDI Spring `ExecutorProvider`, no-op metrics/tracing/coordinator
   defaults, policies/providers, explicit caller-principal precedence
   (the container-fallback step resolves `run.ratchet.spi.PrincipalSource`
   beans, matching the CDI default provider), and a synchronous Spring
   event bridge. Every user-owned bean backs off and is
   never closed by Ratchet.
3. Complete catalog registration of the core graph with mock store
   capabilities. Replace concrete `DefaultJobCreationService` injection edges
   with proxy-safe interfaces; extract `BatchService`'s
   `NOT_SUPPORTED` → `REQUIRES_NEW` recovery call into a managed collaborator;
   repair every additional edge found by the transactional graph inventory.
   Run the complete graph under both proxy settings and extend required
   `core-compat`.

### Acceptance

- JSON-B round-trips representative payloads on both Boot lines.
- A user `PayloadSerializer` and a user `Jsonb` back off correctly; a
  close-counting user `Jsonb` with container destruction disabled is never
  closed by Ratchet.
- `ratchet.enabled=false` creates no Ratchet runtime/store beans, threads, or
  Ratchet schema/store work. A user `RatchetOptions` replaces the bound
  default.
- Transaction-manager selection covers one candidate, `@Primary`, explicit
  bean name, missing name, ambiguous candidates, wrong type, and a
  `JpaTransactionManager` bound to the wrong EMF, with concrete remediation.
- Empty invocation packages fail startup unless the existing
  `ratchet.allow-empty-class-policy=true` test-only opt-in is explicit.
  Prefix validation, hardcoded gadget denials, invocation permission, and the
  separate result-type allowlist pass through the exported factory.
- The default executor path performs no JNDI lookup and closes its owned
  platform/scheduled pools exactly once; a user `ExecutorProvider` is neither
  replaced nor closed.
- Dependency trees show Ratchet adds neither Jackson generation. MVC and
  WebFlux consumers retain Boot's preferred HTTP mapper before/after Ratchet.
- Principal precedence remains: configured resolver → bound `JobContext`
  principal → container fallback → empty. Tests cover nested submission,
  per-call evaluation, null/empty, and exceptions.
- Spring events are delivered once and a closed context is not retained.
- Every catalogued transactional target and downstream reference is advised;
  the batch recovery propagation path proves it never calls a raw target.

### Stop

Apply managed-core-graph, runtime-defaults, Jakarta-parity, and
Boot-compatibility gates.

### Verification

```bash
integrations/ratchet-spring-boot/integration-tests/verify.sh core-compat
```

## PR 8 — Full PostgreSQL Scheduler and Qualification Gate

### Tasks

1. Add one high-phase auto-start `SmartLifecycle`. Start after singleton
   creation but before `ApplicationRunner`/`CommandLineRunner`; migrate before
   node/workers. Complete `stop(Runnable)` only after bounded drain while the
   EMF/datasource remain available. Add a runner that submits a real job.
2. Add `SpringRatchetTckRuntime`, profile, store cleaner, clocked runtime,
   and thin subclasses of shared API/transaction contracts, reusing the
   shared `ratchet-tck-api` `ListenerProbe`/`RatchetTckRuntimeSupport` and
   the `ratchet-tck-store` clocked test-jar rather than adding new copies.
   Run persistence, claims, retries, recurring, events, ClassPolicy, repeated
   contexts, and shutdown against PostgreSQL 16 on both Boot lines from the
   same jars. Complete PostgreSQL entries in `tck-coverage.json` and upload
   `spring-boot-postgresql-conformance` with fail-on-missing semantics.
3. Expand the existing required `spring-boot-postgresql-jvm` job from
   `postgresql-store` to `postgresql-runtime`. After all gates pass, mark the
   parent/common/JPA/starter technically qualified by emitting the
   current-commit reproducible-hash attestation required by topology, but keep
   deployment skipped, Central exclusions present, and BOM/release inventory
   absent.

### Stop

Re-evaluate every JPA, managed-graph, Jakarta-parity, and Boot-compatibility
gate. Release eligibility remains disabled regardless of the result.

### Verification

```bash
integrations/ratchet-spring-boot/integration-tests/verify.sh postgresql-runtime
```

## PR 9 — Isolated MongoDB Flavor

### Tasks

1. Add `MongoJobStoreFactory` and Mongo auto-configuration. Preserve UUID
   validation and collection/index initialization; do not install SQL
   migration behavior.
2. Extract or publish the existing Mongo test fixture for shared use, then run
   the real scheduler and applicable shared contracts on both Boot lines from
   the same Mongo starter jar. Record every applicable/unsupported contract
   with rationale in `tck-coverage.json`; do not copy a fixture or assertion.
   Upload `spring-boot-mongodb-conformance` with fail-on-missing semantics.
3. Parse the runtime dependency graph against an allowlist. Reject all
   `ratchet-store-{postgresql,mysql,oracle,sqlserver}`, Spring JDBC, JDBC
   drivers, `jakarta.persistence`, Hibernate, EclipseLink, and CDI
   implementations. After the gate passes, mark the two Mongo artifacts
   technically qualified through their current-commit/hash attestation but
   release-disabled, and add
   `spring-boot-mongodb-jvm` to the required aggregate.

### Stop

Apply Mongo-isolation, managed-graph, Jakarta-parity, and Boot-compatibility
gates. Release eligibility remains disabled regardless of the result.

### Verification

```bash
integrations/ratchet-spring-boot/integration-tests/verify.sh mongodb-runtime
```

## PR 10 — Spring 7 AOT Discovery and Payload Preflight

### Tasks

1. Add
   `RatchetBeanFactoryInitializationAotProcessor implements
   BeanFactoryInitializationAotProcessor` in `aot-spring7`, registered under
   the interface key in `META-INF/spring/aot.factories`. It reads final bean
   definitions without instantiating them, normalizes user classes, limits
   discovery to configured ClassPolicy packages, and consumes recurring
   descriptors/component catalog.
2. Its contribution registers:
   - invocation reflection for eligible target/recurring methods;
   - Spring 7 `LambdaHint` entries for allowed application bean declaring
     classes and Ratchet's serializable functional interfaces;
   - bounded, cycle-safe JSON-B reflection for allowed job parameter, result,
     and nested payload types discovered from those methods;
   - declaring-class resources for inline-wrapper ASM; and
   - a generated Ratchet AOT manifest used to reject an unregistered
     declaring class before payload persistence.
3. Pin builder/run digests and Liberica NIK 25.x in
   `native-toolchain.properties`, then run a Boot 4.1 no-store native preflight
   for JSON-B DTO round-trip, direct invocation, bound/static method
   reference, inline wrapper, and actionable pre-persistence rejection. Make
   the developer script and `spring-boot-aot` job consume and assert the same
   toolchain, then add that job to `ci-required.needs`. Keep the artifact
   release-disabled and label this preflight—not native scheduler support.

### Stop

Apply the native-metadata gate. If supported Spring inputs cannot make method
references work automatically for Spring bean submission sites, stop and
discuss; do not require consumer hint files.

### Verification

```bash
integrations/ratchet-spring-boot/integration-tests/verify.sh aot-preflight
```

## PR 11 — Real Boot 4 Native PostgreSQL Scheduler

### Tasks

1. Reuse the PR 10 builder/run digests and Liberica NIK 25.x contract. Make
   the full developer script and `spring-boot-native-postgresql` job fail if
   the resolved toolchain differs or fallback is enabled.
2. Run a real Boot 4.1 executable against PostgreSQL through migration, node
   registration, persistence, claim, direct/method-reference/wrapper
   submission, retry history, recurring execution, completion, JSON-B
   round-trip, and clean shutdown. Test creation/invocation ClassPolicy denial
   with otherwise reachable types.
3. After the executable and Boot 3.5 JVM regression pass, emit the
   current-commit/hash qualification attestation for `aot-spring7`, keep it
   release-disabled, and add the native job to the required aggregate.
   Documentation continues to say Boot 4 only for native.

### Stop

Apply the native-metadata and Boot-compatibility gates. No tracing agent,
consumer-local metadata, or Boot 3.5 native claim is allowed.

### Verification

```bash
integrations/ratchet-spring-boot/integration-tests/verify.sh native-postgresql
```

## PR 12 — MySQL

Add `MysqlJobStoreFactory`, isolated MySQL conditional auto-configuration, and
`integration-tests/mysql`. Run schema migration, store contracts,
transactions, full scheduler, recurring, events, and shutdown against a real
MySQL container on both Boot lines. Because this changes the already-qualified
JPA jar, invalidate its evidence and rerun PostgreSQL plus MySQL on both Boot
lines against the new identical jar hash before recording renewed
qualification. Create
`spring-boot-sql-dialects-jvm` with the `mysql` cells and add it to
`ci-required.needs`. Add the MySQL shared-contract mapping to
`tck-coverage.json`.

```bash
integrations/ratchet-spring-boot/integration-tests/verify.sh dialect-mysql
```

Apply JPA simplicity, store management, Jakarta parity, and Boot compatibility
gates before PR 13.

## PR 13 — Oracle

Add `OracleJobStoreFactory`, isolated Oracle conditional auto-configuration,
and `integration-tests/oracle`. Run the same contracts against the real Oracle
fixture on both Boot lines, including its `NOT_SUPPORTED` initialization
boundary. Invalidate JPA evidence, run PostgreSQL, MySQL, and Oracle against
the new identical jar hash, then renew qualification. Add the `oracle` cells
to the required dialect job and coverage manifest.

```bash
integrations/ratchet-spring-boot/integration-tests/verify.sh dialect-oracle
```

Apply the same gates before PR 14.

## PR 14 — SQL Server

Add `SqlserverJobStoreFactory`, isolated SQL Server conditional
auto-configuration, and `integration-tests/sqlserver`. Run the same contracts
against the real SQL Server fixture on both Boot lines. Invalidate JPA
evidence, run PostgreSQL, MySQL, Oracle, and SQL Server against the new
identical jar hash, then renew qualification. Add the `sqlserver` cells to the
required dialect job and coverage manifest.

```bash
integrations/ratchet-spring-boot/integration-tests/verify.sh dialect-sqlserver
```

The dialect job uploads
`spring-boot-{mysql,oracle,sqlserver}-conformance` as three distinct,
fail-on-missing artifacts. Together with the PostgreSQL and Mongo jobs, these
are the five exact conformance artifacts required by release.

## PR 15 — Documentation and Release-Candidate Audit

### Tasks

1. Write BOM-based PostgreSQL and Mongo quickstarts, full configuration
   metadata, multiple-TM remediation, supported overrides, dialect matrix,
   Boot JVM matrix, the explicit Spring 7 AOT dependency, and Boot 4-only
   native limits. Do not add Actuator, security, UI, or coordinator scope.
2. Generate fresh projects outside the reactor and run PostgreSQL/Mongo plus
   one application-owned-entity/no-entity pair. Verify no application glue,
   hidden hints, snapshot leakage, or accidental RI imports.
3. Add `scripts/verify-spring-boot-release-candidate.sh` and
   `spring-boot-release-candidate`. Atomically mark `releaseReady`, enable
   deployment for the parent and six production jars, remove their Central
   exclusions, and add the six jars to the public BOM/release inventory.
   Stage to a temporary file repository—never the Central API—and verify
   signatures/sources/Javadocs/SBOMs, version synchronization, dependency
   review, full Jakarta suites, every Spring matrix, and every database
   conformance report before adding the job to `ci-required.needs`. The
   scenario requires exact, nonempty `spring-boot-postgresql-conformance`,
   `spring-boot-mongodb-conformance`, `spring-boot-mysql-conformance`,
   `spring-boot-oracle-conformance`, and
   `spring-boot-sqlserver-conformance` paths plus qualification attestations
   tied to the release commit and jar hashes. Make
   `.github/workflows/release.yml` run the same non-publishing verifier
   successfully before its first Central API call.

No general release is approved until PostgreSQL, MySQL, Oracle, SQL Server,
and Mongo all pass their required real-database JVM gates.

### Stop

Apply the release-topology gate. Any mismatch keeps every Spring artifact
release-disabled and requires user review.

### Verification

```bash
integrations/ratchet-spring-boot/integration-tests/verify.sh release-candidate
```

## Per-PR Checklist

Every PR description answers:

- Does this change preserve JPA-only/no-JDBC scope?
- Did a hard stop trigger?
- Did CDI/Jakarta and JPMS gates run when core/module code changed?
- Are all new behaviors tested in the same PR?
- Is every production artifact still release-disabled before PR 15, or
  atomically enabled by the complete release-candidate gate?
- Did a shared TCK/fixture move or extend instead of being copied?
- Does common code remain free of Spring and unexported RI access?
- Does the Boot matrix use the exact same installed jar hashes?
- Are configuration/artifact/CI lists canonical or drift-checked?
- Was the complete reactor or a temporary worktree-local Maven repository
  used?
- Was the JobRunr clean-room boundary honored?

## Global Exit Criteria

1. PostgreSQL and Mongo meet the dependency-first, no-glue contract.
2. Every SQL flavor uses one Boot-managed EMF/TM containing application and
   Ratchet entities.
3. Transaction completion and propagation pass shared Jakarta/Spring
   contracts.
4. Migration precedes node registration, runners, and workers.
5. Recurring compatibility, proxy normalization, and managed-bean destruction
   pass.
6. Boot 3.5.16 and 4.1.0 run identical shared JVM artifacts on Java 17.
7. Boot 4.1 native passes the real no-fallback PostgreSQL executable gate.
8. Mongo contains no JPA/SQL/CDI implementation dependency.
9. No starter reaches unexported RI internals or embeds Weld.
10. PostgreSQL, MySQL, Oracle, SQL Server, and Mongo each have a required CI
    conformance artifact.
11. Publication topology, BOM, release assets, Central exclusions, snapshots,
    licenses, SBOMs, version updates, and Dependabot paths are consistent.
12. Unsupported configurations fail at startup or submission with concrete
    remediation.
13. No hard stop was waived implicitly, no JDBC work was performed, and the
    clean-room constraint was preserved.
14. A non-publishing temporary-repository release-candidate job passes before
    any Central API call, and only the parent plus six qualified production
    jars become release eligible.
