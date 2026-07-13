---
title: Adopting the TCK
description: Run Ratchet's store, API, and Jakarta Runtime conformance contracts against another implementation.
---

# Adopting the TCK

Ratchet publishes contract classes for the three product/runtime compatibility tiers covered here.
They are usable by another implementation, but the amount of integration work differs by tier:

| Claim | External adoption status | Implementor supplies |
|-------|--------------------------|----------------------|
| **Ratchet Store Compatible** | Available from Maven Central | A `JobStoreContractFixture` and one concrete subclass for every applicable store contract |
| **Ratchet API Compatible** | Available from Maven Central, but not turnkey | A `RatchetTckRuntime`, a `RatchetTckProbe`, and concrete subclasses for all registered API contracts |
| **Ratchet Jakarta Runtime Compatible** | Available from Maven Central, but deployment-specific | The API-tier bridge, an Arquillian container adapter and deployment, and subclasses for all Jakarta contracts |
| **Ratchet RI Verified** | Project-only | Nothing. This label describes Ratchet's own server/database matrix and is not a fourth self-certification tier. |

The TCK does not provide a command that discovers an implementation or a ready-made runtime bridge.
It provides public abstract JUnit contracts and report listeners. Your test suite connects those
contracts to your implementation.

::: tip Current release
The `ratchet-bom`, `ratchet-tck-store`, `ratchet-tck-api`, `ratchet-tck-jakarta`, and
`ratchet-tck-util` version `0.1.1` artifacts are available from
[Maven Central](https://repo.maven.apache.org/maven2/run/ratchet/ratchet-bom/0.1.1/). The dependency
examples below use that release.

This page tracks the current source tree, which can contain contracts added after `0.1.1`. The
generated report from the artifact you run is authoritative for that release. To test unreleased
contracts from `main`, install the matching source into your local Maven repository:

```bash
mvn -pl :ratchet-tck-store,:ratchet-tck-api,:ratchet-tck-jakarta -am -DskipTests install
```

Then use the version from that source tree instead of `0.1.1`.
:::

## Common Maven setup

Import the Ratchet BOM so the TCK, API, and store artifacts use one version:

```xml
<dependencyManagement>
  <dependencies>
    <dependency>
      <groupId>run.ratchet</groupId>
      <artifactId>ratchet-bom</artifactId>
      <version>0.1.1</version>
      <type>pom</type>
      <scope>import</scope>
    </dependency>
  </dependencies>
</dependencyManagement>
```

Add only the TCK artifact for the tier you are testing. The `ratchet-tck` artifact is a Maven
aggregator, not a dependency for test code. The BOM manages these consumable artifacts:
`ratchet-tck-store`, `ratchet-tck-api`, `ratchet-tck-jakarta`, and the shared
`ratchet-tck-util` module.

`ratchet-tck-coordinator` is a separate component-level contract suite for
`ClusterCoordinator` implementations. It is not another step in the Store, API, and Jakarta
product/runtime tier stack covered here.

## Store Compatible

### 1. Add the store contracts

```xml
<dependency>
  <groupId>run.ratchet</groupId>
  <artifactId>ratchet-tck-store</artifactId>
  <scope>test</scope>
</dependency>
```

The store under test must also put its persistence APIs and test backend on the test classpath.
For example, a JPA store needs its JPA provider and transaction environment; the TCK declares the
Jakarta Persistence, JSON, and Transactions APIs as `provided` rather than choosing those pieces
for the implementor.

### 2. Implement the common fixture

`JobStoreContractFixture` has four required methods:

- `store()` returns the configured store, including its transaction wrapper when one is needed.
- `newPendingJob()` returns a valid, immediately persistable job for the target schema.
- `newBatchParentJob()` returns a valid batch-parent job.
- `cleanupStore()` removes data from every persistence surface touched by the contracts.

Keep that setup in one helper and delegate to it from each contract subclass:

```java
final class MyJobCrudStoreTest extends AbstractJobCrudStoreContract {
    private final MyStoreFixture fixture = MyStoreFixture.start();

    @Override
    public JobStore store() {
        return fixture.store();
    }

    @Override
    public JobEntity newPendingJob() {
        return fixture.newPendingJob();
    }

    @Override
    public JobEntity newBatchParentJob() {
        return fixture.newBatchParentJob();
    }

    @Override
    public void cleanupStore() {
        fixture.cleanupStore();
    }
}
```

The inherited contract invokes `cleanupStore()` before and after its cases. Cleanup must run
through the configured persistence environment and cover every surface the contracts can touch.

### 3. Subclass the complete registry

The source registry separates required contracts from conditional capability and persistence-model
profiles. A report is incomplete when a required contract has no concrete subclass. Conditional
contracts are `N/A` when the store does not advertise the corresponding capability or when the
contract applies only to a different persistence model.

Most contracts use `JobStoreContractFixture`. These conditional profiles need additional adapters:

| Contract | Additional seam |
|----------|-----------------|
| `AbstractRecurringJobStoreContract` | `RecurringJobStore`, `TagStore`, a no-op `JobPayload`, the common fixture, and recurring-data cleanup |
| `AbstractJpaRecurringClaimConcurrencyContract` | `JpaContainerFixture`, `RecurringJobStore`, a no-op payload, and cleanup |
| `AbstractSchemaConformanceContract` | A JDBC connection and `DialectTypeMapper` |
| `AbstractSchemaMigratorContract` | A `DataSource`, `SchemaMigrationDialect`, database reset, and a new JDBC connection |
| `AbstractJobStoreTransactionBoundaryContract` | The concrete JPA store implementation class whose annotations are inspected |

The optional capability accessors on `JobStoreContractFixture` call `store().capability(...)`.
When a capability is absent, the inherited cases abort as JUnit skips and the report records the
contract as `N/A`; absence is not a conformance failure.

Use `ConformanceLevel.getRequiredContracts()` and `ConformanceLevel.getOptionalContracts()` as the
authoritative registry. The report listener cannot distinguish an omitted applicable conditional contract
from one that does not apply when no concrete subclass runs; both appear as `N/A`. You must create a
subclass for every capability the store advertises and every persistence-model profile it uses.

See [Store SPI: Custom Persistence](/advanced/spi-implementation#store-spi-custom-persistence) for
the capability model, the `LockStore` lease rules, and transaction-boundary requirements.

### 4. Name the implementation and run the suite

The report listener is registered through JUnit Platform's service-provider mechanism. Set the
store name in Surefire so the report identifies the implementation:

```xml
<plugin>
  <groupId>org.apache.maven.plugins</groupId>
  <artifactId>maven-surefire-plugin</artifactId>
  <configuration>
    <systemPropertyVariables>
      <ratchet.tck.store.name>${project.artifactId}</ratchet.tck.store.name>
    </systemPropertyVariables>
  </configuration>
</plugin>
```

Then run the test suite:

```bash
mvn test
```

When at least one recognized store contract runs, the listener writes
`target/tck-conformance-report.md`. Check the final claim in that file; running one passing
contract is not enough, because the report marks unexecuted required contracts as `MISSING`.

## API Compatible

The API contracts are container-neutral JUnit classes. They can run in a plain JVM when the
implementation can boot its scheduler there. An implementation that requires a container may run
the same contracts through its own integration-test harness. Ratchet's reference implementation,
for example, currently runs them inside its Arquillian deployment.

### 1. Add the API contracts

```xml
<dependency>
  <groupId>run.ratchet</groupId>
  <artifactId>ratchet-tck-api</artifactId>
  <scope>test</scope>
</dependency>
```

### 2. Implement the runtime bridge and probe

`RatchetTckRuntime` is the implementation boundary. It provides:

- the live `JobSchedulerService` under test;
- a `RatchetTckProbe` that observes lifecycle events by `JobHandle`;
- an optional `TestClock` for contracts that need deterministic time;
- `supportsCallerTransactionRollback()`, which non-JTA stores return as `false` so rollback-only
  Jakarta cases report `N/A` without hard-coded store names; and
- `clear()`, which quiesces work, force-cancels pending retries, pauses or deschedules recurring
  work, resets probe state, and leaves the scheduler usable for the next test.

The full seven-part drain contract is on `RatchetTckRuntime.clear()`. In particular, `clear()` must
join active workers and throw on timeout instead of returning while state can leak into the next
case.

`RatchetTckProbe` is deliberately implementation-neutral. Map your scheduler's events to
`ProbeEvent` values, implement the bounded `await*` methods, count `STARTED` events as invocations,
and drop late events after `clear()`.

```java
final class MyJobLifecycleTest extends AbstractJobLifecycleContract {
    private static final RatchetTckRuntime RUNTIME = MyTckBootstrap.start();

    @Override
    protected RatchetTckRuntime runtime() {
        return RUNTIME;
    }
}
```

Create one concrete subclass for every registered contract. Sharing one runtime object is fine if
its `clear()` implementation satisfies the isolation contract.

### 3. Supply the specialized API fixtures

The generated API conformance report is the authoritative registry: unexecuted entries remain
`MISSING` until a concrete subclass runs. Most contracts need only `runtime()`, but these contracts
expose additional seams:

| Contract | Additional seam |
|----------|-----------------|
| `AbstractJobAuthorizationContract` | Optionally, a scheduler configured with a deny-all policy |
| `AbstractJobQueryContract` | A working `JobQueryService` |
| `AbstractJobQueryDenialContract` | A `JobQueryService` configured to deny reads |
| `AbstractPayloadEncryptionEngineContract` | A new encryption engine and two distinct keys |
| `AbstractResilienceStrategyContract` | A strategy instance and a way to force its circuit open |
| `AbstractRetryPolicyContract` | An optional runtime configured with a custom retry policy; return `Optional.empty()` to report this profile `N/A` |

Returning `Optional.empty()` from `RatchetTckRuntime.clock()` makes deterministic-time cases `N/A`.
Omitting the concrete contract subclass altogether makes the contract `MISSING`.

### 4. Run and inspect the API report

Set `ratchet.tck.runtime.name` in Surefire or Failsafe, then run the phase that owns your concrete
tests:

```xml
<systemPropertyVariables>
  <ratchet.tck.runtime.name>my-runtime</ratchet.tck.runtime.name>
</systemPropertyVariables>
```

The service-loaded listener writes `target/tck-api-conformance-report.md` after a run that exercises
at least one recognized API contract. The claim is complete only when the report's required
contract inventory passes or is explicitly reported `N/A` by a contract's supported assumption.

## Jakarta Runtime Compatible

The Jakarta tier is not a separate runtime implementation. It adds 6 live-container contracts to
the API tier: CDI scheduler injection, CDI event observation, transactional enqueue, and the
documented `NOT_SUPPORTED`, `REQUIRED`, and `SUPPORTS` API transaction behavior. A Jakarta Runtime
Compatible claim requires both the API and Jakarta reports.

The current source Jakarta report registry contains **6 contracts**.

### 1. Add the Jakarta contracts and a container adapter

```xml
<dependency>
  <groupId>run.ratchet</groupId>
  <artifactId>ratchet-tck-jakarta</artifactId>
  <scope>test</scope>
</dependency>
```

This artifact brings in the API contracts and Arquillian JUnit integration. It intentionally does
not choose an application-server adapter or configure a server. Add the managed, remote, or
embedded Arquillian adapter for the server you certify.

### 2. Build the deployment

Each concrete test must use `ArquillianExtension` and provide a `@Deployment`. The archive must
contain:

- the implementation and its runtime dependencies;
- the `run.ratchet.tck.api` and `run.ratchet.tck.jakarta` contract packages used by the tests;
- your `RatchetTckRuntime` and `RatchetTckProbe` implementations;
- `CdiEventCollector` for `AbstractCdiEventContract`; and
- a bean archive descriptor so CDI can discover the scheduler, adapter, probe, and collector.

```java
@ExtendWith(ArquillianExtension.class)
final class MyCdiInjectionIT extends AbstractCdiInjectionContract {
    @Inject
    MyRatchetTckRuntime runtime;

    @Override
    protected RatchetTckRuntime runtime() {
        return runtime;
    }

    @Deployment
    public static WebArchive deployment() {
        return MyTckArchive.deployment();
    }
}
```

The deployment builder is implementation-specific, so the TCK cannot supply a universal
`MyTckArchive`. Package the same bridge in all six Jakarta subclasses and keep server configuration
in the adopter's own Arquillian profile.

### 3. Run both tiers and retain both reports

Run the adopter's Failsafe profile, with `ratchet.tck.runtime.name` set to a stable server/runtime
identity. The listeners write:

- `target/tck-api-conformance-report.md`
- `target/tck-jakarta-conformance-report.md`

Publish both reports with the tested implementation version, application server, Java version, and
database. Ratchet's own matrix pages are examples of project-generated evidence, not a registration
service for third-party claims.

## What the TCK does not supply

The current public seams make external conformance possible, but they do not remove implementation
work. There is no reusable event probe, generic runtime bootstrap, generic Arquillian deployment,
or separate fourth-tier runtime harness. The reference implementation's `RiRatchetTckRuntime`,
event probe, and deployment builder live in its test suite and are examples rather than published
TCK APIs.
