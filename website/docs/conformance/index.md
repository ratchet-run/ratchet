---
id: index
title: TCK Conformance
sidebar_label: TCK Conformance
---

# TCK Conformance

Ratchet defines three product/runtime conformance tiers. Each tier exposes public abstract
contracts that another implementation can run. The API and Jakarta tiers require an
implementation-specific runtime bridge, and the Jakarta tier also requires an Arquillian
deployment. A store author does not need to satisfy Jakarta Runtime contracts to claim Store
compatibility.

The separate `ratchet-tck-coordinator` artifact tests `ClusterCoordinator` implementations. It is a
component-level contract suite, not another step in the Store -> API -> Jakarta tier stack.

## Conformance Tiers

| Tier | Module | What it proves |
|------|--------|----------------|
| **Store Compatible** | `ratchet-tck-store` | The `JobStore` SPI implementation passes every required contract and every applicable conditional contract in the source registry. Unsupported capabilities and persistence-model-specific profiles are reported `N/A`. |
| **API Compatible** | `ratchet-tck-api` | The `JobSchedulerService` implementation passes the container-neutral API contracts. The adopter chooses how to bootstrap the implementation, which may still require its normal runtime environment. |
| **Jakarta Runtime Compatible** | `ratchet-tck-api` + `ratchet-tck-jakarta` | The runtime passes both API contracts and CDI injection, CDI event, and JTA transaction contracts in a live Jakarta EE container |

**Ratchet RI Verified** is the project's own claim: the reference implementation passes all three
tiers on a published matrix of servers and databases. It is not a fourth third-party tier and has
no separate TCK artifact.

See [Adopting the TCK](./adopting-the-tck) for the dependencies, fixtures, runtime adapters,
Arquillian packaging, report paths, and current limits of third-party self-certification.

## Store Conformance Reports

The reports below are generated automatically during CI from the test runs of each store module.
Each report lists every store contract with its pass/fail/missing/N/A status, grouped by Core,
Behavioral, and Advanced categories. An optional capability contract is reported `N/A` when the
store under test does not advertise that capability.

| Store | Report |
|-------|--------|
| MySQL | [MySQL Store Conformance](./mysql) |
| PostgreSQL | [PostgreSQL Store Conformance](./postgresql) |
| Oracle | [Oracle Store Conformance](./oracle) |
| SQL Server | [SQL Server Store Conformance](./sqlserver) |
| MongoDB | [MongoDB Store Conformance](./mongodb) |

## API and Jakarta Runtime Reports

Results are published for WildFly, WildFly EE 11, Payara, Open Liberty, and GlassFish across MySQL,
PostgreSQL, Oracle, SQL Server, and MongoDB, and regenerated after each successful CI run on `main`.

- [API Conformance Matrix](./api/) -- Tier 2 results across all runtimes
- [Jakarta Runtime Conformance Matrix](./jakarta/) -- Tier 3 results across all runtimes

## Running Conformance Tests Locally

**Store tier:**
```bash
mvn test -pl :ratchet-store-mysql -am      # generates stores/ratchet-store-mysql/target/tck-conformance-report.md
mvn test -pl :ratchet-store-postgresql -am
mvn test -pl :ratchet-store-oracle -am
mvn test -pl :ratchet-store-sqlserver -am
mvn test -pl :ratchet-store-mongodb -am
```

**API + Jakarta Runtime tier:**
```bash
mvn verify -P wildfly-managed,mysql -pl :ratchet-testsuite -am
# generates ratchet-testsuite/target/tck-api-conformance-report.md
#           ratchet-testsuite/target/tck-jakarta-conformance-report.md
```

## Implementing a Conformant Store

To claim **Ratchet Store Compatible**, add `ratchet-tck-store` as a test dependency, implement
`JobStoreContractFixture`, and extend every required and applicable abstract contract class. The
`ConformanceReportExtension` listener activates automatically and writes
`target/tck-conformance-report.md` at the end of each recognized test run. Inject your store name
so the report header is meaningful:

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

The complete adapter map and the distinction between `MISSING` and `N/A` are in
[Adopting the TCK](./adopting-the-tck#store-compatible).
