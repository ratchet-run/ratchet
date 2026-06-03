---
id: index
title: TCK Conformance
sidebar_label: TCK Conformance
---

# TCK Conformance

Ratchet defines three conformance tiers. Each tier is independently verifiable by third-party
implementors — a store author does not need to satisfy Jakarta Runtime contracts to claim Store
compatibility.

## Conformance Tiers

| Tier | Module | What it proves |
|------|--------|----------------|
| **Store Compatible** | `ratchet-tck-store` | The `JobStore` SPI implementation correctly persists, queries, and manages job state across 11 required core contracts, plus 15 optional capability contracts reported `N/A` when the store does not advertise the capability, covering CRUD, lifecycle, locking, archival, signals, analytics, and schema conformance |
| **API Compatible** | `ratchet-tck-api` | The `JobSchedulerService` API implementation correctly handles job submission, lifecycle, retry, cancel, delayed scheduling, and idempotency without a Jakarta EE container |
| **Jakarta Runtime Compatible** | `ratchet-tck-api` + `ratchet-tck-jakarta` | The runtime passes both API contracts and CDI injection, CDI event, and JTA transaction contracts in a live Jakarta EE container |

**Ratchet RI Verified** is the project's own claim: the reference implementation passes all three
tiers on a published matrix of servers and databases.

## Store Conformance Reports

The reports below are generated automatically during CI from the test runs of each store module.
Each report lists every store contract with its pass/fail/missing/N/A status, grouped by Core,
Behavioral, and Advanced categories. An optional capability contract is reported `N/A` when the
store under test does not advertise that capability.

| Store | Report |
|-------|--------|
| MySQL | [MySQL Store Conformance](./mysql) |
| PostgreSQL | [PostgreSQL Store Conformance](./postgresql) |
| MongoDB | [MongoDB Store Conformance](./mongodb) |

## API and Jakarta Runtime Reports

Results are published for all 15 server × database combinations (WildFly, WildFly EE 11, Payara,
Open Liberty, GlassFish × MySQL, PostgreSQL, MongoDB) and regenerated after each successful CI run
on `main`.

- [API Conformance Matrix](./api/) — Tier 2 results across all runtimes
- [Jakarta Runtime Conformance Matrix](./jakarta/) — Tier 3 results across all runtimes

## Running Conformance Tests Locally

**Store tier:**
```bash
mvn test -pl ratchet-store-mysql -am      # generates ratchet-store-mysql/target/tck-conformance-report.md
mvn test -pl ratchet-store-postgresql -am
mvn test -pl ratchet-store-mongodb -am
```

**API + Jakarta Runtime tier:**
```bash
mvn verify -P wildfly-managed,mysql -pl ratchet-testsuite -am
# generates ratchet-testsuite/target/tck-api-conformance-report.md
#           ratchet-testsuite/target/tck-jakarta-conformance-report.md
```

## Implementing a Conformant Store

To claim **Ratchet Store Compatible**, add `ratchet-tck-store` as a test dependency and extend
every abstract contract class. The `ConformanceReportExtension` listener activates automatically
and writes `target/tck-conformance-report.md` at the end of each test run. Inject your store
name so the report header is meaningful:

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
