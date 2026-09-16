# Review remediation integration coverage

This report records integration verification for the September 2026 review remediation.
The source baseline is `b073cbce397072d88e76ca557fd0d7e7c93f0d6c` plus the existing
uncommitted remediation. Existing changes and concurrent services are preserved.

## Coverage completed

The follow-up closes these gaps identified in the coverage audit:

| Area | Required proof |
| --- | --- |
| Oracle claiming | A second transaction changes eligibility between candidate selection and locked recheck |
| Completion contention | Competing completion transactions apply accounting and dependencies once |
| Archive rollback | SQL Server rolls back an earlier outer chunk when a later chunk fails |
| Recurring rollback | A later master failure rolls back earlier children and master advancement |
| Engine completion recovery | Real engine/database failure injection verifies rollback, recovery, and event boundaries |
| Public API composition | Permanent idempotency after purge and tenant-constrained recurring query/count/pagination |
| Recurring CDI dispatch | Inherited methods execute through real CDI, with and without JobContext |
| JMS | Existing embedded-Artemis recovery integration tests execute after reconnect changes |
| Jakarta runtime | Focused workflow, batch, timeout, and recurring integration tests execute in an application server |

## Evidence conventions

- Tests backed by actual databases are integration tests even when named `*Test`.
- Compiling an integration test is not evidence that it executed.
- Module/reactor totals that overlap are reported separately and are not added together.
- Negative controls must fail for the intended assertion, not a build or environment error.
- Historical results are distinguished from runs performed for this follow-up.

The clean combined reactor passed all 41 modules: **4,592 tests, 0 failures, 0 errors, 10 skips**, exit 0 in 11m20s. Focused checks below are reported separately because their coverage overlaps.
Logs for this follow-up are kept outside temporary storage at
`/home/jputney/ratchet-it-evidence-20260916/`.

## Findings exposed by the follow-up

- **WildFly batch follow-up transaction:** a post-commit callback could enter a
  `REQUIRED` method while the completed transaction remained associated with the thread.
  Live batch retry failed with `STATUS_COMMITTED` JDBC errors. Parent follow-up now starts
  a separate transaction. The same 18 WildFly/PostgreSQL ITs changed from one failure and three errors to 18 passes, without committed-transaction errors.
- **Quarkus transaction registry discovery:** JNDI-only registry lookup could miss the
  active Quarkus transaction and execute intended post-commit work immediately. A live
  injected parent-completion failure exposed rollback of child accounting. CDI registry
  discovery now uses CDI when JNDI is unavailable. PostgreSQL recovery and event-boundary integration tests pass.

- **JTA callback ordering:** independent synchronization callbacks did not preserve registration order. A real engine/database test observed a dependent event before the terminal event. One transaction-scoped FIFO callback queue now preserves ordering, discards callbacks on rollback, and isolates callback failures.

## Completed focused checks

| Check | Result | Evidence |
| --- | --- | --- |
| Clean combined reactor | 41 modules passed; 4,592 tests, 0 failures/errors, 10 skips | `root/full-reactor.log`, `root/full-reactor-results.json`, `root/full-reactor-reports/` |
| Store transaction/race scenarios | 12 distinct cases passed across all five stores, no skips; strengthened SQL cases and restored controls also passed | `stores/README.md`, `stores/commands.txt`, XML reports |
| Store negative controls | Four controls failed at intended claim/accounting/rollback assertions | `stores/oracle-negative-control.log`, `stores/sqlserver-negative-controls.log` |
| MySQL/PostgreSQL migration ITs | Final snapshot: 6 + 5 passed, no skips | `root/migration-its-final.log` |
| WildFly 39/PostgreSQL focused ITs | 18 passed, no skips on final transaction callback implementation | `integrations/jakarta-wildfly-final-fifo.log`, `integrations/results.json`, `integrations/commands.txt` |
| Quarkus/PostgreSQL completion and API ITs | 6 passed plus 19 supporting units, no skips, final dedicated profile | `engine/postgres-final-profile.log`, `engine/postgres-reports/` |
| Quarkus/MongoDB completion and API ITs | 6 passed plus 19 supporting units, no skips, normal Mongo profile command | `engine/mongo.log`, `engine/mongo-reports/` |
| Quarkus registry negative control | Removing only CDI fallback produced the intended before-commit event assertion failure | `engine/negative-cdi-lookup.log` |
| Embedded Artemis JMS transport/recovery | 21 ITs passed; 33 supporting tests passed; no skips; Maven verify exit 0 | `integrations/jms-verify.log`, `integrations/commands.txt` |
| Inherited recurring CDI discovery/execution, PostgreSQL | 2 tests passed, no skips | `root/inherited-positive.log` |
| Inherited lookup negative control | Replacing only public inherited lookup with declared-only lookup caused both tests to time out on their missing-execution assertions, with matching NoSuchMethodException runtime errors | `root/inherited-negative.log`, `root/commands.txt` |

Evidence paths in this table are relative to `/home/jputney/ratchet-it-evidence-20260916/`.

## Test locations

- Store atomic completion and recurring rollback: shared
  [`AbstractJobTerminalStoreContract`](ratchet-tck/store/src/main/java/run/ratchet/tck/store/AbstractJobTerminalStoreContract.java)
  and [`AbstractRecurringJobStoreContract`](ratchet-tck/store/src/main/java/run/ratchet/tck/store/AbstractRecurringJobStoreContract.java),
  bound to all five real database fixtures.
- Oracle claim interleavings:
  [`OracleClaimEligibilityRaceTest`](../stores/ratchet-store-oracle/src/test/java/run/ratchet/store/oracle/OracleClaimEligibilityRaceTest.java).
- SQL Server later-chunk rollback:
  [`SqlserverArchiveStoreContractTest`](../stores/ratchet-store-sqlserver/src/test/java/run/ratchet/store/sqlserver/SqlserverArchiveStoreContractTest.java).
- Real engine recovery, event boundaries, and public idempotency after direct deletion or archive/purge:
  [`QuarkusCompletionRecoveryTest`](../integrations/ratchet-quarkus/integration-tests/src/test/java/run/ratchet/quarkus/it/tck/QuarkusCompletionRecoveryTest.java).
- Authorization-constrained recurring count/filter/cursor pagination:
  [`QuarkusRecurringTenantQueryTest`](../integrations/ratchet-quarkus/integration-tests/src/test/java/run/ratchet/quarkus/it/tck/QuarkusRecurringTenantQueryTest.java).
- Inherited recurring annotation discovery and actual execution through CDI, with both signatures:
  [`InheritedRecurringMethodTest`](../integrations/ratchet-quarkus/integration-tests/src/test/java/run/ratchet/quarkus/it/InheritedRecurringMethodTest.java).
- Existing real JMS broker recovery:
  [`JmsCoordinatorContractIT`](../coordinators/ratchet-coordinator-jms/src/test/java/run/ratchet/coordinator/jms/tck/JmsCoordinatorContractIT.java),
  alongside `JmsCoordinatorOptionalContractIT` and `JmsClusterCoordinatorIT`.
- Existing managed WildFly cases: `WorkflowBranchIT`, `RiExclusiveWorkflowIT`,
  `BatchExecutionIT`, `BatchCompletionCallbackIT`, `RiBatchRetryIT`, `JobTimeoutIT`,
  `RecurringAnnotationIT`, and `RecurringPauseResumeIT`.

## Reproduction

Run from an isolated copy of the complete checkout. Commands below are separate runs;
`test` does not imply that Maven Failsafe `*IT` tests executed.

```sh
# Combined reactor, including Testcontainers store contracts and Quarkus JVM tests
mvn -B -ntp -fae clean test '-Dtest=Test*,*Test,*Tests,*TestCase,!*$*' \
  -Dsurefire.failIfNoSpecifiedTests=false

# Explicit migration integration tests outside default test-name patterns
mvn -B -ntp -pl stores/ratchet-store-mysql,stores/ratchet-store-postgresql -am test \
  -Dtest=MysqlSchemaMigratorIT,PostgresqlSchemaMigratorIT \
  -Dsurefire.failIfNoSpecifiedTests=false

# Engine/database fault injection, public API retention and tenant queries
mvn -pl integrations/ratchet-quarkus/integration-tests -am test \
  -Dtest=JobWakeupServiceTest,QuarkusCompletionRecoveryTest,QuarkusRecurringTenantQueryTest \
  -Dsurefire.failIfNoSpecifiedTests=false -Dspotless.skip=true
# Repeat the same command with -Pdb-mongo for MongoDB.

# Actual JMS broker integration tests, plus focused deterministic/Weld tests
mvn -B -ntp -pl coordinators/ratchet-coordinator-jms -am verify \
  -Dtest=JmsConnectionLifecycleTest,JmsCoordinatorCdiDeploymentTest \
  -Dsurefire.failIfNoSpecifiedTests=false \
  -Dit.test=JmsClusterCoordinatorIT,JmsCoordinatorContractIT,JmsCoordinatorOptionalContractIT \
  -Dfailsafe.failIfNoSpecifiedTests=false -Dspotless.skip=true

# Jakarta preparation: build/install only, not test evidence (JDK 21)
mvn -B -ntp -pl testing/ratchet-testsuite -am install -Ppostgresql \
  -DskipTests -DskipITs -Dspotbugs.skip=true -Dspotless.skip=true

# Actual managed WildFly/PostgreSQL ITs (JDK 21)
mvn -B -ntp -pl testing/ratchet-testsuite verify -Pwildfly-managed,postgresql \
  -Dtest=NoUnitTests -Dsurefire.failIfNoSpecifiedTests=false \
  -Dit.test=WorkflowBranchIT,RiExclusiveWorkflowIT,BatchExecutionIT,BatchCompletionCallbackIT,RiBatchRetryIT,JobTimeoutIT,RecurringAnnotationIT,RecurringPauseResumeIT \
  -Dspotbugs.skip=true -Dspotless.skip=true
```

The durable `stores/commands.txt` records exact per-method selectors and negative controls;
`engine/README.md` records PostgreSQL/Mongo profiles and transaction-registry negative controls.
Intentionally faulty controls were applied only in isolated copies and restored afterward.

## Snapshot and verification accounting

The combined run used the recorded `final-snapshot-hashes.json`. After that snapshot, the
completion test decorator was restricted to a dedicated profile and Mongo test reloadability
was configured there. Both final test profiles were rerun successfully on PostgreSQL and MongoDB
(6 ITs plus 19 units per run). Production code did not change after the combined snapshot.
Subsequent edits to owned Java files were formatting only; scoped Spotless and `git diff --check` passed.
`final-owned-source-hashes.json` records the final 23 owned Java files.

The 10 reactor skips are five optional optimistic-write fixture contracts, three unsupported
partial-index introspection contracts, and two optional deny-all authorization-provider tests.
They are recorded in `root/full-reactor-skips.json` and are not counted as passes. All new
integration scenarios and explicitly selected JMS/WildFly runs completed without skips.

The original five pre-review dirty files remained byte-for-byte unchanged during this follow-up.
Unrelated later ExplainPlanCaptureIT edits are listed in `external-changes-after-snapshot.txt`;
they were preserved and are outside this snapshot validation.

## Scope and limits

- Local executor ownership/permit races, retry-drain bounds, argument handling, and secret
  redaction retain deterministic unit coverage. Separate database tests would not strengthen
  those particular local invariants.
- Oracle validation uses a test-only tmpfs database-storage workaround to avoid Docker overlay
  disk exhaustion. The patch is retained as `stores/oracle-tmpfs.patch`; repository default
  fixture storage is unchanged.
- The application-server run is WildFly 39 with PostgreSQL, not the full supported-server/database
  matrix. Quarkus engine fault-injection scenarios cover PostgreSQL and MongoDB. No native-image
  build, process-kill durability test, production load benchmark, or browser UAT is claimed.
- Mongo fault-injection uses an application CDI decorator. Quarkus test-mode class loading must
  reload the package-private store together with its generated proxy; this is isolated to the
  dedicated completion test profile. No production visibility change is needed.
- Source snapshots and baseline hashes are retained alongside the logs. Concurrent claim-index
  work is preserved; tests against a snapshot do not establish coverage for later external edits.
- No commits, pushes, shared-container cleanup, or volume pruning were performed in this follow-up.
