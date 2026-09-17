# SQL store performance changes

These optimizations require no new deployment settings or schema migrations.

## Shared paths

- **Job completion:** the existing locked queue-row read includes the attempt
  count. If the completion plan has the same count, the store skips the redundant
  update. Changed counts are still written before the terminal transition, in
  the same transaction. MySQL, PostgreSQL, Oracle and SQL Server use this path.
- **Inline-lambda submission:** parsed bytecode is cached by context class loader
  and class name, up to 128 classes per loader. Captured arguments are resolved
  anew for each submission. Loader keys are weak so the cache does not keep an
  undeployed application alive; different loaders do not share parsed classes.
  This optimization also applies to non-SQL stores. Failed bytecode reads are
  retried on subsequent submissions.
- **Signal timeout scans:** a one-row probe avoids acquiring a lease when there
  is no expired work. A nonempty probe still requires lease ownership and a fresh
  batch read before processing. Empty results are not cached.
- **Singleton leases:** transient acquisition failures receive at most three
  attempts. SQL facades use a fresh transaction for each attempt. Contention and
  non-transient failures are not retried; exhausted attempts remain logged.

MySQL additionally replaces `INSERT IGNORE` lease creation with a no-op upsert
followed by an owner/expiry-checked update. Only the conditional update decides
ownership. See [lease evidence and limitations](ratchet-store-mysql/RECURRING-CLEANUP-FOLLOW-UP.md).

## Measuring the result

Compare clean builds with identical data, warm-up, workload and deployment
settings. Measure completed jobs through queue drain, not only HTTP acceptance.
See the [load-test comparison procedure](../infra/loadtest/README.md#comparing-library-changes).

The code reduces work in specific paths. Actual gains depend on workload and
which resource limits throughput; deployment tuning remains optional advice.

## Controlled no-op workload, 2026-09-16

Two clean-build trials per variant and database, with a fresh database and ten
application nodes for each trial. Each trial processed 10,000 warm-up jobs and
100,000 measured inline-lambda jobs. Worker counts, connection pools, database
settings and client concurrency were identical between variants. MySQL used its
original 128 MiB load-test buffer pool and READ COMMITTED isolation. These earlier
results did not establish support or throughput at REPEATABLE READ.

| Database | Baseline completed jobs/s | Candidate completed jobs/s | Combined gain |
| --- | ---: | ---: | ---: |
| MySQL | 978–1,003 | 1,036–1,037 | 4.7% |
| PostgreSQL | 1,354 | 1,380–1,402 | 2.7% |

Enqueue p95 fell from 413–423 ms to 389–394 ms on MySQL and from 584–599 ms to
539–556 ms on PostgreSQL. All 880,000 jobs, including warm-ups, succeeded. All
sixteen k6 stages exited 0. Measured MySQL deadlocks fell from twelve to zero;
one candidate warm-up had a recovered claim deadlock. PostgreSQL had no deadlocks.

These are small gains for the combined patch in this workload, not a prediction
for every application. The trials ran on a shared 32-CPU VM and used no-op jobs;
I/O-heavy jobs may see little change. There were two repeats per variant, not a
statistical confidence study. SQL Server and Oracle passed native completion
contracts; no throughput claim is made for those dialects.

## MySQL default isolation

The MySQL store now accepts both REPEATABLE READ and READ COMMITTED at startup.
Ordinary and recurring claims use a nonlocking candidate lookup followed by primary-key
locking reads that recheck eligibility. The claim update also forces the primary
key: a captured REPEATABLE READ deadlock showed MySQL scanning the priority index
for an update of only two IDs, locking unrelated pending jobs near queue drain.
Contended candidates are skipped; each call
examines at most sixteen candidate pages before returning its available claims. The
next poll can retry. At READ COMMITTED, results are deduplicated if concurrent inserts
or priority changes shift pages.

Resource-limit checks lock the resource, then read current permits. A caller's older
REPEATABLE READ snapshot must not hide permits acquired by other transactions.

The other SQL stores retain their existing isolation checks and claim SQL. No schema
migration, server setting, connection setting, or new public option is needed for
MySQL's default isolation. Historical migration files retain their original text and
checksums; the canonical schema comment and current deployment guides describe the
current library.

See MySQL's [locking-read documentation](https://dev.mysql.com/doc/refman/8.0/en/innodb-locking-reads.html)
and [lock scope rules](https://dev.mysql.com/doc/refman/8.0/en/innodb-locks-set.html).

### Controlled isolation comparison, 2026-09-16

The corrected library was tested in four fresh ten-node trials, ordered REPEATABLE
READ, READ COMMITTED, READ COMMITTED, REPEATABLE READ. Each used the same clean WAR,
128 MiB MySQL buffer pool, durability settings, worker counts, connection pools and
client settings. Each processed 10,000 warm-up jobs, then 100,000 measured no-op jobs.
The database server supplied isolation; neither the JDBC URL nor the datasource
forced it. Live application sessions were checked before and after each stage.

| Isolation | Completed jobs/s, two trials | Combined jobs/s | Enqueue p95 | Job completion p95 |
| --- | ---: | ---: | ---: | ---: |
| REPEATABLE READ | 952; 941 | 946 | 410–419 ms | 57.09–57.77 s |
| READ COMMITTED | 977; 975 | 976 | 403–406 ms | 54.39–55.11 s |

REPEATABLE READ's combined rate was 3.0% below READ COMMITTED. All 440,000 jobs,
including warm-ups, succeeded; all eight k6 stages exited 0. Neither isolation had
a deadlock, lock timeout, HTTP failure, or terminal lease failure. No overlapping
Maven/test processes were observed. These results support using MySQL's default
isolation without a required deployment change. They do not establish equal
performance for all workloads or a production SLA.

An earlier prototype exposed claim-update deadlocks near queue drain. Its results
are retained separately and are excluded from this final comparison. Forcing primary-key
access in the claim UPDATE fixed the captured access-path problem. The final application
logs contain only the existing WildFly/CDI startup warnings. MySQL reported nine
redo-capacity pressure warnings across the four trials, with database settings kept
fixed. Additional database tuning remains optional.
