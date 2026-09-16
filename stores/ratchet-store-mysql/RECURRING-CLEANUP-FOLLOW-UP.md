# Recurring orphan cleanup: database follow-up

Registration publishes its discovered keys once; cleanup retries within the same
10-attempt registration budget, with a 500 ms managed scheduling delay.

## Transaction documentation

Audit the LockStore transaction documentation separately. Its tryLock Javadoc
specifies REQUIRED and participation in the caller's transaction, while
MysqlJobStoreImpl.tryLock uses REQUIRES_NEW. MysqlNodeLockOperations now describes
that actual independent transaction. Review the facade annotations and comments
across dialects before changing the SPI contract or transaction behavior.

## MySQL lease acquisition

The local ten-node stress test reproduced a lease deadlock at READ-COMMITTED.
The captured graph shows concurrent `INSERT IGNORE` attempts for
`signalTimeoutScan` while a lease row is being released. A separate integration
regression also reproduced the deadlock without automatic transaction retries.

Acquisition now creates the row with a no-op `ON DUPLICATE KEY UPDATE`, which takes
an exclusive lock on an existing row, then conditionally updates its owner and TTL.
Both statements run in the facade's existing REQUIRES_NEW transaction. Only the
conditional update decides whether acquisition succeeded; the upsert's row count
is ignored because Connector/J can report a matched, unchanged row as affected.
This adds a statement to same-owner reacquisition. Other SQL dialects are unchanged.

The live retest still found a delete/reinsert deadlock involving insert-intention
locks after this change. SingletonLeaseService therefore retries transient
acquisition failures up to three times through the store facade. SQL facades start
a fresh REQUIRES_NEW transaction on each attempt. Contention returns immediately;
non-transient failures are not retried, and exhausted attempts retain an error log.
This recovery applies to any store that reports RatchetTransientStoreException.

Integration coverage runs the production SQL with `useAffectedRows=true` and
`false`, at both READ COMMITTED and REPEATABLE READ, without the shared fixture's
automatic retry wrapper. The concurrent
release/reacquisition loop explicitly allows at most three fresh transaction
attempts, matching the caller's recovery budget. It checks contention,
expiration, same-owner reacquisition, rejected renewal/release by a different node,
and concurrent acquisition/release with at most one owner.

## Avoid idle timeout lease writes

The shared timeout handler probes for one expired signal wait before acquiring
`signalTimeoutScan`. An empty result ends that scan without writing a lease row.
A nonempty result still requires the lease, followed by a fresh batch read before
processing. Probe results are not cached or processed directly. This applies to
all SQL stores through the existing SignalStore API and adds one limited read
when expired work exists.

These changes require no new deployment settings, isolation-level changes, or
schema migrations. The load harness's optional MySQL buffer-pool setting is only
for performance experiments; its default remains 128 MiB.

## Production follow-up

The local reproduction does not establish the cause of the earlier production
incident. Obtain that transaction's isolation level and deadlock graph before
attributing it to this pattern. Keep transient failures visible and retain bounded
recovery at callers that need it; this change does not promise that all MySQL
deadlocks are impossible.
