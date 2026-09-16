# Recurring orphan cleanup: database follow-up

The retry repair preserves MySQL lock SQL and all other dialect implementations.
Registration publishes its discovered keys once; cleanup retries within the same
10-attempt registration budget, with a 500 ms managed scheduling delay.

## Transaction documentation

Audit the LockStore transaction documentation separately. Its tryLock Javadoc
specifies REQUIRED and participation in the caller's transaction, while
MysqlJobStoreImpl.tryLock uses REQUIRES_NEW. MysqlNodeLockOperations now describes
that actual independent transaction. Review the facade annotations and comments
across dialects before changing the SPI contract or transaction behavior.

## Evidence required before changing SQL

Obtain the affected production transaction's isolation level and the production
deadlock graph. Prior local database probes alone do not establish the production
cause, and unit tests of this retry repair do not validate production locking.

Any later upsert proposal must prove ownership correctness with both Connector/J
affected-row settings and concurrent acquisition. Test contention, expiration,
and same-owner renewal, including that an unsuccessful acquirer cannot report
ownership or release another node's lease.
