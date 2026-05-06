package run.ratchet.store.spi;

import java.time.Duration;
import run.ratchet.api.Incubating;

/**
 * Expiring store-backed lease operations for cluster-wide coordination.
 *
 * <h2>Semantic model: best-effort coordination with caller idempotency</h2>
 *
 * <p>This SPI provides a best-effort distributed lease primitive. It is <em>not</em> a
 * strict-exclusive lock. Implementations MUST reject a second acquirer for the same {@code name}
 * while a lease is live, but they MUST NOT be relied upon to prevent writes from a previous holder
 * whose lease has expired and been reacquired by another node. Such stale-holder writes are a
 * recognised failure mode of expiring-lease coordination; fencing tokens are intentionally omitted
 * from this SPI because atomically verifying a monotonic token on every protected write is beyond
 * what many practical stores can provide without bespoke optimistic-lock machinery.
 *
 * <p>Callers MUST make every operation performed under a lease idempotent with respect to
 * concurrent execution by a subsequent lease holder. In practice, ratchet's protected writes
 * (CAS-based status transitions, upsert-with-version, {@code INSERT...ON DUPLICATE KEY UPDATE}
 * patterns) are idempotent by construction. The specification follows JSR-352 {@code JobRepository}
 * §10 and MicroProfile Fault Tolerance {@code @Retry}, both of which rely on caller idempotency in
 * place of infrastructure fencing.
 *
 * <p><b>Stale-holder recovery path.</b> When a protected write by a former holder commits after the
 * lease has been reacquired, recovery is the responsibility of the framework's stale-RUNNING
 * detection (orphan reset on startup, periodic heartbeat sweeps). The {@code REQUIRES_NEW}
 * transaction boundary on post-execution handlers ensures stale writes do not invalidate the
 * completion ack recorded by the current holder.
 *
 * <h2>Clock source</h2>
 *
 * <p>Implementations SHOULD derive expiry timestamps from a server-side clock (for example, SQL
 * {@code CURRENT_TIMESTAMP} / {@code NOW(3)} / {@code statement_timestamp()}, or MongoDB {@code
 * $$NOW} inside aggregation-pipeline updates) rather than a client-side clock. Client-side clocks
 * produce split-brain lease acquisition under NTP drift: a skewed client observes a live lease as
 * expired and overwrites it, violating the single-holder invariant at the moment it matters most.
 *
 * <h2>Thread-safety</h2>
 *
 * <p>Implementations MUST be thread-safe. The framework invokes {@code tryLock} concurrently from
 * multiple nodes and periodically re-invokes {@code renewLock} from a scheduled executor while work
 * runs under the lease.
 *
 * @see Incubating
 */
@Incubating
public interface LockStore {

  /**
   * Attempts to acquire the named lease for the specified node with the given time-to-live.
   *
   * <p>Implementations MUST perform acquisition atomically: if two callers race for the same {@code
   * name}, at most one MUST observe {@code true} and the other(s) MUST observe {@code false}.
   * Expiry comparison SHOULD use a server-side clock (see class Javadoc).
   *
   * <p>A previously-held, expired lease MUST be reclaimable by any caller — implementations MUST
   * NOT require the previous holder to {@link #unlock(String, String)} before expiry allows
   * reacquisition.
   *
   * @param name the lease name; MUST be non-null and non-empty
   * @param ttl the time-to-live from the moment of acquisition; MUST be positive
   * @param nodeId identifier of the acquiring node; MUST be non-null
   * @return {@code true} if the lease was acquired by this caller; {@code false} otherwise (lease
   *     currently held by another live owner)
   */
  boolean tryLock(String name, Duration ttl, String nodeId);

  /**
   * Releases the named lease if and only if it is currently held by the specified node.
   *
   * <p>This method MUST be a no-op when invoked with a {@code nodeId} that does not match the
   * current holder. Specifically, it MUST NOT release a lease held by a different owner (for
   * example, after this caller's lease expired and another node acquired). Implementations MUST NOT
   * throw in the non-owner case.
   *
   * @param name the lease name; MUST be non-null and non-empty
   * @param nodeId identifier of the releasing node; MUST be non-null
   */
  void unlock(String name, String nodeId);

  /**
   * Extends the named lease's expiry by the given duration, if and only if it is currently held by
   * the specified node.
   *
   * <p>Implementations MUST verify current ownership atomically with the expiry update. When the
   * caller is no longer the owner — because its lease expired and another node acquired, or because
   * a cluster operation reassigned ownership — this method MUST return {@code false} without
   * modifying any state. A {@code false} return is the signal to the caller that it MUST stop
   * performing work under this lease.
   *
   * <p>Implementations MUST NOT throw in the non-owner case; a {@code false} return is the
   * contractual response.
   *
   * @param name the lease name; MUST be non-null and non-empty
   * @param extension duration to add to the current expiry; MUST be positive
   * @param nodeId identifier of the renewing node; MUST be non-null
   * @return {@code true} if the lease was still owned by {@code nodeId} and was extended; {@code
   *     false} if the caller has lost ownership
   */
  boolean renewLock(String name, Duration extension, String nodeId);
}
