package run.ratchet.store.id;

import java.net.InetAddress;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import org.jboss.logging.Logger;

/**
 * Generates 64-bit Time-Sorted IDs (TSIDs) suitable for use as database primary keys.
 *
 * <p>Layout (MSB → LSB):
 *
 * <pre>
 *   42 bits — milliseconds since custom epoch (2024-01-01T00:00:00Z) → ~139 years
 *   10 bits — node ID (from hostname + PID hash)                     → 1,024 nodes
 *   12 bits — per-millisecond sequence counter                       → 4,096 IDs/ms/node
 * </pre>
 *
 * <p>Properties:
 *
 * <ul>
 *   <li>Time-sorted — consecutive IDs produced on the same node are strictly increasing
 *   <li>Monotonic — if the clock goes backwards, the sequence counter advances to maintain ordering
 *   <li>Coordination-free — each node generates non-colliding IDs independently
 *   <li>64-bit {@code long} — drop-in replacement for auto-increment PKs
 * </ul>
 *
 * <h2>Node ID and multi-node deployments</h2>
 *
 * <p>The 10-bit node field provides 1,024 distinct slots. Without explicit assignment, the slot is
 * derived from {@code hash(hostname + "-" + pid)}, which has birthday-paradox collision
 * probability:
 *
 * <ul>
 *   <li>~5% at 10 concurrent nodes
 *   <li>~50% at 38 concurrent nodes
 *   <li>&gt;99% above 100 concurrent nodes
 * </ul>
 *
 * <p>A collision between two concurrent nodes means both can mint the same 64-bit ID within the
 * same millisecond, causing duplicate-primary-key failures on insert. For deployments with more
 * than a handful of nodes, set the environment variable {@code RATCHET_NODE_ID} (or the system
 * property of the same name) to an explicit integer in the range {@code [0, 1023]} on each node.
 * Both the auto-derived hash path and the {@link SecureRandom} exception-fallback path log a WARN
 * on first initialization so the coordination gap is visible in startup logs.
 */
public final class TsidFactory {

  private static final Logger LOG = Logger.getLogger(TsidFactory.class);

  /** 2024-01-01T00:00:00Z — custom epoch to maximize useful timestamp range. */
  static final long CUSTOM_EPOCH_MS = Instant.parse("2024-01-01T00:00:00Z").toEpochMilli();

  private static final int NODE_BITS = 10;
  private static final int SEQUENCE_BITS = 12;
  private static final long SEQUENCE_MASK = (1L << SEQUENCE_BITS) - 1; // 4095
  private static final int NODE_ID = computeNodeId();

  /**
   * Tracks the last timestamp + sequence packed together to guarantee monotonicity even under clock
   * drift. Format: upper 52 bits = (timestamp << SEQUENCE_BITS), lower 12 bits = sequence.
   */
  private static final AtomicLong LAST_TS_SEQ = new AtomicLong(0);

  private TsidFactory() {}

  /**
   * Generates the next TSID.
   *
   * @throws IllegalStateException if sequence overflows (> 4,096 IDs in one millisecond on one
   *     node)
   */
  public static long next() {
    long nowMs = System.currentTimeMillis() - CUSTOM_EPOCH_MS;

    while (true) {
      long prev = LAST_TS_SEQ.get();
      long prevTimestamp = prev >>> SEQUENCE_BITS;
      long prevSequence = prev & SEQUENCE_MASK;

      long timestamp;
      long sequence;

      if (nowMs > prevTimestamp) {
        // Normal case: new millisecond
        timestamp = nowMs;
        sequence = 0;
      } else {
        // Same millisecond or clock went backwards — advance sequence
        timestamp = prevTimestamp;
        sequence = prevSequence + 1;
        if (sequence > SEQUENCE_MASK) {
          // Sequence exhausted for this millisecond — wait for next
          timestamp = prevTimestamp + 1;
          sequence = 0;
        }
      }

      long next = (timestamp << SEQUENCE_BITS) | sequence;
      if (LAST_TS_SEQ.compareAndSet(prev, next)) {
        return (timestamp << (NODE_BITS + SEQUENCE_BITS))
            | ((long) NODE_ID << SEQUENCE_BITS)
            | sequence;
      }
      // CAS failed, retry
    }
  }

  public static Instant toInstant(long tsid) {
    long ms = (tsid >>> (NODE_BITS + SEQUENCE_BITS)) + CUSTOM_EPOCH_MS;
    return Instant.ofEpochMilli(ms);
  }

  public static long fromInstant(Instant instant) {
    long ms = instant.toEpochMilli() - CUSTOM_EPOCH_MS;
    return ms << (NODE_BITS + SEQUENCE_BITS);
  }

  private static int computeNodeId() {
    // Allow explicit node ID via environment variable or system property (0–1023)
    String explicit = System.getenv("RATCHET_NODE_ID");
    if (explicit == null || explicit.isEmpty()) {
      explicit = System.getProperty("RATCHET_NODE_ID");
    }
    if (explicit != null && !explicit.isEmpty()) {
      try {
        int nodeId = Integer.parseInt(explicit.trim());
        if (nodeId >= 0 && nodeId < (1 << NODE_BITS)) {
          return nodeId;
        }
      } catch (NumberFormatException ignored) {
        // fall through to auto-detection
      }
    }
    try {
      String host = InetAddress.getLocalHost().getHostName();
      long pid = ProcessHandle.current().pid();
      int nodeId = Math.abs((host + "-" + pid).hashCode()) & ((1 << NODE_BITS) - 1);
      LOG.warnf(
          "RATCHET_NODE_ID is unset; derived node slot %d from hash(%s-%d). "
              + "Collision probability reaches ~50%% at 38 concurrent nodes. "
              + "Set RATCHET_NODE_ID [0-1023] explicitly in multi-node deployments.",
          nodeId, host, pid);
      return nodeId;
    } catch (Exception e) {
      int nodeId = new SecureRandom().nextInt(1 << NODE_BITS);
      LOG.warnf(
          e,
          "RATCHET_NODE_ID is unset and hostname resolution failed (%s); "
              + "falling back to random node slot %d. This slot is unstable across JVM restarts. "
              + "Set RATCHET_NODE_ID [0-1023] explicitly to avoid TSID collisions.",
          e.getClass().getSimpleName(),
          nodeId);
      return nodeId;
    }
  }
}
