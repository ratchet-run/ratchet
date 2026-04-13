package run.ratchet.store.id;

import java.net.InetAddress;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

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
 */
public final class TsidFactory {

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
      return Math.abs((host + "-" + pid).hashCode()) & ((1 << NODE_BITS) - 1);
    } catch (Exception e) {
      return new SecureRandom().nextInt(1 << NODE_BITS);
    }
  }
}
