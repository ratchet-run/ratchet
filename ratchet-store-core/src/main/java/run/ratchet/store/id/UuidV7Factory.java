package run.ratchet.store.id;

import java.security.SecureRandom;
import java.util.UUID;

/**
 * RFC 9562 §5.7 UUIDv7 factory. Layout:
 *
 * <ul>
 *   <li>48 bits — unix milliseconds (big-endian, occupies the high 48 bits of {@code msb})
 *   <li>4 bits — version = 7
 *   <li>12 bits — {@code rand_a}, used here as a per-millisecond monotonic counter (RFC 9562 §6.2
 *       method 1)
 *   <li>2 bits — variant = 10
 *   <li>62 bits — {@code rand_b}, drawn from {@link SecureRandom}
 * </ul>
 *
 * <p>Counter overflow within a single millisecond busy-spins until the wall clock advances
 * (wait-for-tick variant; RFC 9562 §6.2). The rejected alternative (timestamp-increment) advances
 * the embedded timestamp ahead of wall-clock time and corrupts time-correlation queries.
 *
 * <p>Clock-backward steps (NTP corrections) are absorbed by holding {@code lastTimestampMs} and
 * continuing to increment the counter, then waiting for the wall clock to catch up if the counter
 * overflows. Monotonicity of generated IDs is preserved.
 *
 * <p>Thread-safe via a single instance lock around counter mutation.
 */
public final class UuidV7Factory {

  private static final SecureRandom RNG = new SecureRandom();
  private static final int COUNTER_MAX = 0x0FFF; // 12 bits
  private static final Object LOCK = new Object();

  private static long lastTimestampMs = 0L;
  private static int counter = 0;

  private UuidV7Factory() {}

  public static UUID create() {
    long timestampMs;
    int counterValue;
    synchronized (LOCK) {
      timestampMs = System.currentTimeMillis();
      if (timestampMs == lastTimestampMs) {
        counter++;
        if (counter > COUNTER_MAX) {
          timestampMs = waitForNextTick(lastTimestampMs);
          counter = 0;
        }
      } else if (timestampMs < lastTimestampMs) {
        // Clock went backward (e.g. NTP step). Stay on lastTimestampMs to preserve monotonicity.
        timestampMs = lastTimestampMs;
        counter++;
        if (counter > COUNTER_MAX) {
          timestampMs = waitForNextTick(lastTimestampMs);
          counter = 0;
        }
      } else {
        counter = 0;
      }
      lastTimestampMs = timestampMs;
      counterValue = counter;
    }

    long msb = (timestampMs & 0x0000_FFFF_FFFF_FFFFL) << 16;
    msb |= 0x7000L; // version = 7
    msb |= (counterValue & 0x0FFFL);

    long lsb = RNG.nextLong();
    lsb &= 0x3FFF_FFFF_FFFF_FFFFL; // clear top 2 bits
    lsb |= 0x8000_0000_0000_0000L; // variant = 10

    return new UUID(msb, lsb);
  }

  private static long waitForNextTick(long previousMs) {
    long now;
    while ((now = System.currentTimeMillis()) <= previousMs) {
      Thread.onSpinWait();
    }
    return now;
  }
}
