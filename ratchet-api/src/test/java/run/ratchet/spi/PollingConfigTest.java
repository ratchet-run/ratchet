package run.ratchet.spi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class PollingConfigTest {

  @Test
  void acceptsValidValues() {
    PollingConfig config = new PollingConfig(50, 100, 1_000, 5_000, 10_000, 3, 25);

    assertEquals(50, config.burstDelayMs());
    assertEquals(100, config.minDelayMs());
    assertEquals(1_000, config.maxDelayMs());
    assertEquals(5_000, config.deepIdleDelayMs());
    assertEquals(10_000, config.deepIdleThresholdMs());
    assertEquals(3, config.idleThreshold());
    assertEquals(25, config.batchSize());
  }

  @Test
  void rejectsInvalidValues() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new PollingConfig(-1, 100, 1_000, 5_000, 10_000, 3, 25));
    assertThrows(
        IllegalArgumentException.class,
        () -> new PollingConfig(50, -1, 1_000, 5_000, 10_000, 3, 25));
    assertThrows(
        IllegalArgumentException.class, () -> new PollingConfig(50, 100, -1, 5_000, 10_000, 3, 25));
    assertThrows(
        IllegalArgumentException.class, () -> new PollingConfig(50, 100, 1_000, -1, 10_000, 3, 25));
    assertThrows(
        IllegalArgumentException.class, () -> new PollingConfig(50, 100, 1_000, 5_000, -1, 3, 25));
    assertThrows(
        IllegalArgumentException.class,
        () -> new PollingConfig(50, 1_001, 1_000, 5_000, 10_000, 3, 25));
    assertThrows(
        IllegalArgumentException.class,
        () -> new PollingConfig(50, 100, 5_001, 5_000, 10_000, 3, 25));
    assertThrows(
        IllegalArgumentException.class,
        () -> new PollingConfig(50, 100, 1_000, 5_000, 10_000, -1, 25));
    assertThrows(
        IllegalArgumentException.class,
        () -> new PollingConfig(50, 100, 1_000, 5_000, 10_000, 3, 0));
  }
}
