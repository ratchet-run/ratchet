package run.ratchet.ri.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import run.ratchet.spi.RatchetConfig;
import run.ratchet.spi.RatchetConfigKey;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class DefaultRatchetConfigTest {

  private static final RatchetConfigKey<Integer> KEY =
      RatchetConfigKey.integer(
          "ratchet.test.value",
          "RATCHET_TEST_VALUE",
          "scheduler.test.value",
          "SCHEDULER_TEST_VALUE",
          7);
  private static final RatchetConfigKey<Boolean> BOOL_KEY =
      RatchetConfigKey.bool(
          "ratchet.test.enabled",
          "RATCHET_TEST_ENABLED",
          "scheduler.test.enabled",
          "SCHEDULER_TEST_ENABLED",
          true);

  @AfterEach
  void clearProperties() {
    System.clearProperty("ratchet.test.value");
    System.clearProperty("RATCHET_TEST_VALUE");
    System.clearProperty("scheduler.test.value");
    System.clearProperty("SCHEDULER_TEST_VALUE");
    System.clearProperty("ratchet.test.enabled");
    System.clearProperty("RATCHET_TEST_ENABLED");
    System.clearProperty("scheduler.test.enabled");
    System.clearProperty("SCHEDULER_TEST_ENABLED");
  }

  @Test
  void readsPreferredSystemProperty() {
    System.setProperty("ratchet.test.value", "42");

    RatchetConfig config = new DefaultRatchetConfig(new EnvironmentRatchetConfigSource());

    assertEquals(42, config.get(KEY));
  }

  @Test
  void fallsBackToLegacyProperty() {
    System.setProperty("scheduler.test.value", "11");

    RatchetConfig config = new DefaultRatchetConfig(new EnvironmentRatchetConfigSource());

    assertEquals(11, config.get(KEY));
  }

  @Test
  void invalidNumericValueReturnsDefault() {
    System.setProperty("ratchet.test.value", "bad");

    RatchetConfig config = new DefaultRatchetConfig(new EnvironmentRatchetConfigSource());

    assertEquals(7, config.get(KEY));
  }

  @Test
  void negativeNumericValueReturnsDefault() {
    System.setProperty("ratchet.test.value", "-1");

    RatchetConfig config = new DefaultRatchetConfig(new EnvironmentRatchetConfigSource());

    assertEquals(7, config.get(KEY));
  }

  @Test
  void invalidBooleanValueReturnsDefault() {
    System.setProperty("ratchet.test.enabled", "definitely");

    RatchetConfig config = new DefaultRatchetConfig(new EnvironmentRatchetConfigSource());

    assertTrue(config.get(BOOL_KEY));
  }

  @Test
  void validBooleanValueIsCaseInsensitive() {
    System.setProperty("ratchet.test.enabled", "FALSE");

    RatchetConfig config = new DefaultRatchetConfig(new EnvironmentRatchetConfigSource());

    assertFalse(config.get(BOOL_KEY));
  }
}
