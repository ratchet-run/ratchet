package run.ratchet.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import run.ratchet.spi.RatchetConfig;
import run.ratchet.spi.RatchetConfigKey;

class DefaultRatchetConfigTest {

  private static final RatchetConfigKey<Integer> KEY =
      RatchetConfigKey.integer("ratchet.test.value", "RATCHET_TEST_VALUE", 7);
  private static final RatchetConfigKey<Boolean> BOOL_KEY =
      RatchetConfigKey.bool("ratchet.test.enabled", "RATCHET_TEST_ENABLED", true);

  @AfterEach
  void clearProperties() {
    System.clearProperty("ratchet.test.value");
    System.clearProperty("RATCHET_TEST_VALUE");
    System.clearProperty("ratchet.test.enabled");
    System.clearProperty("RATCHET_TEST_ENABLED");
  }

  @Test
  void readsCanonicalSystemProperty() {
    System.setProperty("ratchet.test.value", "42");

    RatchetConfig config = new DefaultRatchetConfig(List.of(new EnvironmentRatchetConfigSource()));

    assertEquals(42, config.get(KEY));
  }

  @Test
  void invalidNumericValueReturnsDefault() {
    System.setProperty("ratchet.test.value", "bad");

    RatchetConfig config = new DefaultRatchetConfig(List.of(new EnvironmentRatchetConfigSource()));

    assertEquals(7, config.get(KEY));
  }

  @Test
  void negativeNumericValueReturnsDefault() {
    System.setProperty("ratchet.test.value", "-1");

    RatchetConfig config = new DefaultRatchetConfig(List.of(new EnvironmentRatchetConfigSource()));

    assertEquals(7, config.get(KEY));
  }

  @Test
  void invalidBooleanValueReturnsDefault() {
    System.setProperty("ratchet.test.enabled", "definitely");

    RatchetConfig config = new DefaultRatchetConfig(List.of(new EnvironmentRatchetConfigSource()));

    assertTrue(config.get(BOOL_KEY));
  }

  @Test
  void validBooleanValueIsCaseInsensitive() {
    System.setProperty("ratchet.test.enabled", "FALSE");

    RatchetConfig config = new DefaultRatchetConfig(List.of(new EnvironmentRatchetConfigSource()));

    assertFalse(config.get(BOOL_KEY));
  }
}
