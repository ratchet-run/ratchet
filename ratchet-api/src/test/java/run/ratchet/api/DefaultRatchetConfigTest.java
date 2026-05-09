package run.ratchet.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
  private static final RatchetConfigKey<Float> FLOAT_RANGE_KEY =
      RatchetConfigKey.floatingRange("ratchet.test.ratio", "RATCHET_TEST_RATIO", 0.5f, 0.1f, 1.0f);
  private static final RatchetConfigKey<Integer> INT_MIN_KEY =
      RatchetConfigKey.integerAtLeast("ratchet.test.count", "RATCHET_TEST_COUNT", 3, 2);
  private static final RatchetConfigKey<Long> LONG_MIN_KEY =
      RatchetConfigKey.longAtLeast("ratchet.test.delay", "RATCHET_TEST_DELAY", 10L, 5L);

  @AfterEach
  void clearProperties() {
    System.clearProperty("ratchet.test.value");
    System.clearProperty("RATCHET_TEST_VALUE");
    System.clearProperty("ratchet.test.enabled");
    System.clearProperty("RATCHET_TEST_ENABLED");
    System.clearProperty("ratchet.test.ratio");
    System.clearProperty("RATCHET_TEST_RATIO");
    System.clearProperty("ratchet.test.count");
    System.clearProperty("RATCHET_TEST_COUNT");
    System.clearProperty("ratchet.test.delay");
    System.clearProperty("RATCHET_TEST_DELAY");
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

  @Test
  void floatingRangeAcceptsBoundaryValues() {
    System.setProperty("ratchet.test.ratio", "0.1");
    RatchetConfig config = new DefaultRatchetConfig(List.of(new EnvironmentRatchetConfigSource()));
    assertEquals(0.1f, config.get(FLOAT_RANGE_KEY));

    System.setProperty("ratchet.test.ratio", "1.0");
    assertEquals(1.0f, config.get(FLOAT_RANGE_KEY));
  }

  @Test
  void floatingRangeRejectsOutOfRangeValuesWithDetails() {
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> FLOAT_RANGE_KEY.parser().apply("1.1"));

    assertTrue(exception.getMessage().contains("1.1"));
    assertTrue(exception.getMessage().contains("0.1"));
    assertTrue(exception.getMessage().contains("1.0"));
  }

  @Test
  void integerAtLeastAcceptsMinimumAndRejectsLowerValuesWithDetails() {
    System.setProperty("ratchet.test.count", "2");
    RatchetConfig config = new DefaultRatchetConfig(List.of(new EnvironmentRatchetConfigSource()));
    assertEquals(2, config.get(INT_MIN_KEY));

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> INT_MIN_KEY.parser().apply("1"));

    assertTrue(exception.getMessage().contains("1"));
    assertTrue(exception.getMessage().contains("2"));
  }

  @Test
  void longAtLeastAcceptsMinimumAndRejectsLowerValuesWithDetails() {
    System.setProperty("ratchet.test.delay", "5");
    RatchetConfig config = new DefaultRatchetConfig(List.of(new EnvironmentRatchetConfigSource()));
    assertEquals(5L, config.get(LONG_MIN_KEY));

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> LONG_MIN_KEY.parser().apply("4"));

    assertTrue(exception.getMessage().contains("4"));
    assertTrue(exception.getMessage().contains("5"));
  }
}
