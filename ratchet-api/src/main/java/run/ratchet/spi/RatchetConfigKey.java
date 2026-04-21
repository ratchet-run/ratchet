package run.ratchet.spi;

import run.ratchet.api.Incubating;
import java.util.Objects;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Typed configuration key used by {@link RatchetConfig}.
 *
 * <p>Ratchet checks the canonical property/env name. Invalid values fall back to the key default
 * and emit a single WARN log so operators can spot typos instead of discovering them as silent
 * behavior drift.
 */
@Incubating
public record RatchetConfigKey<T>(
    String name, String environmentVariable, T defaultValue, Function<String, T> parser) {

  private static final Logger LOG = Logger.getLogger(RatchetConfigKey.class.getName());

  public RatchetConfigKey {
    Objects.requireNonNull(name, "name must not be null");
    Objects.requireNonNull(environmentVariable, "environmentVariable must not be null");
    Objects.requireNonNull(defaultValue, "defaultValue must not be null");
    Objects.requireNonNull(parser, "parser must not be null");
  }

  public static RatchetConfigKey<Boolean> bool(
      String name, String environmentVariable, boolean defaultValue) {
    return new RatchetConfigKey<>(
        name, environmentVariable, defaultValue, RatchetConfigKey::parseStrictBoolean);
  }

  public static RatchetConfigKey<Float> floating(
      String name, String environmentVariable, float defaultValue) {
    return new RatchetConfigKey<>(
        name, environmentVariable, defaultValue, raw -> requireNonNegative(Float.parseFloat(raw)));
  }

  public static RatchetConfigKey<Float> floatingRange(
      String name,
      String environmentVariable,
      float defaultValue,
      float minInclusive,
      float maxInclusive) {
    return new RatchetConfigKey<>(
        name,
        environmentVariable,
        defaultValue,
        raw -> requireRange(Float.parseFloat(raw), minInclusive, maxInclusive));
  }

  public static RatchetConfigKey<Integer> integer(
      String name, String environmentVariable, int defaultValue) {
    return new RatchetConfigKey<>(
        name, environmentVariable, defaultValue, raw -> requireNonNegative(Integer.parseInt(raw)));
  }

  public static RatchetConfigKey<Integer> integerAtLeast(
      String name, String environmentVariable, int defaultValue, int minInclusive) {
    return new RatchetConfigKey<>(
        name,
        environmentVariable,
        defaultValue,
        raw -> requireAtLeast(Integer.parseInt(raw), minInclusive));
  }

  public static RatchetConfigKey<Long> longValue(
      String name, String environmentVariable, long defaultValue) {
    return new RatchetConfigKey<>(
        name, environmentVariable, defaultValue, raw -> requireNonNegative(Long.parseLong(raw)));
  }

  public static RatchetConfigKey<Long> longAtLeast(
      String name, String environmentVariable, long defaultValue, long minInclusive) {
    return new RatchetConfigKey<>(
        name,
        environmentVariable,
        defaultValue,
        raw -> requireAtLeast(Long.parseLong(raw), minInclusive));
  }

  public static RatchetConfigKey<String> string(
      String name, String environmentVariable, String defaultValue) {
    return new RatchetConfigKey<>(name, environmentVariable, defaultValue, Function.identity());
  }

  private static boolean parseStrictBoolean(String raw) {
    if ("true".equalsIgnoreCase(raw)) {
      return true;
    }
    if ("false".equalsIgnoreCase(raw)) {
      return false;
    }
    throw new IllegalArgumentException("Boolean value must be true or false");
  }

  private static float requireNonNegative(float value) {
    if (!Float.isFinite(value) || value < 0.0f) {
      throw new IllegalArgumentException("Value must be a non-negative finite number");
    }
    return value;
  }

  private static float requireRange(float value, float minInclusive, float maxInclusive) {
    if (!Float.isFinite(value) || value < minInclusive || value > maxInclusive) {
      throw new IllegalArgumentException("Value is outside the allowed range");
    }
    return value;
  }

  private static int requireNonNegative(int value) {
    return requireAtLeast(value, 0);
  }

  private static int requireAtLeast(int value, int minInclusive) {
    if (value < minInclusive) {
      throw new IllegalArgumentException("Value is below the allowed minimum");
    }
    return value;
  }

  private static long requireNonNegative(long value) {
    return requireAtLeast(value, 0L);
  }

  private static long requireAtLeast(long value, long minInclusive) {
    if (value < minInclusive) {
      throw new IllegalArgumentException("Value is below the allowed minimum");
    }
    return value;
  }

  public T parse(String raw) {
    if (raw == null || raw.isBlank()) {
      return defaultValue;
    }
    try {
      return parser.apply(raw.trim());
    } catch (RuntimeException e) {
      LOG.log(
          Level.WARNING,
          e,
          () ->
              "Invalid value for Ratchet config key '"
                  + name
                  + "' (env '"
                  + environmentVariable
                  + "'): '"
                  + raw
                  + "' — falling back to default '"
                  + defaultValue
                  + "'");
      return defaultValue;
    }
  }
}
