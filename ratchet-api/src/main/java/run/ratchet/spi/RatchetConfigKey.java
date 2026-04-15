package run.ratchet.spi;

import run.ratchet.api.Incubating;
import java.util.Objects;
import java.util.function.Function;

/**
 * Typed configuration key used by {@link RatchetConfig}.
 *
 * <p>Ratchet checks the preferred environment variable/property first, then the optional legacy
 * environment variable/property. Invalid values fall back to the key default.
 */
@Incubating
public record RatchetConfigKey<T>(
    String name,
    String environmentVariable,
    String legacyName,
    String legacyEnvironmentVariable,
    T defaultValue,
    Function<String, T> parser) {

  public RatchetConfigKey {
    Objects.requireNonNull(name, "name must not be null");
    Objects.requireNonNull(environmentVariable, "environmentVariable must not be null");
    Objects.requireNonNull(defaultValue, "defaultValue must not be null");
    Objects.requireNonNull(parser, "parser must not be null");
  }

  public T parse(String raw) {
    if (raw == null || raw.isBlank()) {
      return defaultValue;
    }
    try {
      return parser.apply(raw.trim());
    } catch (RuntimeException e) {
      return defaultValue;
    }
  }

  public boolean hasLegacyName() {
    return legacyName != null && !legacyName.isBlank();
  }

  public boolean hasLegacyEnvironmentVariable() {
    return legacyEnvironmentVariable != null && !legacyEnvironmentVariable.isBlank();
  }

  public static RatchetConfigKey<Boolean> bool(
      String name,
      String environmentVariable,
      String legacyName,
      String legacyEnvironmentVariable,
      boolean defaultValue) {
    return new RatchetConfigKey<>(
        name,
        environmentVariable,
        legacyName,
        legacyEnvironmentVariable,
        defaultValue,
        RatchetConfigKey::parseStrictBoolean);
  }

  public static RatchetConfigKey<Float> floating(
      String name,
      String environmentVariable,
      String legacyName,
      String legacyEnvironmentVariable,
      float defaultValue) {
    return new RatchetConfigKey<>(
        name,
        environmentVariable,
        legacyName,
        legacyEnvironmentVariable,
        defaultValue,
        raw -> requireNonNegative(Float.parseFloat(raw)));
  }

  public static RatchetConfigKey<Float> floatingRange(
      String name,
      String environmentVariable,
      String legacyName,
      String legacyEnvironmentVariable,
      float defaultValue,
      float minInclusive,
      float maxInclusive) {
    return new RatchetConfigKey<>(
        name,
        environmentVariable,
        legacyName,
        legacyEnvironmentVariable,
        defaultValue,
        raw -> requireRange(Float.parseFloat(raw), minInclusive, maxInclusive));
  }

  public static RatchetConfigKey<Integer> integer(
      String name,
      String environmentVariable,
      String legacyName,
      String legacyEnvironmentVariable,
      int defaultValue) {
    return new RatchetConfigKey<>(
        name,
        environmentVariable,
        legacyName,
        legacyEnvironmentVariable,
        defaultValue,
        raw -> requireNonNegative(Integer.parseInt(raw)));
  }

  public static RatchetConfigKey<Integer> integerAtLeast(
      String name,
      String environmentVariable,
      String legacyName,
      String legacyEnvironmentVariable,
      int defaultValue,
      int minInclusive) {
    return new RatchetConfigKey<>(
        name,
        environmentVariable,
        legacyName,
        legacyEnvironmentVariable,
        defaultValue,
        raw -> requireAtLeast(Integer.parseInt(raw), minInclusive));
  }

  public static RatchetConfigKey<Long> longValue(
      String name,
      String environmentVariable,
      String legacyName,
      String legacyEnvironmentVariable,
      long defaultValue) {
    return new RatchetConfigKey<>(
        name,
        environmentVariable,
        legacyName,
        legacyEnvironmentVariable,
        defaultValue,
        raw -> requireNonNegative(Long.parseLong(raw)));
  }

  public static RatchetConfigKey<Long> longAtLeast(
      String name,
      String environmentVariable,
      String legacyName,
      String legacyEnvironmentVariable,
      long defaultValue,
      long minInclusive) {
    return new RatchetConfigKey<>(
        name,
        environmentVariable,
        legacyName,
        legacyEnvironmentVariable,
        defaultValue,
        raw -> requireAtLeast(Long.parseLong(raw), minInclusive));
  }

  public static RatchetConfigKey<String> string(
      String name,
      String environmentVariable,
      String legacyName,
      String legacyEnvironmentVariable,
      String defaultValue) {
    return new RatchetConfigKey<>(
        name,
        environmentVariable,
        legacyName,
        legacyEnvironmentVariable,
        defaultValue,
        Function.identity());
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
}
