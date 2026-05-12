package run.ratchet.spi;

import java.util.Objects;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import run.ratchet.api.Incubating;

/**
 * Typed configuration key used by {@link RatchetConfig}.
 *
 * <p>Ratchet checks the canonical property/env name. Invalid values fall back to the key default
 * and emit a single WARN log so operators can spot typos instead of discovering them as silent
 * behavior drift.
 *
 * @param name dotted property name, for example {@code ratchet.poller.batch-size}
 * @param environmentVariable environment variable fallback, for example {@code
 *     RATCHET_POLLER_BATCH_SIZE}
 * @param defaultValue typed default returned when no valid raw value is available; never {@code
 *     null}
 * @param parser parser used after trimming raw input; must throw {@link RuntimeException} for
 *     invalid values
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

  /**
   * Creates a strict boolean key.
   *
   * @param name dotted property name
   * @param environmentVariable environment variable fallback
   * @param defaultValue value returned when no valid raw value is available
   * @return boolean configuration key
   */
  public static RatchetConfigKey<Boolean> bool(
      String name, String environmentVariable, boolean defaultValue) {
    return new RatchetConfigKey<>(
        name, environmentVariable, defaultValue, RatchetConfigKey::parseStrictBoolean);
  }

  /**
   * Creates a non-negative floating-point key.
   *
   * @param name dotted property name
   * @param environmentVariable environment variable fallback
   * @param defaultValue non-negative finite default value
   * @return floating-point configuration key
   * @throws IllegalArgumentException if {@code defaultValue} is negative or not finite
   */
  public static RatchetConfigKey<Float> floating(
      String name, String environmentVariable, float defaultValue) {
    requireNonNegative(defaultValue);
    return new RatchetConfigKey<>(
        name, environmentVariable, defaultValue, raw -> requireNonNegative(Float.parseFloat(raw)));
  }

  /**
   * Creates a floating-point key constrained to an inclusive range.
   *
   * @param name dotted property name
   * @param environmentVariable environment variable fallback
   * @param defaultValue finite default value within the inclusive range
   * @param minInclusive minimum accepted value
   * @param maxInclusive maximum accepted value
   * @return floating-point configuration key
   * @throws IllegalArgumentException if the bounds are inverted or non-finite, or if {@code
   *     defaultValue} is outside the range
   */
  public static RatchetConfigKey<Float> floatingRange(
      String name,
      String environmentVariable,
      float defaultValue,
      float minInclusive,
      float maxInclusive) {
    requireValidRange(minInclusive, maxInclusive);
    requireRange(defaultValue, minInclusive, maxInclusive);
    return new RatchetConfigKey<>(
        name,
        environmentVariable,
        defaultValue,
        raw -> requireRange(Float.parseFloat(raw), minInclusive, maxInclusive));
  }

  /**
   * Creates a non-negative integer key.
   *
   * @param name dotted property name
   * @param environmentVariable environment variable fallback
   * @param defaultValue non-negative default value
   * @return integer configuration key
   * @throws IllegalArgumentException if {@code defaultValue} is negative
   */
  public static RatchetConfigKey<Integer> integer(
      String name, String environmentVariable, int defaultValue) {
    requireNonNegative(defaultValue);
    return new RatchetConfigKey<>(
        name, environmentVariable, defaultValue, raw -> requireNonNegative(Integer.parseInt(raw)));
  }

  /**
   * Creates an integer key constrained to a minimum value.
   *
   * @param name dotted property name
   * @param environmentVariable environment variable fallback
   * @param defaultValue default value greater than or equal to {@code minInclusive}
   * @param minInclusive minimum accepted value
   * @return integer configuration key
   * @throws IllegalArgumentException if {@code defaultValue} is below {@code minInclusive}
   */
  public static RatchetConfigKey<Integer> integerAtLeast(
      String name, String environmentVariable, int defaultValue, int minInclusive) {
    requireAtLeast(defaultValue, minInclusive);
    return new RatchetConfigKey<>(
        name,
        environmentVariable,
        defaultValue,
        raw -> requireAtLeast(Integer.parseInt(raw), minInclusive));
  }

  /**
   * Creates a non-negative long key.
   *
   * @param name dotted property name
   * @param environmentVariable environment variable fallback
   * @param defaultValue non-negative default value
   * @return long configuration key
   * @throws IllegalArgumentException if {@code defaultValue} is negative
   */
  public static RatchetConfigKey<Long> longValue(
      String name, String environmentVariable, long defaultValue) {
    requireNonNegative(defaultValue);
    return new RatchetConfigKey<>(
        name, environmentVariable, defaultValue, raw -> requireNonNegative(Long.parseLong(raw)));
  }

  /**
   * Creates a long key constrained to a minimum value.
   *
   * @param name dotted property name
   * @param environmentVariable environment variable fallback
   * @param defaultValue default value greater than or equal to {@code minInclusive}
   * @param minInclusive minimum accepted value
   * @return long configuration key
   * @throws IllegalArgumentException if {@code defaultValue} is below {@code minInclusive}
   */
  public static RatchetConfigKey<Long> longAtLeast(
      String name, String environmentVariable, long defaultValue, long minInclusive) {
    requireAtLeast(defaultValue, minInclusive);
    return new RatchetConfigKey<>(
        name,
        environmentVariable,
        defaultValue,
        raw -> requireAtLeast(Long.parseLong(raw), minInclusive));
  }

  /**
   * Creates a string key.
   *
   * @param name dotted property name
   * @param environmentVariable environment variable fallback
   * @param defaultValue default value returned when no valid raw value is available
   * @return string configuration key
   */
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
      throw new IllegalArgumentException(
          "Value "
              + value
              + " must be a finite number between "
              + minInclusive
              + " and "
              + maxInclusive
              + " inclusive");
    }
    return value;
  }

  private static void requireValidRange(float minInclusive, float maxInclusive) {
    if (!Float.isFinite(minInclusive)
        || !Float.isFinite(maxInclusive)
        || minInclusive > maxInclusive) {
      throw new IllegalArgumentException(
          "Minimum value must be less than or equal to maximum value");
    }
  }

  private static int requireNonNegative(int value) {
    return requireAtLeast(value, 0);
  }

  private static int requireAtLeast(int value, int minInclusive) {
    if (value < minInclusive) {
      throw new IllegalArgumentException(
          "Value " + value + " must be greater than or equal to " + minInclusive);
    }
    return value;
  }

  private static long requireNonNegative(long value) {
    return requireAtLeast(value, 0L);
  }

  private static long requireAtLeast(long value, long minInclusive) {
    if (value < minInclusive) {
      throw new IllegalArgumentException(
          "Value " + value + " must be greater than or equal to " + minInclusive);
    }
    return value;
  }

  /**
   * Parses a raw configuration value.
   *
   * <p>{@code null} or blank input returns {@link #defaultValue()}. Nonblank input is trimmed
   * before parsing. If parsing throws a {@link RuntimeException}, the exception is logged at {@code
   * WARN} and {@link #defaultValue()} is returned.
   *
   * @param raw raw string value from a config source; may be {@code null}
   * @return parsed value or the key default; never {@code null}
   */
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
