package run.ratchet.spi;

import java.util.Optional;
import run.ratchet.api.Incubating;

/** Typed runtime configuration facade used to build {@code RatchetOptions}. */
@Incubating
public interface RatchetConfig {

  /**
   * Reads and parses a typed configuration value.
   *
   * <p>Missing, blank, or invalid raw values return the key default rather than {@code null}.
   *
   * @param key configuration key to read; never {@code null}
   * @return parsed value or {@link RatchetConfigKey#defaultValue()}; never {@code null}
   */
  <T> T get(RatchetConfigKey<T> key);

  /**
   * Reads the first raw value available for a key before parsing.
   *
   * @param key configuration key to read; never {@code null}
   * @return raw string value from the configured sources, or {@link Optional#empty()} when absent
   */
  Optional<String> raw(RatchetConfigKey<?> key);
}
