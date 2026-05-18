package run.ratchet.spi;

import java.util.Optional;
import run.ratchet.api.Incubating;

/** Raw configuration source used by {@link RatchetConfig}. */
@Incubating
public interface RatchetConfigSource {

  /**
   * Returns a raw configuration value for a property/env pair.
   *
   * <p>When both names are supplied and both exist, lookup precedence is implementation-defined.
   * Callers must order their configured sources rather than depending on intra-source precedence.
   *
   * @param propertyName dotted property name, for example {@code ratchet.poller.batch-size}; {@code
   *     null} or blank values are treated as absent
   * @param environmentVariable environment variable fallback, for example {@code
   *     RATCHET_POLLER_BATCH_SIZE}; {@code null} or blank values are treated as absent
   * @return raw value when present, otherwise {@link Optional#empty()}
   * @throws RuntimeException if the source cannot be read; callers may continue to the next source
   */
  Optional<String> get(String propertyName, String environmentVariable);
}
