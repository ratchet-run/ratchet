package run.ratchet.spi;

import java.util.Optional;
import run.ratchet.api.Incubating;

/** Raw configuration source used by {@link RatchetConfig}. */
@Incubating
public interface RatchetConfigSource {

  /**
   * Returns a raw configuration value for a property/env pair.
   *
   * @param propertyName dotted property name, for example {@code ratchet.poller.batch-size}; never
   *     {@code null}
   * @param environmentVariable environment variable fallback, for example {@code
   *     RATCHET_POLLER_BATCH_SIZE}; never {@code null}
   * @return raw value when present, otherwise {@link Optional#empty()}
   * @throws RuntimeException if the source cannot be read; callers may continue to the next source
   */
  Optional<String> get(String propertyName, String environmentVariable);
}
