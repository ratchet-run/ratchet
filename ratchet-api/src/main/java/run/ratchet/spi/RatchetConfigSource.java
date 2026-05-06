package run.ratchet.spi;

import java.util.Optional;
import run.ratchet.api.Incubating;

/** Raw configuration source used by {@link RatchetConfig}. */
@Incubating
public interface RatchetConfigSource {

  Optional<String> get(String propertyName, String environmentVariable);
}
