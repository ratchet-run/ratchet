package run.ratchet.spi;

import run.ratchet.api.Incubating;
import java.util.Optional;

/** Raw configuration source used by {@link RatchetConfig}. */
@Incubating
public interface RatchetConfigSource {

  Optional<String> get(String propertyName, String environmentVariable);
}
