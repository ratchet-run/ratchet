package run.ratchet.spi;

import run.ratchet.api.Incubating;
import java.util.Optional;

/** Typed runtime configuration facade. */
@Incubating
public interface RatchetConfig {

  <T> T get(RatchetConfigKey<T> key);

  Optional<String> raw(RatchetConfigKey<?> key);
}
