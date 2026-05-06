package run.ratchet.spi;

import java.util.Optional;
import run.ratchet.api.Incubating;

/** Typed runtime configuration facade used to build {@code RatchetOptions}. */
@Incubating
public interface RatchetConfig {

  <T> T get(RatchetConfigKey<T> key);

  Optional<String> raw(RatchetConfigKey<?> key);
}
