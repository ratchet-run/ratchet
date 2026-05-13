package run.ratchet.api;

import java.io.Serializable;
import java.util.function.BiConsumer;

/**
 * A {@link BiConsumer} that is also {@link Serializable}. Lambda implementations must capture only
 * serializable values to avoid {@link java.io.NotSerializableException} at runtime.
 */
@FunctionalInterface
public interface SerializableBiConsumer<T, U> extends BiConsumer<T, U>, Serializable {

  @Override
  void accept(T t, U u);
}
