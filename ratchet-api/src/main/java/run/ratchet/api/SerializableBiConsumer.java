package run.ratchet.api;

import java.io.Serializable;
import java.util.function.BiConsumer;

/** Serializable {@link BiConsumer} for persistence of two-argument job callbacks. */
@FunctionalInterface
public interface SerializableBiConsumer<T, U> extends BiConsumer<T, U>, Serializable {

  @Override
  void accept(T t, U u);
}
