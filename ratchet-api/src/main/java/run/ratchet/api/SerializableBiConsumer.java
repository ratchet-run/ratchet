package run.ratchet.api;

import java.io.Serializable;
import java.util.function.BiConsumer;

/**
 * Serializable {@link BiConsumer} for persistence of two-argument job callbacks.
 *
 * @param <T> the type of the first input to the operation
 * @param <U> the type of the second input to the operation
 */
@FunctionalInterface
public interface SerializableBiConsumer<T, U> extends BiConsumer<T, U>, Serializable {

  @Override
  void accept(T t, U u);
}
