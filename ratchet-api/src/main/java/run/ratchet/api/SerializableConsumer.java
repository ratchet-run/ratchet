package run.ratchet.api;

import java.io.Serializable;
import java.util.function.Consumer;

/**
 * Serializable {@link Consumer} for persistence of job callbacks.
 *
 * @param <T> the type of the input to the operation
 */
@FunctionalInterface
public interface SerializableConsumer<T> extends Consumer<T>, Serializable {

  @Override
  void accept(T t);
}
