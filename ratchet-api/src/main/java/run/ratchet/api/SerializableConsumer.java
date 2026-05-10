package run.ratchet.api;

import java.io.Serializable;
import java.util.function.Consumer;

/**
 * {@link Consumer} that is also {@link Serializable}.
 *
 * <p>Use this for callbacks that Ratchet may persist with a job or batch definition. Captured state
 * must itself be serializable.
 *
 * @param <T> consumed value type
 */
@FunctionalInterface
public interface SerializableConsumer<T> extends Consumer<T>, Serializable {

  /** {@inheritDoc} */
  @Override
  void accept(T t);
}
