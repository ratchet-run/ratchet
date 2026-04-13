package run.ratchet.api;

import java.io.Serializable;
import java.util.function.Consumer;

/** Serializable {@link Consumer} for persistence of job callbacks. */
@FunctionalInterface
public interface SerializableConsumer<T> extends Consumer<T>, Serializable {

  @Override
  void accept(T t);
}
