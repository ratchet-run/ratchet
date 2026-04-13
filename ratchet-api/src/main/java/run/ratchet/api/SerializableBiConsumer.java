package run.ratchet.api;

import java.io.Serializable;
import java.util.function.BiConsumer;

@FunctionalInterface
public interface SerializableBiConsumer<T, U> extends BiConsumer<T, U>, Serializable {

  @Override
  void accept(T t, U u);
}
