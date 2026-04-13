package run.ratchet.api;

import java.io.Serializable;
import java.util.function.Predicate;

@FunctionalInterface
public interface SerializablePredicate<T> extends Predicate<T>, Serializable {

  @Override
  boolean test(T t);
}
