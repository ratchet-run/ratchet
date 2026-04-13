package run.ratchet.api;

import java.io.Serializable;
import java.util.function.Predicate;

/** Serializable {@link Predicate} for persistence of workflow conditions. */
@FunctionalInterface
public interface SerializablePredicate<T> extends Predicate<T>, Serializable {

  @Override
  boolean test(T t);
}
