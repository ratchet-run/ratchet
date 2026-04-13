package run.ratchet.api;

import java.io.Serializable;
import java.util.function.Predicate;

/**
 * Serializable {@link Predicate} for persistence of workflow conditions.
 *
 * @param <T> the type of the input to the predicate
 */
@FunctionalInterface
public interface SerializablePredicate<T> extends Predicate<T>, Serializable {

  @Override
  boolean test(T t);
}
