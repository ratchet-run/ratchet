package run.ratchet.api;

import java.io.Serializable;
import java.util.function.Function;

/**
 * Serializable {@link Function} for persistence of result transformations.
 *
 * @param <T> the type of the input to the function
 * @param <R> the type of the result of the function
 */
@FunctionalInterface
public interface SerializableFunction<T, R> extends Function<T, R>, Serializable {

  @Override
  R apply(T t);
}
