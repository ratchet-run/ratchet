package run.ratchet.api;

import java.io.Serializable;
import java.util.function.Function;

/**
 * {@link Function} that is also {@link Serializable}.
 *
 * <p>When used for persisted workflow conditions, RI lambda analysis expects the function body to
 * resolve to a single public method invocation. Prefer method references or one-line forwarding
 * lambdas.
 *
 * @param <T> input type
 * @param <R> result type
 */
@FunctionalInterface
public interface SerializableFunction<T, R> extends Function<T, R>, Serializable {

  /** {@inheritDoc} */
  @Override
  R apply(T t);
}
