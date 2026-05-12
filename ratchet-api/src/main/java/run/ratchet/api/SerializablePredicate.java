package run.ratchet.api;

import java.io.Serializable;
import java.util.function.Predicate;

/**
 * {@link Predicate} that is also {@link Serializable}, intended for workflow branch conditions.
 *
 * <p>When persisted by the RI, predicate lambdas must resolve to a single public method invocation
 * so the scheduler can capture them as job payload metadata. Prefer method references or one-line
 * forwarding lambdas.
 *
 * @param <T> tested value type
 * @see WorkflowCondition
 */
@FunctionalInterface
public interface SerializablePredicate<T> extends Predicate<T>, Serializable {

  long serialVersionUID = 1L;

  /** {@inheritDoc} */
  @Override
  boolean test(T t);
}
