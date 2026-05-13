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

  // Intentionally no serialVersionUID. Interface fields are implicitly public static final, and
  // the JVM serialization spec only honors serialVersionUID when declared as private static final
  // on a class. Lambda instances derive their UID from the JVM-synthesized implementation class,
  // not from this interface.

  /** {@inheritDoc} */
  @Override
  boolean test(T t);
}
