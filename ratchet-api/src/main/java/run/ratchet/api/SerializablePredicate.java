/*
 * Copyright 2026 Ratchet Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
