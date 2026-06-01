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
