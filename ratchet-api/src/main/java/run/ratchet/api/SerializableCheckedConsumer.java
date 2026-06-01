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

/**
 * Serializable consumer variant that can throw checked exceptions.
 *
 * <p>Used for streaming-batch item processors. When the RI persists one of these lambdas as a job
 * payload, the lambda must resolve to a single public method invocation, matching the constraint on
 * {@link SerializableCheckedRunnable}. Multi-statement lambdas fail during submission with {@link
 * IllegalArgumentException}.
 *
 * @param <T> consumed item type
 */
@FunctionalInterface
@SuppressWarnings("java:S112")
// Generic exception intentional for streaming batch functional interface
public interface SerializableCheckedConsumer<T> extends Serializable {

  /**
   * Consumes one item.
   *
   * @param t item to process
   * @throws Exception if item processing fails
   */
  void accept(T t) throws Exception;
}
