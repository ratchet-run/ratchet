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
 * Serializable {@link Runnable} variant that can throw checked exceptions. Primary functional
 * interface for defining job tasks in the scheduler.
 *
 * <p><strong>Method Reference Constraint:</strong> Due to serialization constraints, job lambdas
 * must contain exactly one method invocation. Multi-statement lambdas will fail at submission time
 * with an {@code IllegalArgumentException}.
 *
 * <h2>Correct</h2>
 *
 * <pre>{@code
 * scheduler.enqueue(MyService::processData).submit();
 * scheduler.enqueue(myService::sendEmail).submit();
 * scheduler.enqueue(() -> myService.process(userId)).submit();
 * }</pre>
 *
 * <h2>Incorrect multi-statement lambda</h2>
 *
 * <pre>{@code
 * // WRONG - Will throw IllegalArgumentException
 * scheduler.enqueue(() -> {
 *     processData();
 *     updateDatabase();
 * }).submit();
 * }</pre>
 *
 * <p>For multi-step operations, extract a method in a CDI bean and reference it:
 *
 * <pre>{@code
 * scheduler.enqueue(() -> userJobHandler.processUserData(userId)).submit();
 * }</pre>
 */
@FunctionalInterface
@SuppressWarnings("java:S112")
// Generic exception intentional for job execution functional interface
public interface SerializableCheckedRunnable extends Serializable {

  /**
   * Executes this job task.
   *
   * @throws Exception if the task fails; the scheduler records the failure and applies retry policy
   */
  void run() throws Exception;
}
