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
package run.ratchet.tck.api;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import run.ratchet.api.exception.CircuitBreakerOpenException;
import run.ratchet.spi.ResilienceStrategy;

/**
 * Base contract for {@link ResilienceStrategy} open- and closed-circuit semantics.
 *
 * <p>Implementations subclass this contract with a strategy instance and an implementation-specific
 * way to force the named service into an open-circuit state before assertions run.
 */
public abstract class AbstractResilienceStrategyContract {

  @Test
  void closedCircuitInvokesTaskAndReturnsItsResult() throws Exception {
    String serviceName = "tck-closed-circuit";
    Object sentinel = new Object();
    AtomicBoolean taskCalled = new AtomicBoolean(false);

    assertTrue(
        resilienceStrategy().isServiceAvailable(serviceName),
        "A service that was never tripped must report available");

    Object result =
        resilienceStrategy()
            .execute(
                serviceName,
                () -> {
                  taskCalled.set(true);
                  return sentinel;
                });

    assertTrue(taskCalled.get(), "Closed circuit must invoke the task body");
    assertSame(sentinel, result, "Closed circuit must return the task's own result unchanged");
  }

  @Test
  void openCircuitExecuteThrowsDistinctRuntimeExceptionWithoutCallingTask() throws Exception {
    String serviceName = "tck-open-circuit";
    forceOpenCircuit(serviceName);

    AtomicBoolean taskCalled = new AtomicBoolean(false);

    assertThrows(
        CircuitBreakerOpenException.class,
        () ->
            resilienceStrategy()
                .execute(
                    serviceName,
                    () -> {
                      taskCalled.set(true);
                      return "should not run";
                    }));
    assertFalse(taskCalled.get(), "Open-circuit rejection must not invoke the task body");
  }

  @Test
  void openCircuitReportsUnavailableAndNonNegativeRetryDelay() throws Exception {
    String serviceName = "tck-open-circuit-delay";
    forceOpenCircuit(serviceName);

    assertFalse(
        resilienceStrategy().isServiceAvailable(serviceName),
        "Forced-open circuit must report unavailable");

    Duration retryDelay = resilienceStrategy().getRetryDelay(serviceName);
    assertFalse(retryDelay.isNegative(), "getRetryDelay must not return a negative duration");
  }

  protected abstract ResilienceStrategy resilienceStrategy();

  protected abstract void forceOpenCircuit(String serviceName) throws Exception;
}
