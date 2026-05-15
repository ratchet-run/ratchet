package run.ratchet.tck.api;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import run.ratchet.api.exception.CircuitBreakerOpenException;
import run.ratchet.spi.ResilienceStrategy;

/**
 * Base contract for {@link ResilienceStrategy} open-circuit semantics.
 *
 * <p>Implementations subclass this contract with a strategy instance and an implementation-specific
 * way to force the named service into an open-circuit state before assertions run.
 */
public abstract class AbstractResilienceStrategyContract {

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
