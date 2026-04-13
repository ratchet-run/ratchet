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
 * <h3>Correct:</h3>
 *
 * <pre>{@code
 * scheduler.enqueue(MyService::processData).submit();
 * scheduler.enqueue(myService::sendEmail).submit();
 * scheduler.enqueue(() -> myService.process(userId)).submit();
 * }</pre>
 *
 * <h3>Incorrect (multi-statement):</h3>
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

  void run() throws Exception;
}
