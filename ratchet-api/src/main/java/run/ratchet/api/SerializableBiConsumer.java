package run.ratchet.api;

import java.io.Serializable;
import java.util.function.BiConsumer;

/**
 * Serializable variant of {@link BiConsumer} for use in job scheduling operations.
 *
 * <p>This functional interface extends {@link BiConsumer} with {@link Serializable} capability,
 * enabling lambda expressions and method references to be persisted in job payloads. It accepts two
 * input arguments and returns no result, making it ideal for operations where both context and item
 * data are needed.
 *
 * <h2>Primary Use Cases:</h2>
 *
 * <ul>
 *   <li>Error handling with context information
 *   <li>Two-parameter event handlers
 *   <li>Callbacks requiring both context and data
 * </ul>
 *
 * <h2>Usage Example:</h2>
 *
 * <pre>{@code
 * // In error handlers
 * scheduler.enqueue(() -> riskyOperation())
 *     .onFailure((JobContext context, Throwable error) -> {
 *         errorLogger.error("Job {} failed: {}",
 *                          context.jobId(), error.getMessage());
 *         alertService.sendAlert(context.jobId(), error);
 *     })
 *     .submit();
 * }</pre>
 *
 * <h2>Serialization Requirements:</h2>
 *
 * <p>Lambda expressions using this interface must capture only serializable state. Non-serializable
 * captures will cause runtime serialization failures.
 *
 * @param <T> the type of the first input to the operation
 * @param <U> the type of the second input to the operation
 * @see JobBuilder#onFailure(SerializableBiConsumer)
 * @see SerializableConsumer
 */
@FunctionalInterface
public interface SerializableBiConsumer<T, U> extends BiConsumer<T, U>, Serializable {

  /**
   * Performs this operation on the given arguments.
   *
   * <p>This method is inherited from {@link BiConsumer} and enhanced with serialization support.
   * Implementations should perform side-effect operations using both input arguments without
   * returning a result.
   *
   * <p>When used in job scheduling contexts, both the lambda implementation and any captured
   * variables must be serializable to allow persistence in job queues.
   *
   * @param t the first input argument
   * @param u the second input argument
   */
  @Override
  void accept(T t, U u);
}
