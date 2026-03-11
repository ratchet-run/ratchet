package run.ratchet.api;

import java.io.Serializable;
import java.util.function.Consumer;

/**
 * Serializable variant of {@link Consumer} for use in job callbacks and event handlers.
 *
 * <p>This functional interface extends {@link Consumer} with {@link Serializable} capability,
 * enabling lambda expressions and method references to be persisted as part of job configurations.
 * It accepts a single input argument and returns no result, making it ideal for callbacks, event
 * handlers, and side-effect operations.
 *
 * <h2>Primary Use Cases:</h2>
 *
 * <ul>
 *   <li>Success callbacks with job context
 *   <li>Progress monitoring hooks
 *   <li>Event notification handlers
 *   <li>State update operations
 * </ul>
 *
 * <h2>Usage Examples:</h2>
 *
 * <pre>{@code
 * // Success callback
 * scheduler.enqueue(() -> processOrder(orderId))
 *     .onSuccess(context -> {
 *         log.info("Order {} processed successfully by job {}",
 *                  orderId, context.jobId());
 *         notificationService.sendOrderComplete(orderId);
 *     })
 *     .submit();
 *
 * // Progress monitoring
 * scheduler.enqueueBatch("Data Migration")
 *     .forEach(records, record -> migrateRecord(record))
 *     .onProgress(context -> {
 *         int percent = context.percentDone();
 *         log.info("Migration {}% complete", percent);
 *
 *         // Update UI or send notifications
 *         if (percent % 25 == 0) {
 *             websocket.broadcast("migration.progress", percent);
 *         }
 *     })
 *     .submit();
 *
 * // Method reference usage
 * public class OrderService {
 *     public void handleOrderComplete(JobContext context) {
 *         updateOrderStatus(context.param("orderId"), "COMPLETE");
 *         sendConfirmationEmail(context.param("customerEmail"));
 *     }
 *
 *     public JobHandle processOrderAsync(String orderId) {
 *         return scheduler.enqueue(() -> processOrder(orderId))
 *             .withParam("orderId", orderId)
 *             .onSuccess(this::handleOrderComplete)
 *             .submit();
 *     }
 * }
 * }</pre>
 *
 * <h2>Best Practices:</h2>
 *
 * <ul>
 *   <li>Keep callback logic lightweight and fast
 *   <li>Avoid blocking operations in callbacks
 *   <li>Use parameters from JobContext rather than capturing state
 *   <li>Handle exceptions gracefully within the consumer
 * </ul>
 *
 * @param <T> the type of the input to the operation
 * @see JobBuilder#onSuccess(SerializableConsumer)
 * @see BatchBuilder#onProgress(SerializableConsumer)
 * @see SerializableBiConsumer
 */
@FunctionalInterface
public interface SerializableConsumer<T> extends Consumer<T>, Serializable {

  /**
   * Performs this operation on the given argument.
   *
   * <p>This method is inherited from {@link Consumer} and enhanced with serialization support.
   * Implementations should perform side-effect operations using the input argument without
   * returning a result.
   *
   * <p>When used in job scheduling contexts, both the lambda implementation and any captured
   * variables must be serializable to allow persistence in job queues.
   *
   * @param t the input argument
   */
  @Override
  void accept(T t);
}
