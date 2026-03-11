package run.ratchet.api;

import java.io.Serializable;

/**
 * Serializable variant of {@link java.util.function.Consumer} that can throw checked exceptions.
 *
 * <p>This functional interface extends {@link Serializable} capability and allows the accept method
 * to throw checked exceptions. This is essential for streaming batch operations where the
 * processing action may need to propagate checked exceptions to the caller for proper error
 * handling during stream consumption.
 *
 * <h2>Primary Use Cases:</h2>
 *
 * <ul>
 *   <li>Streaming batch item processing where methods throw checked exceptions
 *   <li>Database operations that throw SQLException
 *   <li>I/O operations that throw IOException
 *   <li>Any single-parameter action that needs checked exception propagation
 * </ul>
 *
 * <h2>Usage Example:</h2>
 *
 * <pre>{@code
 * // In streaming batch processing
 * scheduler.streamingBatch("Process Users")
 *     .fromStream(userRepository.streamActiveUsers())
 *     .process(userId -> {
 *         // This method can throw checked exceptions
 *         userService.processUser(userId);
 *     })
 *     .start();
 * }</pre>
 *
 * <h2>Comparison with SerializableConsumer:</h2>
 *
 * <p>{@link SerializableConsumer} extends {@link java.util.function.Consumer} which does not allow
 * checked exceptions. This interface provides the same serialization capability but with checked
 * exception support, making it suitable for operations that may fail with checked exceptions.
 *
 * <h2>Serialization Requirements:</h2>
 *
 * <p>Lambda expressions using this interface must capture only serializable state. Non-serializable
 * captures will cause runtime serialization failures.
 *
 * @param <T> the type of the input to the operation
 * @see SerializableConsumer
 * @see StreamingBatchBuilder#process(SerializableCheckedConsumer)
 */
@FunctionalInterface
@SuppressWarnings("java:S112")
// Generic exception intentional for streaming batch functional interface
public interface SerializableCheckedConsumer<T> extends Serializable {

  /**
   * Performs this operation on the given argument.
   *
   * @param t the input argument
   * @throws Exception if unable to process the argument
   */
  void accept(T t) throws Exception;
}
