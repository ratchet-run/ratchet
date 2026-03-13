package run.ratchet.api;

import java.io.Serializable;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * A builder interface for constructing and managing streaming batch jobs. This allows for
 * configuration and processing of large datasets in chunks, with support for custom actions,
 * progress tracking, and conditional workflows.
 *
 * @param <T> the type of items to be processed in the streaming batch, which must be {@link
 *     Serializable}
 */
public interface StreamingBatchBuilder<T extends Serializable> {

  /**
   * Initializes the {@link StreamingBatchBuilder} with a given stream of items to process. This
   * method sets the input data source for the batching operation, allowing further configuration of
   * processing logic, chunk size, and progress tracking.
   *
   * @param <U> the type of items to be processed in the streaming batch, which must be {@link
   *     Serializable}
   * @param stream the stream of items to be used as input for the batching operation
   * @return a {@link StreamingBatchBuilder} instance configured for processing the given stream of
   *     items
   */
  <U extends Serializable> StreamingBatchBuilder<U> fromStream(Stream<U> stream);

  /**
   * Configures the processing logic for each item in the batch. The specified action will be
   * applied to every individual item in the stream during batch processing. The action can throw
   * checked exceptions, which will be handled by the streaming batch framework based on the
   * configured error-handling behaviors.
   *
   * @param action the {@link SerializableCheckedConsumer} that defines the processing logic for
   *     each item in the batch. This action can throw checked exceptions.
   * @return the current {@link StreamingBatchBuilder} instance, allowing further configuration of
   *     the batch processing pipeline.
   */
  StreamingBatchBuilder<T> process(SerializableCheckedConsumer<T> action);

  /**
   * Specifies the size of chunks in which the streaming batch will process data. Each chunk
   * represents the number of items grouped together for a single batch operation. A proper chunk
   * size can optimize performance based on the size of the dataset and the processing constraints.
   *
   * @param size the number of items to include in each chunk for batch processing. Must be a
   *     positive integer; otherwise, the configuration may result in an error or undefined
   *     behavior.
   * @return the current {@code StreamingBatchBuilder} instance with the chunk size configured,
   *     enabling further method chaining for additional configuration.
   */
  StreamingBatchBuilder<T> withChunkSize(int size);

  /**
   * Attaches a progress hook to the {@code StreamingBatchBuilder}. The specified hook will be
   * invoked periodically during the stream consumption phase to provide updates about the progress
   * of items being streamed and chunks being processed.
   *
   * <p>This method allows users to monitor or log the progress of the operations by inspecting the
   * {@link StreamingBatchContext} details passed to the hook.
   *
   * @param hook a {@link Consumer} that processes instances of {@link StreamingBatchContext},
   *     providing real-time updates on the streaming progress. The consumer can be used to log
   *     progress, analyze streaming metrics, or trigger side effects based on progress data.
   * @return the current {@link StreamingBatchBuilder} instance, allowing further method chaining
   *     for configuration or execution of the batching operation.
   */
  StreamingBatchBuilder<T> onProgress(Consumer<StreamingBatchContext> hook);

  /**
   * Attaches a batch-specific progress hook to the {@code StreamingBatchBuilder}. The specified
   * hook will be invoked during batch processing to provide updates about the progress and state of
   * the current batch. This allows for monitoring or handling events specific to the batch's
   * execution.
   *
   * @param hook a {@link SerializableConsumer} that consumes a {@link BatchContext} instance,
   *     providing detailed information about the status of the current batch, including metrics
   *     such as completion percentage, counts of completed and failed items, and the batch
   *     identifier.
   * @return the current {@link StreamingBatchBuilder} instance, enabling further method chaining
   *     for additional configuration or execution of the batch processing pipeline.
   */
  StreamingBatchBuilder<T> onBatchProgress(SerializableConsumer<BatchContext> hook);

  /**
   * Starts the batch processing operation as configured in the {@code StreamingBatchBuilder}. This
   * method triggers the execution of the pipeline defined by prior configuration, including stream
   * input, processing logic, chunk size, and progress hooks.
   *
   * <p>The operation runs asynchronously, and a {@link JobHandle} is returned to provide a
   * reference to the submitted job. The {@link JobHandle} allows clients to track the job's
   * execution, query its status, or perform other interactions.
   *
   * @return a {@link JobHandle} representing the submitted batch processing job, enabling tracking
   *     and management of its lifecycle.
   */
  JobHandle start();

  /**
   * Configures a callback to be executed upon successful completion of a batch. This method allows
   * the addition of logic to handle scenarios where a batch has been processed successfully without
   * any errors.
   *
   * @param next a {@link SerializableCheckedRunnable} representing the callback logic to be
   *     executed when a batch is successfully completed. The callback can perform any follow-up
   *     actions and may throw checked exceptions.
   * @return the current {@link StreamingBatchBuilder} instance, enabling further method chaining
   *     for additional configuration or execution of the batching pipeline.
   */
  StreamingBatchBuilder<T> thenOnBatchSuccess(SerializableCheckedRunnable next);

  /**
   * Configures a callback to be executed when a batch processing operation fails. The specified
   * callback logic will be triggered if an error occurs during the processing of a batch, allowing
   * users to handle failure scenarios appropriately.
   *
   * @param next a {@link SerializableCheckedRunnable} representing the logic to be executed upon a
   *     batch failure. This callback can perform any recovery, logging, or notification actions and
   *     may throw checked exceptions.
   * @return the current {@link StreamingBatchBuilder} instance, enabling further method chaining
   *     for additional configuration or execution of the batching pipeline.
   */
  StreamingBatchBuilder<T> thenOnBatchFailure(SerializableCheckedRunnable next);

  /**
   * Configures a conditional callback that will be executed when the specified condition related to
   * batch processing is met. The condition is evaluated using the provided {@link
   * SerializablePredicate}, which receives a {@link BatchContext} containing details about the
   * current batch's state. If the condition evaluates to {@code true}, the associated callback
   * logic represented by the {@link SerializableCheckedRunnable} will be executed.
   *
   * @param condition a {@link SerializablePredicate} that defines the condition to be evaluated
   *     against the {@link BatchContext}. The callback will only be triggered if this condition
   *     returns {@code true}.
   * @param next a {@link SerializableCheckedRunnable} representing the logic to be executed when
   *     the condition is satisfied. This runnable can perform any necessary actions and may throw
   *     checked exceptions.
   * @return the current {@link StreamingBatchBuilder} instance, allowing further method chaining
   *     for additional configuration or execution of the batch processing pipeline.
   */
  StreamingBatchBuilder<T> thenWhenBatch(
      SerializablePredicate<BatchContext> condition, SerializableCheckedRunnable next);

  /**
   * Configures the builder to execute the specified runnable action if the number of failures
   * encountered during processing reaches a specified maximum threshold.
   *
   * @param maxFailures the maximum number of failures allowed before triggering the provided
   *     action.
   * @param next the action to be executed when the failure count reaches the specified threshold.
   *     This action must implement the SerializableCheckedRunnable interface.
   * @return the current instance of StreamingBatchBuilder with the specified failure condition and
   *     action configured.
   */
  StreamingBatchBuilder<T> thenWhenFailureCount(int maxFailures, SerializableCheckedRunnable next);

  /**
   * Specifies the next action to be executed when the success rate of the current streaming batch
   * operation reaches or exceeds the given threshold.
   *
   * @param minRate The minimum success rate, as a decimal value between 0.0 and 1.0, which must be
   *     met or exceeded to trigger the next action.
   * @param next The runnable action to execute when the success rate condition is satisfied.
   * @return The updated StreamingBatchBuilder instance configured with this success rate condition.
   */
  StreamingBatchBuilder<T> thenWhenSuccessRate(double minRate, SerializableCheckedRunnable next);
}
