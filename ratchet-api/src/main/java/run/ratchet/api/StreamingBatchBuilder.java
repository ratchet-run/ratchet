package run.ratchet.api;

import java.io.Serializable;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * Fluent builder for streaming batch jobs that process large datasets in chunks.
 *
 * @param <T> the item type, must be {@link Serializable}
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
   * @return this builder
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
   * @return this builder
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
   * @return this builder
   */
  StreamingBatchBuilder<T> withChunkSize(int size);

  /**
   * Attaches a progress hook invoked during stream consumption with current streaming metrics.
   *
   * @param hook receives a {@link StreamingBatchContext} with streaming progress
   * @return this builder
   */
  StreamingBatchBuilder<T> onProgress(Consumer<StreamingBatchContext> hook);

  /**
   * Attaches a batch-level progress hook invoked during job execution with batch metrics.
   *
   * @param hook receives a {@link BatchContext} with batch execution progress
   * @return this builder
   */
  StreamingBatchBuilder<T> onBatchProgress(SerializableConsumer<BatchContext> hook);

  /**
   * Submits the configured streaming batch for execution.
   *
   * @return a {@link JobHandle} for the submitted batch job
   */
  JobHandle start();

  /**
   * Executes the given task when a batch completes successfully.
   *
   * @param next the task to run on batch success
   * @return this builder
   */
  StreamingBatchBuilder<T> thenOnBatchSuccess(SerializableCheckedRunnable next);

  /**
   * Executes the given task when a batch fails.
   *
   * @param next the task to run on batch failure
   * @return this builder
   */
  StreamingBatchBuilder<T> thenOnBatchFailure(SerializableCheckedRunnable next);

  /**
   * Executes the given task when a custom batch condition is met.
   *
   * @param condition predicate evaluated against the {@link BatchContext}
   * @param next the task to run when the condition is satisfied
   * @return this builder
   */
  StreamingBatchBuilder<T> thenWhenBatch(
      SerializablePredicate<BatchContext> condition, SerializableCheckedRunnable next);

  /**
   * Executes the given task when the failure count reaches the specified threshold.
   *
   * @param maxFailures failure count threshold
   * @param next the task to run when the threshold is reached
   * @return this builder
   */
  StreamingBatchBuilder<T> thenWhenFailureCount(int maxFailures, SerializableCheckedRunnable next);

  /**
   * Executes the given task when the success rate meets or exceeds the threshold.
   *
   * @param minRate minimum success rate (0.0 to 1.0)
   * @param next the task to run when the rate is met
   * @return this builder
   */
  StreamingBatchBuilder<T> thenWhenSuccessRate(double minRate, SerializableCheckedRunnable next);
}
