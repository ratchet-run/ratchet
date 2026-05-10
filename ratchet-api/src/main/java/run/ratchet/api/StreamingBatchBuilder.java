package run.ratchet.api;

import java.io.Serializable;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * Fluent builder for streaming batch jobs that process large datasets in chunks.
 *
 * <p>Each item type must be {@link Serializable} because streaming batches persist chunk boundaries
 * and may replay individual items after failures. The stream itself is consumed by the builder and
 * is not serialized, but every emitted item and any batch callback supplied to this API must remain
 * serializable across scheduler restarts.
 *
 * @param <T> the item type, must be {@link Serializable}
 */
public interface StreamingBatchBuilder<T extends Serializable> {

  /** Sets the input stream of items to process. The stream item type must match this builder. */
  StreamingBatchBuilder<T> fromStream(Stream<T> stream);

  /** Sets the processing action applied to each item; may throw checked exceptions. */
  StreamingBatchBuilder<T> process(SerializableCheckedConsumer<T> action);

  /** Sets the number of items per chunk (must be positive). */
  StreamingBatchBuilder<T> withChunkSize(int size);

  /**
   * Attaches a progress hook invoked during stream consumption with current streaming metrics.
   *
   * @param hook receives a {@link StreamingBatchContext} with streaming progress
   */
  StreamingBatchBuilder<T> onProgress(Consumer<StreamingBatchContext> hook);

  /**
   * Attaches a batch-level progress hook invoked during job execution with batch metrics.
   *
   * @param hook receives a {@link BatchContext} with batch execution progress
   */
  StreamingBatchBuilder<T> onBatchProgress(SerializableConsumer<BatchContext> hook);

  /**
   * Submits the configured streaming batch for execution.
   *
   * @return a {@link JobHandle} for the submitted batch job
   */
  JobHandle start();

  /** Executes the given task when a batch completes successfully. */
  StreamingBatchBuilder<T> thenOnBatchSuccess(SerializableCheckedRunnable next);

  /** Executes the given task when a batch fails. */
  StreamingBatchBuilder<T> thenOnBatchFailure(SerializableCheckedRunnable next);

  /**
   * Executes the given task when a custom batch condition is met.
   *
   * @param condition predicate evaluated against the {@link BatchContext}
   */
  StreamingBatchBuilder<T> thenWhenBatch(
      SerializablePredicate<BatchContext> condition, SerializableCheckedRunnable next);

  /**
   * Executes the given task when the failure count reaches the specified threshold.
   *
   * @param maxFailures failure count threshold
   */
  StreamingBatchBuilder<T> thenWhenFailureCount(int maxFailures, SerializableCheckedRunnable next);

  /**
   * Executes the given task when the success rate meets or exceeds the threshold.
   *
   * @param minRate minimum success rate (0.0 to 1.0)
   */
  StreamingBatchBuilder<T> thenWhenSuccessRate(double minRate, SerializableCheckedRunnable next);
}
