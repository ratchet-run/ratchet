package run.ratchet.api;

import java.io.Serializable;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * Fluent API for creating streaming batch jobs with bounded memory usage.
 *
 * <p>Concrete implementations are provided by the RI and obtained via {@link
 * JobSchedulerService#streamingBatch(String)}.
 *
 * @param <T> the type of items in the stream
 */
public interface StreamingBatchBuilder<T extends Serializable> {

  <U extends Serializable> StreamingBatchBuilder<U> fromStream(Stream<U> stream);

  StreamingBatchBuilder<T> process(SerializableCheckedConsumer<T> action);

  StreamingBatchBuilder<T> withChunkSize(int size);

  StreamingBatchBuilder<T> onProgress(Consumer<StreamingBatchContext> hook);

  StreamingBatchBuilder<T> onBatchProgress(SerializableConsumer<BatchContext> hook);

  JobHandle start();

  StreamingBatchBuilder<T> thenOnBatchSuccess(SerializableCheckedRunnable next);

  StreamingBatchBuilder<T> thenOnBatchFailure(SerializableCheckedRunnable next);

  StreamingBatchBuilder<T> thenWhenBatch(
      SerializablePredicate<BatchContext> condition, SerializableCheckedRunnable next);

  StreamingBatchBuilder<T> thenWhenFailureCount(int maxFailures, SerializableCheckedRunnable next);

  StreamingBatchBuilder<T> thenWhenSuccessRate(double minRate, SerializableCheckedRunnable next);
}
