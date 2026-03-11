package run.ratchet.api;

import java.io.Serializable;
import java.util.Collection;

/**
 * Fluent API for creating batch jobs that execute multiple tasks in parallel.
 *
 * <p>Concrete implementations are provided by the RI and obtained via {@link
 * JobSchedulerService#enqueueBatch(String)}.
 */
public interface BatchBuilder {

  <T extends Serializable> BatchBuilder forEach(
      Collection<T> items, SerializableConsumer<T> action);

  BatchBuilder onProgress(SerializableConsumer<BatchContext> hook);

  JobHandle submit();

  BatchBuilder thenBranch(
      WorkflowCondition condition, SerializableCheckedRunnable next, String description);

  BatchBuilder thenOnBatchFailure(SerializableCheckedRunnable next);

  BatchBuilder thenOnBatchSuccess(SerializableCheckedRunnable next);

  BatchBuilder thenWhenBatch(
      SerializablePredicate<BatchContext> condition, SerializableCheckedRunnable next);

  BatchBuilder thenWhenFailureCount(int maxFailures, SerializableCheckedRunnable next);

  BatchBuilder thenWhenSuccessRate(double minRate, SerializableCheckedRunnable next);
}
