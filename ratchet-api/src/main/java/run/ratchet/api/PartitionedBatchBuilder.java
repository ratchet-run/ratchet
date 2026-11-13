/*
 * Copyright 2026 Ratchet Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package run.ratchet.api;

import java.time.Duration;
import java.util.Collection;

/** A generation of a durable partition set. Callbacks and progress cover this generation. */
@Incubating
public interface PartitionedBatchBuilder {
  /** Sets the submission idempotency key, in the ordinary job namespace. */
  @Incubating
  PartitionedBatchBuilder withIdempotencyKey(String key);

  /** Sets retry backoff for all targeted children. */
  @Incubating
  PartitionedBatchBuilder withBackoff(BackoffPolicy policy, Duration param);

  /** Sets maximum child retries; zero disables retries. */
  @Incubating
  PartitionedBatchBuilder withMaxRetries(int retries);

  /** Sets child execution timeout; null uses the deployment default. */
  @Incubating
  PartitionedBatchBuilder withTimeout(Duration timeout);

  /** Sets the child resource pool; blank clears it. */
  @Incubating
  PartitionedBatchBuilder withResource(String resourceName);

  /** Selects the virtual executor. */
  @Incubating
  PartitionedBatchBuilder virtual();

  /** Selects the platform executor. */
  @Incubating
  PartitionedBatchBuilder platform();

  /** Sets options for callback jobs in this generation. */
  @Incubating
  PartitionedBatchBuilder withCallbackOptions(JobOptions options);

  /** Registers a progress hook for this generation. */
  @Incubating
  PartitionedBatchBuilder onProgress(SerializableConsumer<BatchContext> hook);

  /** Registers a described conditional branch for this generation. */
  @Incubating
  PartitionedBatchBuilder thenBranch(
      WorkflowCondition condition, SerializableCheckedRunnable next, String description);

  /** Registers a failure callback for this generation. */
  @Incubating
  PartitionedBatchBuilder thenOnBatchFailure(SerializableCheckedRunnable next);

  /** Registers a success callback for this generation. */
  @Incubating
  PartitionedBatchBuilder thenOnBatchSuccess(SerializableCheckedRunnable next);

  /** Registers a callback selected by a predicate on this generation. */
  @Incubating
  PartitionedBatchBuilder thenWhenBatch(
      SerializablePredicate<BatchContext> condition, SerializableCheckedRunnable next);

  /** Registers a callback when this generation has at most maxFailures failures. */
  @Incubating
  PartitionedBatchBuilder thenWhenFailureCount(int maxFailures, SerializableCheckedRunnable next);

  /** Registers a callback when this generation meets the success rate. */
  @Incubating
  PartitionedBatchBuilder thenWhenSuccessRate(double minRate, SerializableCheckedRunnable next);

  /**
   * Reopens selected indexes from the start. Duplicates are ignored. Invalid indexes throw
   * IllegalArgumentException. Submission requires an idempotency key and a sealed existing
   * generation.
   */
  @Incubating
  PartitionedBatchBuilder reopen(Collection<Integer> indexes);

  /** Persists the generation in a REQUIRED transaction and returns its parent handle. */
  @Incubating
  JobHandle submit();
}
