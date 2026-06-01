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
package run.ratchet.ri.core;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.jboss.logging.Logger;
import run.ratchet.api.BatchContext;
import run.ratchet.api.ExecutorTargets;
import run.ratchet.api.JobHandle;
import run.ratchet.api.SerializableCheckedConsumer;
import run.ratchet.api.SerializableCheckedRunnable;
import run.ratchet.api.SerializableConsumer;
import run.ratchet.api.SerializablePredicate;
import run.ratchet.api.StreamingBatchBuilder;
import run.ratchet.api.StreamingBatchContext;
import run.ratchet.api.WorkflowBranch;
import run.ratchet.api.WorkflowCondition;

/** {@inheritDoc} */
class DefaultStreamingBatchBuilder<T extends Serializable> implements StreamingBatchBuilder<T> {

  private static final Logger log = Logger.getLogger(DefaultStreamingBatchBuilder.class);
  private static final int MIN_CHUNK_SIZE = 1;
  private static final int DEFAULT_CHUNK_SIZE = 100;

  private final String name;
  private final StreamingBatchSubmitter submitter;

  private final List<WorkflowBranch> workflowBranches = new ArrayList<>();
  private Stream<T> stream;
  private SerializableCheckedConsumer<T> action;
  private int chunkSize = DEFAULT_CHUNK_SIZE;
  private Consumer<StreamingBatchContext> localProgressHook;
  private SerializableConsumer<BatchContext> batchProgressHook;
  private String executionTarget;

  DefaultStreamingBatchBuilder(String name, StreamingBatchSubmitter submitter) {
    this.name = name;
    this.submitter = submitter;
  }

  @Override
  public StreamingBatchBuilder<T> fromStream(Stream<T> stream) {
    this.stream = Objects.requireNonNull(stream, "stream must not be null");
    return this;
  }

  @Override
  public StreamingBatchBuilder<T> process(SerializableCheckedConsumer<T> action) {
    this.action = action;
    return this;
  }

  @Override
  public StreamingBatchBuilder<T> withChunkSize(int size) {
    if (size < MIN_CHUNK_SIZE) {
      throw new IllegalArgumentException("Chunk size must be greater than zero");
    }
    this.chunkSize = size;
    return this;
  }

  @Override
  public StreamingBatchBuilder<T> onProgress(Consumer<StreamingBatchContext> hook) {
    this.localProgressHook = hook;
    return this;
  }

  @Override
  public StreamingBatchBuilder<T> onBatchProgress(SerializableConsumer<BatchContext> hook) {
    this.batchProgressHook = hook;
    return this;
  }

  @Override
  public StreamingBatchBuilder<T> virtual() {
    this.executionTarget = ExecutorTargets.VIRTUAL;
    return this;
  }

  @Override
  public StreamingBatchBuilder<T> platform() {
    this.executionTarget = ExecutorTargets.PLATFORM;
    return this;
  }

  @Override
  public JobHandle start() {
    return submitter.submit(this);
  }

  @Override
  public StreamingBatchBuilder<T> thenOnBatchSuccess(SerializableCheckedRunnable next) {
    workflowBranches.add(new WorkflowBranch(WorkflowCondition.batchSuccess(), next));
    return this;
  }

  @Override
  public StreamingBatchBuilder<T> thenOnBatchFailure(SerializableCheckedRunnable next) {
    workflowBranches.add(new WorkflowBranch(WorkflowCondition.batchFailure(), next));
    return this;
  }

  @Override
  public StreamingBatchBuilder<T> thenWhenBatch(
      SerializablePredicate<BatchContext> condition, SerializableCheckedRunnable next) {
    workflowBranches.add(new WorkflowBranch(WorkflowCondition.batchCustom(condition), next));
    return this;
  }

  @Override
  public StreamingBatchBuilder<T> thenWhenFailureCount(
      int maxFailures, SerializableCheckedRunnable next) {
    workflowBranches.add(new WorkflowBranch(WorkflowCondition.failureCount(maxFailures), next));
    return this;
  }

  @Override
  public StreamingBatchBuilder<T> thenWhenSuccessRate(
      double minRate, SerializableCheckedRunnable next) {
    workflowBranches.add(new WorkflowBranch(WorkflowCondition.successRate(minRate), next));
    return this;
  }

  void invokeLocalProgressHook(UUID batchId, int processedItems, int chunksInserted) {
    if (localProgressHook == null) {
      return;
    }

    try {
      localProgressHook.accept(new StreamingBatchContext(batchId, processedItems, chunksInserted));
    } catch (Exception e) {
      log.warn("Streaming progress hook threw exception", e);
    }
  }

  void validateReady() {
    if (stream == null) {
      throw new IllegalStateException("Stream must be set via fromStream() before calling start()");
    }
    if (action == null) {
      throw new IllegalStateException(
          "Processing action must be set via process() before calling start()");
    }
  }

  String name() {
    return name;
  }

  Stream<T> stream() {
    return stream;
  }

  SerializableCheckedConsumer<T> action() {
    return action;
  }

  int chunkSize() {
    return chunkSize;
  }

  List<T> newChunk() {
    return new ArrayList<>(chunkSize);
  }

  SerializableConsumer<BatchContext> batchProgressHook() {
    return batchProgressHook;
  }

  String executionTarget() {
    return executionTarget;
  }

  List<WorkflowBranch> workflowBranches() {
    return workflowBranches;
  }
}
