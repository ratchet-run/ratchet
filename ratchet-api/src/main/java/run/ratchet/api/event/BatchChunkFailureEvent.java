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
package run.ratchet.api.event;

import java.io.Serial;
import java.time.Instant;
import java.util.UUID;
import run.ratchet.api.Incubating;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;

/**
 * Fired when a streaming-batch chunk fails to persist (the chunk's bulk insert threw) on the
 * invocation-mode submission path.
 *
 * <p><b>Best-effort pre-rollback diagnostic.</b> Streaming submission runs in one transaction: a
 * chunk failure rolls back the batch parent and every previously inserted chunk along with it. The
 * event therefore fires <em>before</em> that rollback and may reference a batch parent id that
 * never commits — treat it as an operational signal of the failed submission, not as a pointer to
 * durable rows. {@link #getJobId()} carries the batch parent id.
 */
@Incubating
public class BatchChunkFailureEvent extends AbstractJobSchedulerEvent {

  @Serial private static final long serialVersionUID = 1L;

  private final int chunkIndex;
  private final int chunkSize;
  private final String failureReason;

  public BatchChunkFailureEvent(
      UUID jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      Instant timestamp,
      int chunkIndex,
      int chunkSize,
      String failureReason) {
    super(jobId, businessKey, jobType, priority, nodeId, timestamp);
    this.chunkIndex = chunkIndex;
    this.chunkSize = chunkSize;
    this.failureReason = failureReason;
  }

  public BatchChunkFailureEvent(
      UUID jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      int chunkIndex,
      int chunkSize,
      String failureReason) {
    super(jobId, businessKey, jobType, priority, nodeId);
    this.chunkIndex = chunkIndex;
    this.chunkSize = chunkSize;
    this.failureReason = failureReason;
  }

  /** Zero-based index of the chunk whose bulk insert failed. */
  public int getChunkIndex() {
    return chunkIndex;
  }

  /** Number of items in the failed chunk. */
  public int getChunkSize() {
    return chunkSize;
  }

  /** Failure description from the underlying store exception. */
  public String getFailureReason() {
    return failureReason;
  }
}
