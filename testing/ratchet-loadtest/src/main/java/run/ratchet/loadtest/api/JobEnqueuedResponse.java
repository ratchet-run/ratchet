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
package run.ratchet.loadtest.api;

import java.time.Instant;
import java.util.UUID;

public class JobEnqueuedResponse {

  public String runId;
  public UUID jobId;
  public int sequence;
  public String workload;
  public String acceptedNodeId;
  public Instant acceptedAt;

  public JobEnqueuedResponse() {}

  public JobEnqueuedResponse(
      String runId,
      UUID jobId,
      int sequence,
      String workload,
      String acceptedNodeId,
      Instant acceptedAt) {
    this.runId = runId;
    this.jobId = jobId;
    this.sequence = sequence;
    this.workload = workload;
    this.acceptedNodeId = acceptedNodeId;
    this.acceptedAt = acceptedAt;
  }
}
