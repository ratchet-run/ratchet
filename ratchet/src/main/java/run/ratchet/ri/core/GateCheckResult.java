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

import java.util.UUID;
import run.ratchet.store.entity.JobExecutionType;

/**
 * Outcome of checking drain, rate-limit, and permit gates before job submission. A CLEAR result
 * means a permit was acquired on the {@link #resolvedPoolName() resolved pool} and must be released
 * through execution or explicit release against that same pool. Blocked results hold no permit and
 * carry a null pool name.
 */
record GateCheckResult(GateStatus status, String reason, String resolvedPoolName) {

  static GateCheckResult clear(String resolvedPoolName) {
    return new GateCheckResult(GateStatus.CLEAR, null, resolvedPoolName);
  }

  static GateCheckResult draining(UUID jobId) {
    return new GateCheckResult(
        GateStatus.DRAINING, "Node draining - returning job " + jobId + " to PENDING", null);
  }

  static GateCheckResult noPermits(JobExecutionType jobType, UUID jobId) {
    return new GateCheckResult(
        GateStatus.NO_PERMITS,
        String.format(
            "Executor for %s saturated - returning job %s to PENDING for other nodes",
            jobType, jobId),
        null);
  }

  static GateCheckResult rateLimited(
      JobExecutionType jobType, UUID jobId, int currentCount, int limit) {
    return new GateCheckResult(
        GateStatus.RATE_LIMITED,
        String.format(
            "Rate limit exceeded for %s (current: %d/min, limit: %d/min) - "
                + "returning job %s to PENDING",
            jobType, currentCount, limit, jobId),
        null);
  }

  boolean isClear() {
    return status == GateStatus.CLEAR;
  }

  boolean isBlocked() {
    return status != GateStatus.CLEAR;
  }

  enum GateStatus {
    CLEAR,
    DRAINING,
    RATE_LIMITED,
    NO_PERMITS
  }
}
