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
package run.ratchet.store.util;

import run.ratchet.api.JobStatus;
import run.ratchet.store.entity.JobExecutionType;

/** Shared status/type predicates used by store implementations. */
public final class StatusClassifier {

  private StatusClassifier() {}

  public static boolean isPollerExecutable(JobExecutionType jobType) {
    return jobType == JobExecutionType.SINGLE
        || jobType == JobExecutionType.BATCH_CHILD
        || jobType == JobExecutionType.CHAIN_STEP
        || jobType == JobExecutionType.WORKFLOW_BRANCH;
  }

  public static boolean isLiveStatus(JobStatus status) {
    return status == JobStatus.PENDING
        || status == JobStatus.RUNNING
        || status == JobStatus.PAUSED
        || status == JobStatus.WAITING;
  }

  public static boolean isTerminalStatus(JobStatus status) {
    return status == JobStatus.SUCCEEDED
        || status == JobStatus.FAILED
        || status == JobStatus.CANCELED;
  }

  public static JobStatus effectiveStatus(JobStatus status) {
    return status == null ? JobStatus.PENDING : status;
  }

  public static String recStatusForLiveStatus(JobStatus status) {
    if (status == JobStatus.PENDING) {
      return "P";
    }
    if (status == JobStatus.PAUSED) {
      return "A";
    }
    return null;
  }

  public static JobStatus recStatusDecode(String value) {
    if ("P".equals(value)) {
      return JobStatus.PENDING;
    }
    if ("A".equals(value)) {
      return JobStatus.PAUSED;
    }
    return null;
  }
}
