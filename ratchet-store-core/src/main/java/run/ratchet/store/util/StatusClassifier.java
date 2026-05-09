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
