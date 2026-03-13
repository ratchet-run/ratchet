package run.ratchet.store.entity;

import run.ratchet.api.JobType;

/**
 * Internal execution roles used by the RI and store implementations.
 *
 * <p>Unlike the public {@link JobType}, these values model scheduler mechanics such as batch
 * parent/child expansion and workflow branch orchestration.
 */
public enum JobExecutionType {
  SINGLE(JobType.SINGLE),
  RECURRING(JobType.RECURRING),
  BATCH_PARENT(JobType.BATCH),
  BATCH_CHILD(JobType.BATCH),
  CHAIN_STEP(JobType.CHAIN),
  DLQ_ALERT(JobType.SYSTEM),
  WORKFLOW_BRANCH(JobType.WORKFLOW),
  WORKFLOW_JOIN(JobType.WORKFLOW);

  private final JobType publicType;

  JobExecutionType(JobType publicType) {
    this.publicType = publicType;
  }

  public JobType toPublicType() {
    return publicType;
  }
}
