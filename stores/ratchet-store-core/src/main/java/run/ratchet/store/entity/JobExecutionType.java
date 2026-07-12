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
