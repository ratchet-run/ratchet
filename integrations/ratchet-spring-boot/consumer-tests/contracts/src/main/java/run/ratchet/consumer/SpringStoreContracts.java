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
package run.ratchet.consumer;

import java.util.List;
import java.util.UUID;
import org.springframework.context.ConfigurableApplicationContext;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobStatus;
import run.ratchet.api.exception.RatchetOptimisticLockException;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.store.spi.JobStore;
import run.ratchet.tck.store.JobStoreContractFixture;

/** Supplies the real Spring-managed store proxy to the existing store TCK contracts. */
public abstract class SpringStoreContracts implements JobStoreContractFixture {

  protected abstract ConfigurableApplicationContext context();

  protected abstract void resetStore();

  @Override
  public final JobStore store() {
    return context().getBean(JobStore.class);
  }

  @Override
  public JobEntity newPendingJob() {
    return newJob(JobExecutionType.SINGLE, "com.example.TestJob");
  }

  @Override
  public JobEntity newBatchParentJob() {
    return newJob(JobExecutionType.BATCH_PARENT, "com.example.BatchJob");
  }

  @Override
  public final void cleanupStore() {
    resetStore();
  }

  @Override
  public boolean isStaleWriteException(Throwable failure) {
    for (Throwable current = failure; current != null; current = current.getCause()) {
      if (current instanceof RatchetOptimisticLockException) {
        return true;
      }
    }
    return false;
  }

  private JobEntity newJob(JobExecutionType type, String target) {
    JobEntity job = new JobEntity();
    job.setStatus(JobStatus.PENDING);
    job.setScheduledTime(store().getDatabaseTime().minusSeconds(1));
    job.setJobType(type);
    job.setPriority(JobPriority.NORMAL);
    job.setBackoffPolicy(BackoffPolicy.NONE);
    job.setIdempotencyKey(UUID.randomUUID().toString());
    job.setPayload(new JobPayload(target, "execute", "()V", false, List.of()));
    return job;
  }
}
