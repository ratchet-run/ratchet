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
package run.ratchet.testsuite.dlq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobStatus;
import run.ratchet.ri.payload.JobPayloadFactory;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.JobBulkStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.TestDataManipulator;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/** Verifies DLQ purge semantics across store implementations. */
class DlqPurgeIT extends BaseRatchetIT {

  @Inject private JobCrudStore jobCrudStore;

  @Inject private JobBulkStore jobBulkStore;

  @Inject private TestDataManipulator dataManipulator;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");

    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  @Test
  void deleteDlqOlderThan_shouldPurgeFailedJobsRegardlessOfExecutionType() {
    JobEntity staleSingle = persistFailedJob(JobExecutionType.SINGLE);
    JobEntity staleRecurring = persistFailedJob(JobExecutionType.RECURRING);
    JobEntity staleBatchChild = persistFailedJob(JobExecutionType.BATCH_CHILD);
    JobEntity freshFailed = persistFailedJob(JobExecutionType.SINGLE);
    JobEntity exactCutoffFailed = persistFailedJob(JobExecutionType.SINGLE);

    Instant cutoff = Instant.now().minus(Duration.ofDays(2));
    dataManipulator.setJobUpdatedAt(staleSingle.getId(), cutoff.minus(Duration.ofHours(1)));
    dataManipulator.setJobUpdatedAt(staleRecurring.getId(), cutoff.minus(Duration.ofHours(1)));
    dataManipulator.setJobUpdatedAt(staleBatchChild.getId(), cutoff.minus(Duration.ofHours(1)));
    dataManipulator.setJobUpdatedAt(freshFailed.getId(), cutoff.plus(Duration.ofHours(1)));
    dataManipulator.setJobUpdatedAt(exactCutoffFailed.getId(), cutoff);

    int deleted = jobBulkStore.deleteDlqOlderThan(cutoff);

    assertEquals(3, deleted);
    assertFalse(jobCrudStore.findById(staleSingle.getId()).isPresent());
    assertFalse(jobCrudStore.findById(staleRecurring.getId()).isPresent());
    assertFalse(jobCrudStore.findById(staleBatchChild.getId()).isPresent());
    assertTrue(jobCrudStore.findById(freshFailed.getId()).isPresent());
    assertTrue(
        jobCrudStore.findById(exactCutoffFailed.getId()).isPresent(),
        "deleteDlqOlderThan uses an exclusive cutoff");
  }

  @Test
  void deleteDlqOlderThan_whenNoJobsMatch_shouldReturnZero() {
    Instant cutoff = Instant.now().minus(Duration.ofDays(2));

    int deleted = jobBulkStore.deleteDlqOlderThan(cutoff);

    assertEquals(0, deleted);
  }

  @Test
  void deleteDlqOlderThan_whenAllJobsAreStale_shouldDeleteAll() {
    JobEntity first = persistFailedJob(JobExecutionType.SINGLE);
    JobEntity second = persistFailedJob(JobExecutionType.RECURRING);
    Instant cutoff = Instant.now().minus(Duration.ofDays(2));
    dataManipulator.setJobUpdatedAt(first.getId(), cutoff.minus(Duration.ofHours(1)));
    dataManipulator.setJobUpdatedAt(second.getId(), cutoff.minus(Duration.ofHours(1)));

    int deleted = jobBulkStore.deleteDlqOlderThan(cutoff);

    assertEquals(2, deleted);
    assertFalse(jobCrudStore.findById(first.getId()).isPresent());
    assertFalse(jobCrudStore.findById(second.getId()).isPresent());
  }

  private JobEntity persistFailedJob(JobExecutionType executionType) {
    JobEntity job = new JobEntity();
    job.setJobType(executionType);
    job.setStatus(JobStatus.FAILED);
    job.setPriority(JobPriority.NORMAL);
    job.setScheduledTime(Instant.now().minusSeconds(5));
    job.setPayload(JobPayloadFactory.noop());
    job.setIdempotencyKey(UUID.randomUUID().toString());
    job.setAttempts(0);
    job.setMaxRetries(0);
    job.setLastError("boom");
    return jobCrudStore.save(job);
  }
}
