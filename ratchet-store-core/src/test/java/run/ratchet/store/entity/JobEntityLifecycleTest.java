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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobStatus;

class JobEntityLifecycleTest {

  @Test
  void prePersistRejectsMissingRequiredFieldsBeforeDatabaseFlush() {
    JobEntity job = requiredJob();
    job.setScheduledTime(null);

    IllegalStateException ex = assertThrows(IllegalStateException.class, job::prePersist);

    assertTrue(ex.getMessage().contains("scheduledTime"));
  }

  @Test
  void preUpdateRejectsMissingRequiredFieldsBeforeDatabaseFlush() {
    JobEntity job = requiredJob();
    job.setPayload(null);

    IllegalStateException ex = assertThrows(IllegalStateException.class, job::preUpdate);

    assertTrue(ex.getMessage().contains("payload"));
  }

  @Test
  void lifecycleCallbacksKeepTimestampsForValidJob() {
    JobEntity job = requiredJob();

    job.prePersist();
    assertNotNull(job.getCreatedAt());
    assertNotNull(job.getUpdatedAt());

    job.preUpdate();
    assertNotNull(job.getUpdatedAt());
  }

  private static JobEntity requiredJob() {
    JobEntity job = new JobEntity();
    job.setStatus(JobStatus.PENDING);
    job.setScheduledTime(Instant.parse("2026-05-07T12:00:00Z"));
    job.setJobType(JobExecutionType.SINGLE);
    job.setPriority(JobPriority.NORMAL);
    job.setBackoffPolicy(BackoffPolicy.NONE);
    job.setCronExpr("");
    job.setZoneId("UTC");
    job.setPayload(new JobPayload("com.example.Job", "run", "()V", false, List.of()));
    job.setIdempotencyKey("idem-1");
    return job;
  }
}
