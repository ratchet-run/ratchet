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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobStatus;
import run.ratchet.store.entity.ArchivedJobEntity;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobPayload;

class ArchiveHelperTest {

  @Test
  void buildArchiveRejectsNullJobWithClearContract() {
    assertThrows(
        NullPointerException.class, () -> ArchiveHelper.buildArchive(null, "retention", "node"));
  }

  @Test
  void buildArchivePreservesNullableExecutionFieldsForIncompleteTerminalJob() {
    JobEntity job = terminalJobWithoutExecutionTimes();

    ArchivedJobEntity archive = ArchiveHelper.buildArchive(job, "retention", "node-1");

    assertEquals(job.getId(), archive.getOriginalJobId());
    assertNull(archive.getFirstExecutionTime());
    assertNull(archive.getCompletionTime());
    assertNull(archive.getTotalExecutionTimeMs());
    assertNull(archive.getQueueWaitMs());
  }

  private static JobEntity terminalJobWithoutExecutionTimes() {
    JobEntity job = new JobEntity();
    job.setId(UUID.fromString("00000000-0000-0000-0000-000000000001"));
    job.setStatus(JobStatus.CANCELED);
    job.setJobType(JobExecutionType.SINGLE);
    job.setPriority(JobPriority.NORMAL);
    job.setAttempts(1);
    job.setMaxRetries(3);
    job.setBackoffPolicy(BackoffPolicy.NONE);
    job.setBackoffParamMs(0);
    job.setTimeoutSec(60);
    job.setCronExpr("");
    job.setZoneId("UTC");
    job.setScheduledTime(Instant.parse("2026-05-07T12:00:00Z"));
    job.setCreatedAt(Instant.parse("2026-05-07T12:00:01Z"));
    job.setPayload(new JobPayload("com.example.Job", "run", "()V", false, List.of()));
    job.setIdempotencyKey("idem-1");
    return job;
  }
}
