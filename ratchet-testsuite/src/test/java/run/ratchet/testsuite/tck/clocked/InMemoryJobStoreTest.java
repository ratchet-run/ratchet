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
package run.ratchet.testsuite.tck.clocked;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobStatus;
import run.ratchet.store.entity.JobEntity;

class InMemoryJobStoreTest {

  private static final Instant NOW = Instant.parse("2026-05-09T12:00:00Z");

  @Test
  void findTimedOutSignalJobsRejectsZeroLimit() {
    InMemoryJobStore store = new InMemoryJobStore(Clock.fixed(NOW, ZoneOffset.UTC));
    store.save(waitingJob(NOW.minusSeconds(1)));

    IllegalArgumentException thrown =
        assertThrows(IllegalArgumentException.class, () -> store.findTimedOutSignalJobs(NOW, 0));

    assertEquals("limit must be positive: 0", thrown.getMessage());
  }

  @Test
  void findTimedOutSignalJobsHonorsPositiveLimit() {
    InMemoryJobStore store = new InMemoryJobStore(Clock.fixed(NOW, ZoneOffset.UTC));
    store.save(waitingJob(NOW.minusSeconds(3)));
    store.save(waitingJob(NOW.minusSeconds(2)));

    List<JobEntity> timedOut = store.findTimedOutSignalJobs(NOW, 1);

    assertEquals(1, timedOut.size());
  }

  private static JobEntity waitingJob(Instant signalTimeout) {
    JobEntity job = new JobEntity();
    job.setStatus(JobStatus.WAITING);
    job.setSignalTimeout(signalTimeout);
    return job;
  }
}
