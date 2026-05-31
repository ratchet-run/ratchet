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
package run.ratchet.testsuite.event;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

import jakarta.inject.Inject;
import java.time.Duration;
import java.util.List;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobStatus;
import run.ratchet.api.event.JobsBulkCancelledEvent;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.SimpleJob;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/**
 * End-to-end CDI verification that {@code cancelJobsByTag} fires exactly one {@link
 * JobsBulkCancelledEvent} per call with the expected {@code (tag, count)}, after the surrounding
 * transaction commits.
 */
class CancelJobsByTagIT extends BaseRatchetIT {

  private static final String TAG = "axon-deadline-fund-reservation";

  @Inject private TestJobService jobService;

  @Inject private JobCrudStore jobCrudStore;

  @Inject private BulkCancelEventCapture eventCapture;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");

    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addClasses(SimpleJob.class, TestJobService.class, BulkCancelEventCapture.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  @BeforeEach
  void resetState() {
    SimpleJob.resetCount();
    eventCapture.clear();
  }

  @Test
  void cancelJobsByTag_firesOneBulkEvent_andCancelsAllMatchingJobs() {
    JobHandle handle1 =
        jobService.schedule(Duration.ofHours(1), SimpleJob::execute).withTags(TAG).submit();
    JobHandle handle2 =
        jobService.schedule(Duration.ofHours(1), SimpleJob::execute).withTags(TAG).submit();
    JobHandle handle3 =
        jobService.schedule(Duration.ofHours(1), SimpleJob::execute).withTags(TAG).submit();
    JobHandle untagged = jobService.schedule(Duration.ofHours(1), SimpleJob::execute).submit();

    int cancelledCount = jobService.getScheduler().cancelJobsByTag(TAG);

    assertEquals(3, cancelledCount, "Should cancel exactly the 3 tagged jobs");

    await()
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofMillis(100))
        .untilAsserted(
            () -> {
              List<JobsBulkCancelledEvent> events = eventCapture.getEvents();
              assertEquals(1, events.size(), "Exactly one bulk event should be observed");
              JobsBulkCancelledEvent event = events.get(0);
              assertEquals(TAG, event.getTag());
              assertEquals(3, event.getCount());
            });

    assertEquals(JobStatus.CANCELED, jobCrudStore.getJobStatus(handle1.id()));
    assertEquals(JobStatus.CANCELED, jobCrudStore.getJobStatus(handle2.id()));
    assertEquals(JobStatus.CANCELED, jobCrudStore.getJobStatus(handle3.id()));
    assertEquals(
        JobStatus.PENDING,
        jobCrudStore.getJobStatus(untagged.id()),
        "Untagged job must remain PENDING");
  }

  @Test
  void cancelJobsByTag_noMatches_doesNotFireEvent() {
    jobService.schedule(Duration.ofHours(1), SimpleJob::execute).withTags("other-tag").submit();

    int cancelledCount = jobService.getScheduler().cancelJobsByTag(TAG);

    assertEquals(0, cancelledCount);
    assertEquals(
        0, eventCapture.getEvents().size(), "Bulk event must not fire when no jobs are cancelled");
  }
}
