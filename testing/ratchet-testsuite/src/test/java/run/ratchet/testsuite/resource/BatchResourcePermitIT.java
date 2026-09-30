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
package run.ratchet.testsuite.resource;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.inject.Inject;
import java.time.Duration;
import java.util.List;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobStatus;
import run.ratchet.ri.core.ResourcePermitService;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.ResourceTestJob;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/** Validates that a batch-level resource caps how many children of one batch run at once. */
class BatchResourcePermitIT extends BaseRatchetIT {

  private static final int PERMITS = 2;
  private static final List<String> ITEMS = List.of("r1", "r2", "r3", "r4", "r5", "r6");

  @Inject private TestJobService jobService;

  @Inject private JobCrudStore jobCrudStore;

  @Inject private ResourcePermitService resourcePermitService;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");

    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addClasses(ResourceTestJob.class, TestJobService.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  @BeforeEach
  void resetState() {
    ResourceTestJob.reset();
  }

  @Test
  void batchChildrenSharingAResource_shouldNeverExceedPermitCount() {
    resourcePermitService.configureResource("batch-res", PERMITS, 500, "batch test");

    JobHandle handle =
        jobService
            .enqueueBatch("resource-batch")
            .forEach(ITEMS, ResourceTestJob::executeItem)
            .withResource("batch-res")
            .submit();

    // Children wait for permits between attempts, so allow well beyond the serial run time.
    JobAssertions.assertBatchSucceeded(jobCrudStore, handle, Duration.ofSeconds(90));
    await()
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofMillis(200))
        .until(() -> ResourceTestJob.getCompletedCount() >= ITEMS.size());

    List<JobEntity> children =
        jobCrudStore.findDependants(handle.id(), JobCrudStore.DEFAULT_PAGE_LIMIT, 0);
    assertEquals(ITEMS.size(), children.size(), "every item should become one batch child");
    for (JobEntity child : children) {
      assertEquals(JobStatus.SUCCEEDED, child.getStatus(), "child " + child.getId());
      assertEquals("batch-res", child.getResourceName(), "child " + child.getId());
    }
    assertEquals(ITEMS.size(), ResourceTestJob.getCompletedCount());
    assertTrue(
        ResourceTestJob.getMaxConcurrentSeen() <= PERMITS,
        "Max concurrent children should be <= "
            + PERMITS
            + " (permit limit) but was "
            + ResourceTestJob.getMaxConcurrentSeen());
  }
}
