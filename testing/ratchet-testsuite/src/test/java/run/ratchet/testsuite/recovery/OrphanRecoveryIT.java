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
package run.ratchet.testsuite.recovery;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.inject.Inject;
import java.time.Instant;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobStatus;
import run.ratchet.ri.core.PollerScheduler;
import run.ratchet.ri.core.ResourcePermitService;
import run.ratchet.ri.core.internal.OrphanRecoveryTimer;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.store.spi.JobBatchStatusStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.NodeStore;
import run.ratchet.testsuite.app.SimpleJob;
import run.ratchet.testsuite.app.TestDataManipulator;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.PollerControl;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/** Proves dead-node recovery through the production timer and a live worker. */
class OrphanRecoveryIT extends BaseRatchetIT {

  private static final String DEAD_NODE = "orphan-recovery-dead-node";
  private static final String RESOURCE = "orphan-recovery-resource";

  @Inject private TestJobService jobService;
  @Inject private JobCrudStore jobCrudStore;
  @Inject private JobBatchStatusStore jobBatchStatusStore;
  @Inject private NodeStore nodeStore;
  @Inject private NodeIdentityProvider nodeIdentityProvider;
  @Inject private ResourcePermitService resourcePermitService;
  @Inject private PollerScheduler pollerScheduler;
  @Inject private OrphanRecoveryTimer orphanRecoveryTimer;
  @Inject private TestDataManipulator dataManipulator;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");

    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addClasses(SimpleJob.class, TestJobService.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  @BeforeEach
  void resetJobs() {
    SimpleJob.resetCount();
  }

  @Test
  void deadNodeJobIsRecoveredAndExecutedWhileLiveNodeJobIsPreserved() {
    PollerControl.stopAndAwait(pollerScheduler);
    orphanRecoveryTimer.stop();

    String liveNode = nodeIdentityProvider.getNodeId();
    Instant now = Instant.now();
    Instant stale = now.minusSeconds(300);
    nodeStore.upsertHeartbeat(DEAD_NODE, stale);
    nodeStore.upsertHeartbeat(liveNode, now);

    JobHandle orphan = jobService.enqueueNow(SimpleJob::execute);
    JobHandle liveOwned = jobService.enqueueNow(SimpleJob::execute);
    assertTrue(jobBatchStatusStore.tryPickUpJob(orphan.id(), DEAD_NODE));
    assertTrue(jobBatchStatusStore.tryPickUpJob(liveOwned.id(), liveNode));
    dataManipulator.setJobPickedAt(orphan.id(), stale);
    dataManipulator.setJobPickedAt(liveOwned.id(), stale);

    resourcePermitService.configureResource(RESOURCE, 1, 0, "Orphan recovery integration test");
    assertTrue(resourcePermitService.tryAcquire(RESOURCE, orphan.id(), DEAD_NODE));

    orphanRecoveryTimer.recoverNow();

    assertEquals(JobStatus.PENDING, jobCrudStore.getJobStatus(orphan.id()));
    assertEquals(JobStatus.RUNNING, jobCrudStore.getJobStatus(liveOwned.id()));
    assertTrue(nodeStore.findNodeById(DEAD_NODE).isEmpty());
    assertTrue(nodeStore.findNodeById(liveNode).isPresent());
    assertTrue(resourcePermitService.tryAcquire(RESOURCE, liveOwned.id(), liveNode));
    resourcePermitService.release(RESOURCE, liveOwned.id());

    pollerScheduler.start();
    try {
      pollerScheduler.wakeup();
      JobAssertions.assertJobCompleted(jobCrudStore, orphan);
      assertEquals(JobStatus.RUNNING, jobCrudStore.getJobStatus(liveOwned.id()));
      assertEquals(1, SimpleJob.getInvocationCount());
    } finally {
      PollerControl.stopAndAwait(pollerScheduler);
    }
  }
}
