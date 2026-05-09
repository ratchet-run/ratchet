package run.ratchet.testsuite.chain;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.FailingJob;
import run.ratchet.testsuite.app.SimpleJob;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.app.WorkflowBranchTracker;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/** Validates workflow branching: success branch fires on success, failure branch on failure. */
class WorkflowBranchIT extends BaseRatchetIT {

  private static final Duration BRANCH_TIMEOUT = Duration.ofSeconds(10);

  @Inject private TestJobService jobService;

  @Inject private JobCrudStore jobCrudStore;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");

    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addClasses(
            SimpleJob.class, FailingJob.class, WorkflowBranchTracker.class, TestJobService.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  @BeforeEach
  void resetTrackers() {
    SimpleJob.resetCount();
    FailingJob.resetCount();
    WorkflowBranchTracker.reset();
  }

  @Test
  void successfulJob_shouldTriggerSuccessBranch() {
    JobHandle handle =
        jobService
            .enqueue(SimpleJob::execute)
            .thenOnSuccess(WorkflowBranchTracker::onSuccessScenarioSuccess)
            .thenOnFailure(WorkflowBranchTracker::onSuccessScenarioFailure)
            .submit();

    JobAssertions.assertJobCompleted(jobCrudStore, handle);

    await()
        .atMost(BRANCH_TIMEOUT)
        .untilAsserted(() -> assertTrue(WorkflowBranchTracker.successScenarioSuccessBranchFired()));

    assertPersistedBranchStatus(handle, "onSuccessScenarioSuccess", JobStatus.SUCCEEDED);
    assertPersistedBranchStatus(handle, "onSuccessScenarioFailure", JobStatus.PENDING);

    assertFalse(
        WorkflowBranchTracker.successScenarioFailureBranchFired(),
        "Failure branch should not fire on success");
  }

  @Test
  void failingJob_shouldTriggerFailureBranch() {
    JobHandle handle =
        jobService
            .enqueue(FailingJob::execute)
            .thenOnSuccess(WorkflowBranchTracker::onFailureScenarioSuccess)
            .thenOnFailure(WorkflowBranchTracker::onFailureScenarioFailure)
            .submit();

    JobAssertions.assertJobFailed(jobCrudStore, handle);

    await()
        .atMost(BRANCH_TIMEOUT)
        .untilAsserted(() -> assertTrue(WorkflowBranchTracker.failureScenarioFailureBranchFired()));

    assertPersistedBranchStatus(handle, "onFailureScenarioFailure", JobStatus.SUCCEEDED);
    assertPersistedBranchStatus(handle, "onFailureScenarioSuccess", JobStatus.PENDING);

    assertFalse(
        WorkflowBranchTracker.failureScenarioSuccessBranchFired(),
        "Success branch should not fire on failure");
  }

  private void assertPersistedBranchStatus(
      JobHandle parentHandle, String branchMethodName, JobStatus expectedStatus) {
    await()
        .atMost(BRANCH_TIMEOUT)
        .untilAsserted(
            () -> {
              List<JobEntity> branches = jobCrudStore.findDependants(parentHandle.id());
              JobEntity branch =
                  branches.stream()
                      .filter(job -> branchMethodName.equals(job.getMethodName()))
                      .findFirst()
                      .orElseThrow(
                          () ->
                              new AssertionError(
                                  "Expected persisted workflow branch "
                                      + branchMethodName
                                      + " for parent "
                                      + parentHandle.id()));

              assertEquals(
                  expectedStatus,
                  branch.getStatus(),
                  "Persisted workflow branch " + branchMethodName + " has unexpected status");
            });
  }
}
