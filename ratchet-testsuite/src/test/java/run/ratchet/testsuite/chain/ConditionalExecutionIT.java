package run.ratchet.testsuite.chain;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.inject.Inject;
import java.time.Duration;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobResult;
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

/** Validates conditional workflow execution using {@code when(predicate, task)}. */
class ConditionalExecutionIT extends BaseRatchetIT {

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
  void whenConditionTrue_shouldExecuteConditionalBranch() {
    JobHandle handle =
        jobService
            .enqueue(SimpleJob::execute)
            .<Void>when(JobResult::isSuccess, WorkflowBranchTracker::onConditional)
            .<Void>when(JobResult::isFailure, WorkflowBranchTracker::onFailure)
            .submit();

    JobAssertions.assertJobCompleted(jobCrudStore, handle);

    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(() -> assertTrue(WorkflowBranchTracker.conditionalBranchFired()));

    await()
        .during(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(
            () -> {
              assertEquals(
                  1,
                  WorkflowBranchTracker.conditionalBranchExecutionCount(),
                  "Conditional branch should execute exactly once");
              assertFalse(
                  WorkflowBranchTracker.failureBranchFired(),
                  "Failure conditional branch should not fire when condition is false");
            });
  }

  @Test
  void whenConditionFalse_shouldNotExecuteConditionalBranch() {
    JobHandle handle =
        jobService
            .enqueue(FailingJob::execute)
            .<Void>when(JobResult::isSuccess, WorkflowBranchTracker::onConditional)
            .submit();

    JobAssertions.assertJobFailed(jobCrudStore, handle);

    assertOnlyDependantStatus(handle, JobStatus.CANCELED);
    assertFalse(
        WorkflowBranchTracker.conditionalBranchFired(),
        "Conditional branch should not fire when condition is false");
  }

  @Test
  void whenPredicateThrows_shouldNotExecuteConditionalBranch() {
    JobHandle handle =
        jobService
            .enqueue(SimpleJob::execute)
            .<Void>when(
                WorkflowBranchTracker::throwingCondition, WorkflowBranchTracker::onConditional)
            .submit();

    JobAssertions.assertJobCompleted(jobCrudStore, handle);

    assertOnlyDependantStatus(handle, JobStatus.CANCELED);
    assertFalse(
        WorkflowBranchTracker.conditionalBranchFired(),
        "Conditional branch should not fire when predicate evaluation fails");
  }

  @Test
  void whenConditionalTaskThrows_shouldCaptureExceptionInResult() {
    JobHandle handle =
        jobService
            .enqueue(SimpleJob::execute)
            .<Void>when(JobResult::isSuccess, WorkflowBranchTracker::throwingConditional)
            .submit();

    JobAssertions.assertJobCompleted(jobCrudStore, handle);

    assertOnlyDependantStatus(handle, JobStatus.FAILED);
  }

  private void assertOnlyDependantStatus(JobHandle handle, JobStatus expected) {
    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () -> {
              JobEntity child = onlyDependant(handle);
              assertEquals(
                  expected,
                  child.getStatus(),
                  "Expected conditional branch child to be " + expected);
            });
  }

  private JobEntity onlyDependant(JobHandle handle) {
    var dependants = jobCrudStore.findDependants(handle.id());
    assertEquals(1, dependants.size(), "Expected exactly one conditional branch child");
    return dependants.get(0);
  }
}
