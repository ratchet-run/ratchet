package run.ratchet.testsuite.chain;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import run.ratchet.api.JobHandle;
import run.ratchet.api.JobResult;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.FailingJob;
import run.ratchet.testsuite.app.SimpleJob;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.app.WorkflowBranchTracker;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;
import jakarta.inject.Inject;
import java.time.Duration;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
        .addTestInfrastructure()
        .addBeansXml()
        .addPersistenceXml(dbType)
        .addDataSource()
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
            .submit();

    JobAssertions.assertJobCompleted(jobCrudStore, handle);

    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(() -> assertTrue(WorkflowBranchTracker.conditionalBranchFired()));
  }

  @Test
  void whenConditionFalse_shouldNotExecuteConditionalBranch() {
    JobHandle handle =
        jobService
            .enqueue(FailingJob::execute)
            .<Void>when(JobResult::isSuccess, WorkflowBranchTracker::onConditional)
            .submit();

    JobAssertions.assertJobFailed(jobCrudStore, handle);

    await()
        .during(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(
            () ->
                assertFalse(
                    WorkflowBranchTracker.conditionalBranchFired(),
                    "Conditional branch should not fire when condition is false"));
  }
}
