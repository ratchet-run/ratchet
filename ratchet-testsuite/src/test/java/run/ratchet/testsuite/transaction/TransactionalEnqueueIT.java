package run.ratchet.testsuite.transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.inject.Inject;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import run.ratchet.api.JobHandle;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.SimpleJob;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/**
 * Validates that job enqueue participates in the caller's JTA transaction: the submitted job row
 * commits and the committed job is eligible for execution.
 *
 * <p>JPA-only: these tests exercise JTA transaction semantics which are not applicable to document
 * stores.
 */
@EnabledIfSystemProperty(named = "ratchet.test.db.type", matches = "mysql|postgresql")
class TransactionalEnqueueIT extends BaseRatchetIT {

  @Inject private TestJobService jobService;

  @Inject private JobCrudStore jobCrudStore;

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
  void enqueue_withinTransaction_shouldCommitWithTransaction() {
    JobHandle handle = jobService.enqueue(SimpleJob::execute).submit();

    var job = jobCrudStore.findById(handle.id());
    assertTrue(job.isPresent(), "Job row should be committed and visible");
    assertEquals(handle.id(), job.orElseThrow().getId(), "Visible row should match submitted job");

    JobAssertions.assertJobCompleted(jobCrudStore, handle);
    assertEquals(1, SimpleJob.getInvocationCount(), "Committed job should execute exactly once");
  }
}
