package run.ratchet.testsuite.transaction;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import run.ratchet.api.JobHandle;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.SimpleJob;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.transaction.UserTransaction;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Validates that job enqueue participates in the caller's JTA transaction — the job row and any
 * business data commit atomically.
 */
class TransactionalEnqueueIT extends BaseRatchetIT {

  @Inject private TestJobService jobService;

  @Inject private JobCrudStore jobCrudStore;

  @PersistenceContext private EntityManager em;

  @Inject private UserTransaction utx;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");

    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addClasses(SimpleJob.class, TestJobService.class)
        .addTestInfrastructure()
        .addBeansXml()
        .addPersistenceXml(dbType)
        .addDataSource()
        .build();
  }

  @BeforeEach
  void resetJobs() {
    SimpleJob.resetCount();
  }

  @Test
  void enqueue_withinTransaction_shouldCommitWithTransaction() {
    // Job is enqueued within implicit container-managed transaction,
    // so it commits atomically with any other work in the same tx.
    JobHandle handle = jobService.enqueue(SimpleJob::execute).submit();

    assertNotNull(handle);

    // Job should be visible and eventually complete
    var job = jobCrudStore.findById(handle.id());
    assertNotNull(job, "Job row should be committed and visible");
    JobAssertions.assertJobCompleted(jobCrudStore, handle);
  }
}
