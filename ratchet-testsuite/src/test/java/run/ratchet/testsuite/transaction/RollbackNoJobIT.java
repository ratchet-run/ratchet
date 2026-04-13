package run.ratchet.testsuite.transaction;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import run.ratchet.api.JobHandle;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.SimpleJob;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;
import jakarta.inject.Inject;
import jakarta.transaction.UserTransaction;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * Validates that rolling back the enclosing transaction also removes the enqueued job — no orphan
 * job rows should exist after a rollback.
 *
 * <p>JPA-only: these tests exercise JTA transaction semantics which are not applicable to document
 * stores.
 */
@EnabledIfSystemProperty(named = "ratchet.test.db.type", matches = "mysql|postgresql")
class RollbackNoJobIT extends BaseRatchetIT {

  @Inject private TestJobService jobService;

  @Inject private JobCrudStore jobCrudStore;

  @Inject private UserTransaction utx;

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
  void rollback_shouldRemoveEnqueuedJob() throws Exception {
    utx.begin();
    JobHandle handle = jobService.enqueue(SimpleJob::execute).submit();
    assertNotNull(handle);
    utx.rollback();

    var job = jobCrudStore.findById(handle.id());
    assertTrue(job.isEmpty(), "Job should not exist after transaction rollback");
  }
}
