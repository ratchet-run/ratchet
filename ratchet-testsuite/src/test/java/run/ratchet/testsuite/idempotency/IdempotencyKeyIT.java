package run.ratchet.testsuite.idempotency;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import run.ratchet.api.JobHandle;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.SimpleJob;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;
import jakarta.inject.Inject;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class IdempotencyKeyIT extends BaseRatchetIT {

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
  void duplicateIdempotencyKey_shouldReturnExistingHandle() {
    String key = "webhook-delivery-12345";

    JobHandle first = jobService.enqueue(SimpleJob::execute).withIdempotencyKey(key).submit();
    assertNotNull(first);

    // Second submission with same key should return the existing job (idempotent)
    JobHandle second = jobService.enqueue(SimpleJob::execute).withIdempotencyKey(key).submit();
    assertNotNull(second);

    assertEquals(first.id(), second.id(), "Duplicate idempotency key should return same job ID");
    JobAssertions.assertJobCompleted(jobCrudStore, first);
    assertEquals(1, SimpleJob.getInvocationCount());
  }

  @Test
  void differentIdempotencyKeys_shouldBothExecute() {
    JobHandle first = jobService.enqueue(SimpleJob::execute).withIdempotencyKey("key-a").submit();

    JobHandle second = jobService.enqueue(SimpleJob::execute).withIdempotencyKey("key-b").submit();

    assertNotNull(first);
    assertNotNull(second);

    JobAssertions.assertJobCompleted(jobCrudStore, first);
    JobAssertions.assertJobCompleted(jobCrudStore, second);
    assertEquals(2, SimpleJob.getInvocationCount());
  }
}
