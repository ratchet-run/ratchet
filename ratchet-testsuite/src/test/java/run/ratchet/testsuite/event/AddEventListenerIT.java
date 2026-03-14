package run.ratchet.testsuite.event;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import run.ratchet.api.JobHandle;
import run.ratchet.api.event.JobCompletedEvent;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.SimpleJob;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;
import jakarta.inject.Inject;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Validates that {@code addEventListener()} works for programmatic (non-CDI) event listeners. */
class AddEventListenerIT extends BaseRatchetIT {

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
  void addEventListener_shouldReceiveEvents() throws InterruptedException {
    CopyOnWriteArrayList<Object> received = new CopyOnWriteArrayList<>();
    CountDownLatch latch = new CountDownLatch(1);

    jobService
        .getScheduler()
        .addEventListener(
            event -> {
              received.add(event);
              if (event instanceof JobCompletedEvent) {
                latch.countDown();
              }
            });

    JobHandle handle = jobService.enqueueNow(SimpleJob::execute);
    assertNotNull(handle);
    JobAssertions.assertJobCompleted(jobCrudStore, handle);

    boolean eventReceived = latch.await(10, TimeUnit.SECONDS);
    assertTrue(eventReceived, "Programmatic listener should have received JobCompletedEvent");
    assertTrue(
        received.stream().anyMatch(e -> e instanceof JobCompletedEvent),
        "Received events should contain JobCompletedEvent");
  }
}
