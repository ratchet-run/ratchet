package run.ratchet.testsuite.executor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import run.ratchet.api.JobHandle;
import run.ratchet.ri.cdi.DefaultExecutorProvider;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.SimpleJob;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;
import jakarta.inject.Inject;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Negative-control test for the executor SPI default path.
 *
 * <p>The standard {@code addStoreInfrastructure()} archive includes no test override for {@link
 * ExecutorProvider}, so CDI must resolve the production {@link DefaultExecutorProvider} bean from
 * the {@code ratchet} library jar. This guards against accidentally re-introducing a test-only
 * executor override that would mask container/JNDI portability bugs in the production class.
 */
class DefaultExecutorProviderIT extends BaseRatchetIT {

  @Inject private ExecutorProvider executorProvider;
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
  void resetJobCounter() {
    SimpleJob.resetCount();
  }

  @Test
  void cdiResolvesProductionDefaultExecutorProvider() {
    assertNotNull(executorProvider, "ExecutorProvider must be CDI-resolvable");
    assertInstanceOf(
        DefaultExecutorProvider.class,
        executorProvider,
        "Expected production DefaultExecutorProvider — a test override would mask portability bugs.");
  }

  @Test
  void jobExecutorIsUsableAndCachedAcrossCalls() throws InterruptedException {
    ExecutorService first = executorProvider.getJobExecutor();
    assertNotNull(first, "job executor must resolve");
    assertSame(first, executorProvider.getJobExecutor(), "executor reference must be cached");

    // Lifecycle methods (isShutdown / shutdown) are forbidden by Jakarta Concurrency on managed
    // executors, so prove liveness by actually executing a task.
    CountDownLatch latch = new CountDownLatch(1);
    first.execute(latch::countDown);
    assertTrue(
        latch.await(5, TimeUnit.SECONDS),
        "container-managed job executor must run submitted tasks");
  }

  @Test
  void scheduledExecutorIsUsableAndCachedAcrossCalls() throws InterruptedException {
    ScheduledExecutorService first = executorProvider.getScheduledExecutor();
    assertNotNull(first, "scheduled executor must resolve");
    assertSame(first, executorProvider.getScheduledExecutor(), "executor reference must be cached");

    CountDownLatch latch = new CountDownLatch(1);
    first.schedule(latch::countDown, 10, TimeUnit.MILLISECONDS);
    assertTrue(
        latch.await(5, TimeUnit.SECONDS),
        "container-managed scheduled executor must run scheduled tasks");
  }

  @Test
  void smallJobRunsThroughDefaultProviderPath() {
    JobHandle handle = jobService.enqueueNow(SimpleJob::execute);
    assertNotNull(handle, "enqueueNow must return a handle");

    JobAssertions.assertJobCompleted(jobCrudStore, handle);
    assertEquals(1, SimpleJob.getInvocationCount(), "job must execute exactly once");
  }
}
