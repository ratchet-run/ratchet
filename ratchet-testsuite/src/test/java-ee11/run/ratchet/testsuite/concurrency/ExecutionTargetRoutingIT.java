package run.ratchet.testsuite.concurrency;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.inject.Inject;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobHandle;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/**
 * Proves per-job routing on Jakarta EE 11 containers: a {@code .virtual()} job runs on the
 * application-declared {@code @ManagedExecutorDefinition(virtual = true)} pool, and a {@code
 * .platform()} job runs on the container default executor. {@link VirtualThreadOptionsProducer}
 * configures both pools (virtual is the default mode), so the two builder calls send work to
 * different executors.
 *
 * <p>The deployment runs only under the EE 11 server profiles (the {@code src/test/java-ee11}
 * source root). That both jobs complete proves both pools resolved and ran. The thread-<em>type</em>
 * assertions run only on GlassFish 8, which actually implements virtual threads for managed
 * executors. WildFly 40 does not — confirmed with the WildFly team — so on WildFly the IT only
 * exercises the {@code @ManagedExecutorDefinition} JNDI routing path, without the {@code
 * isVirtual()} distinction.
 */
class ExecutionTargetRoutingIT extends BaseRatchetIT {

  @Inject private TestJobService jobService;

  @Inject private JobCrudStore jobCrudStore;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-ee11-managed");

    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addClasses(
            VirtualThreadProbeJob.class,
            VirtualThreadOptionsProducer.class,
            VirtualThreadTestExecutor.class,
            TestJobService.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  private static boolean expectsVirtualThreads() {
    return System.getProperty("testsuite.profile", "").contains("glassfish");
  }

  @BeforeEach
  void resetProbe() {
    VirtualThreadProbeJob.reset();
  }

  @Test
  void virtualTargetedJob_runsOnVirtualPool() {
    JobHandle handle =
        jobService.enqueue(VirtualThreadProbeJob::execute).virtual().immediate().submit();

    JobAssertions.assertJobCompleted(jobCrudStore, handle);
    assertEquals(1, VirtualThreadProbeJob.invocations());
    if (expectsVirtualThreads()) {
      assertTrue(
          VirtualThreadProbeJob.allRanOnVirtualThreads(),
          "a .virtual() job must run on the virtual executor's thread; observed="
              + VirtualThreadProbeJob.threadNames());
    }
  }

  @Test
  void platformTargetedJob_runsOnPlatformPool() {
    JobHandle handle =
        jobService.enqueue(VirtualThreadProbeJob::execute).platform().immediate().submit();

    JobAssertions.assertJobCompleted(jobCrudStore, handle);
    assertEquals(1, VirtualThreadProbeJob.invocations());
    if (expectsVirtualThreads()) {
      assertFalse(
          VirtualThreadProbeJob.allRanOnVirtualThreads(),
          "a .platform() job must run on the container default (platform) executor, not the"
              + " virtual one; observed="
              + VirtualThreadProbeJob.threadNames());
    }
  }
}
