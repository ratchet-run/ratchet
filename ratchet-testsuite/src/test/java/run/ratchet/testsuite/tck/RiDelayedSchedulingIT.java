package run.ratchet.testsuite.tck;

import run.ratchet.tck.api.AbstractDelayedSchedulingContract;
import run.ratchet.tck.api.RatchetTckRuntime;
import run.ratchet.tck.util.ConcurrentTestRunner;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;
import jakarta.inject.Inject;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit5.ArquillianExtension;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * RI subclass of {@link AbstractDelayedSchedulingContract}, disabled until {@link
 * RiRatchetTckRuntime#clock()} returns a real {@link run.ratchet.tck.api.TestClock}.
 *
 * <p>The contract base class itself uses {@code Assumptions.assumeTrue} to skip when no clock is
 * present, but the {@code @Disabled} annotation here makes the skip visible in test reports rather
 * than relying on assumption-based silent-skip.
 */
@org.junit.jupiter.api.Disabled(
    "TestClock seam not yet wired into JobExecutorService — tracked for follow-up")
@ExtendWith(ArquillianExtension.class)
class RiDelayedSchedulingIT extends AbstractDelayedSchedulingContract {

  @Inject private RiRatchetTckRuntime runtime;

  @Override
  protected RatchetTckRuntime runtime() {
    return runtime;
  }

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");

    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addPackage(RatchetTckRuntime.class.getPackage())
        .addPackage(ConcurrentTestRunner.class.getPackage())
        .addClasses(RiRatchetTckRuntime.class, ListenerProbe.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }
}
