package run.ratchet.testsuite.tck;

import jakarta.inject.Inject;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit5.ArquillianExtension;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.extension.ExtendWith;
import run.ratchet.tck.api.RatchetTckRuntime;
import run.ratchet.tck.api.TckJobs;
import run.ratchet.tck.jakarta.AbstractTxEnqueueContract;
import run.ratchet.tck.util.ConcurrentTestRunner;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/**
 * RI subclass of {@link AbstractTxEnqueueContract}. The RI's MySQL store enqueues via JPA against
 * the JTA-managed RatchetDS, so it inherits the caller's transaction context.
 */
@ExtendWith(ArquillianExtension.class)
class RiTxEnqueueIT extends AbstractTxEnqueueContract {

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
        .addPackage(AbstractTxEnqueueContract.class.getPackage())
        .addPackage(ConcurrentTestRunner.class.getPackage())
        .addClasses(RiRatchetTckRuntime.class, ListenerProbe.class, TckJobs.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }
}
