package run.ratchet.testsuite.tck;

import jakarta.inject.Inject;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit5.ArquillianExtension;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.extension.ExtendWith;
import run.ratchet.tck.api.RatchetTckRuntime;
import run.ratchet.tck.api.TckJobs;
import run.ratchet.tck.jakarta.AbstractTxEnqueueContract;
import run.ratchet.tck.util.ConcurrentTestRunner;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/**
 * MongoDB subclass of {@link AbstractTxEnqueueContract}.
 *
 * <p>MongoDB does not participate in JTA. A caller's JTA rollback does not roll back a Mongo write
 * that has already been committed inside a {@code ClientSession.withTransaction} block. This
 * implementation is explicitly exempt from the commit-visible / rollback-invisible requirement.
 *
 * <p>This class is retained as a documented contract gap. Implementations that add Mongo XA support
 * in the future should remove the {@code @Disabled} annotation and validate that the contract now
 * passes.
 */
@Disabled("MongoDB store does not participate in JTA — enqueue is not rolled back by a caller TX")
@ExtendWith(ArquillianExtension.class)
class MongoTxEnqueueIT extends AbstractTxEnqueueContract {

  @Inject private RiRatchetTckRuntime runtime;

  @Override
  protected RatchetTckRuntime runtime() {
    return runtime;
  }

  @Deployment
  public static WebArchive createDeployment() {
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");

    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, "mongodb")
        .addPackage(RatchetTckRuntime.class.getPackage())
        .addPackage(AbstractTxEnqueueContract.class.getPackage())
        .addPackage(ConcurrentTestRunner.class.getPackage())
        .addClasses(RiRatchetTckRuntime.class, ListenerProbe.class, TckJobs.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }
}
