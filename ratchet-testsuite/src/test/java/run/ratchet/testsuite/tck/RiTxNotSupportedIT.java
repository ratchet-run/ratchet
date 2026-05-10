package run.ratchet.testsuite.tck;

import jakarta.inject.Inject;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit5.ArquillianExtension;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.extension.ExtendWith;
import run.ratchet.tck.api.RatchetTckRuntime;
import run.ratchet.tck.jakarta.AbstractTxNotSupportedContract;

/**
 * RI subclass of {@link AbstractTxNotSupportedContract}. The RI manages its event listener list in
 * an in-memory {@link java.util.concurrent.CopyOnWriteArrayList} that is not TX-bound, so add/
 * remove operations take effect immediately regardless of the surrounding JTA context.
 */
@ExtendWith(ArquillianExtension.class)
class RiTxNotSupportedIT extends AbstractTxNotSupportedContract {

  @Inject private RiRatchetTckRuntime runtime;

  @Override
  protected RatchetTckRuntime runtime() {
    return runtime;
  }

  @Deployment
  public static WebArchive createDeployment() {
    return RiTckDeployment.create(AbstractTxNotSupportedContract.class.getPackage());
  }
}
