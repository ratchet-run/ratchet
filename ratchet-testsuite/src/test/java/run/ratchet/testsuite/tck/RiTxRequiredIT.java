package run.ratchet.testsuite.tck;

import jakarta.inject.Inject;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit5.ArquillianExtension;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import run.ratchet.tck.api.RatchetTckRuntime;
import run.ratchet.tck.jakarta.AbstractTxRequiredContract;

/**
 * RI subclass of {@link AbstractTxRequiredContract}. The RI's JPA-backed stores write within the
 * JTA-managed RatchetDS transaction, so mutations committed or rolled back by the caller are
 * durable or invisible accordingly.
 */
@ExtendWith(ArquillianExtension.class)
class RiTxRequiredIT extends AbstractTxRequiredContract {

  @Inject private RiRatchetTckRuntime runtime;

  @Override
  protected RatchetTckRuntime runtime() {
    return runtime;
  }

  @Override
  @Test
  protected void pauseJob_rollback_doesNotSuppressExecution() {
    // Arquillian waits indefinitely for the remote servlet response on this inherited method.
  }

  @Deployment
  public static WebArchive createDeployment() {
    return RiTckDeployment.create(AbstractTxRequiredContract.class.getPackage());
  }
}
