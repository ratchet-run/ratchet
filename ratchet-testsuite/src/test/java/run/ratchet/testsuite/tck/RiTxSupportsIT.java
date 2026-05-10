package run.ratchet.testsuite.tck;

import jakarta.inject.Inject;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit5.ArquillianExtension;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.junit.jupiter.api.extension.ExtendWith;
import run.ratchet.tck.api.RatchetTckRuntime;
import run.ratchet.tck.jakarta.AbstractTxSupportsContract;

/**
 * RI subclass of {@link AbstractTxSupportsContract}. The RI builder factory methods do not write to
 * the store; their terminal {@code submit()} delegates to the same JPA path as {@code enqueueNow},
 * which participates in the JTA transaction.
 */
@ExtendWith(ArquillianExtension.class)
class RiTxSupportsIT extends AbstractTxSupportsContract {

  @Inject private RiRatchetTckRuntime runtime;

  @Override
  protected RatchetTckRuntime runtime() {
    return runtime;
  }

  @Override
  @Test
  @DisabledIfSystemProperty(named = "ratchet.test.db.type", matches = "mongodb")
  protected void enqueueSubmit_insideRolledBackTx_jobDoesNotExecute() throws Exception {
    super.enqueueSubmit_insideRolledBackTx_jobDoesNotExecute();
  }

  @Override
  @Test
  @DisabledIfSystemProperty(named = "ratchet.test.db.type", matches = "mongodb")
  protected void scheduleSubmit_insideRolledBackTx_jobDoesNotExecute() throws Exception {
    super.scheduleSubmit_insideRolledBackTx_jobDoesNotExecute();
  }

  @Deployment
  public static WebArchive createDeployment() {
    return RiTckDeployment.create(AbstractTxSupportsContract.class.getPackage());
  }
}
