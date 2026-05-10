package run.ratchet.testsuite.tck;

import jakarta.inject.Inject;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit5.ArquillianExtension;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.extension.ExtendWith;
import run.ratchet.tck.api.AbstractSignalDecisionContract;
import run.ratchet.tck.api.RatchetTckRuntime;

/** RI subclass of {@link AbstractSignalDecisionContract}. */
@ExtendWith(ArquillianExtension.class)
class RiSignalDecisionIT extends AbstractSignalDecisionContract {

  @Inject private RiRatchetTckRuntime runtime;

  @Override
  protected RatchetTckRuntime runtime() {
    return runtime;
  }

  @Deployment
  public static WebArchive createDeployment() {
    return RiTckDeployment.create();
  }
}
