package run.ratchet.testsuite.tck;

import jakarta.inject.Inject;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit5.ArquillianExtension;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.extension.ExtendWith;
import run.ratchet.tck.api.AbstractJobCancelContract;
import run.ratchet.tck.api.RatchetTckRuntime;

/** RI subclass of {@link AbstractJobCancelContract}. */
@ExtendWith(ArquillianExtension.class)
class RiJobCancelIT extends AbstractJobCancelContract {

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
