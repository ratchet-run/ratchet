package run.ratchet.testsuite.tck;

import jakarta.inject.Inject;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit5.ArquillianExtension;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.extension.ExtendWith;
import run.ratchet.tck.api.RatchetTckRuntime;
import run.ratchet.tck.jakarta.AbstractCdiEventContract;
import run.ratchet.tck.jakarta.CdiEventCollector;

/** RI subclass of {@link AbstractCdiEventContract}. */
@ExtendWith(ArquillianExtension.class)
class RiCdiEventIT extends AbstractCdiEventContract {

  @Inject private RiRatchetTckRuntime runtime;

  @Override
  protected RatchetTckRuntime runtime() {
    return runtime;
  }

  @Deployment
  public static WebArchive createDeployment() {
    return RiTckDeployment.createWith(
        new Package[] {AbstractCdiEventContract.class.getPackage()}, CdiEventCollector.class);
  }
}
