package run.ratchet.testsuite.tck;

import jakarta.inject.Inject;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit5.ArquillianExtension;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.extension.ExtendWith;
import run.ratchet.ri.resilience.CircuitBreakerRegistry;
import run.ratchet.spi.ResilienceStrategy;
import run.ratchet.tck.api.AbstractResilienceStrategyContract;

/** RI subclass of {@link AbstractResilienceStrategyContract}. */
@ExtendWith(ArquillianExtension.class)
class RiResilienceStrategyIT extends AbstractResilienceStrategyContract {

  @Inject private ResilienceStrategy resilienceStrategy;
  @Inject private CircuitBreakerRegistry circuitBreakerRegistry;

  @Override
  protected ResilienceStrategy resilienceStrategy() {
    return resilienceStrategy;
  }

  @Override
  protected void forceOpenCircuit(String serviceName) {
    circuitBreakerRegistry.openBreaker(serviceName);
  }

  @Deployment
  public static WebArchive createDeployment() {
    return RiTckDeployment.create();
  }
}
