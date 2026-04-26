package run.ratchet.testsuite.tck.clocked;

import run.ratchet.tck.api.SteppingTestClock;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.enterprise.inject.Produces;
import jakarta.interceptor.Interceptor;
import java.time.Clock;

/**
 * CDI producers that override the production {@link Clock} (from {@code RatchetProducer}) with a
 * single {@link SteppingTestClock} instance. Activated only inside the {@code
 * RiDelayedSchedulingIT} archive via its {@code beans-clocked.xml}; other Ri*IT deployments are
 * unaffected because the alternative is not declared in their {@code beans.xml}.
 */
@ApplicationScoped
public class ClockedTestProducers {

  private final SteppingTestClock instance = new SteppingTestClock();

  @Produces
  @Alternative
  @Priority(Interceptor.Priority.APPLICATION + 100)
  @ApplicationScoped
  public Clock testClock() {
    return instance;
  }

  @Produces
  @ApplicationScoped
  public SteppingTestClock testClockBean() {
    return instance;
  }
}
