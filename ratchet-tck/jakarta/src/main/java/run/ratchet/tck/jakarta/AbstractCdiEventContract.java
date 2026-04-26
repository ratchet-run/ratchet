package run.ratchet.tck.jakarta;

import static org.junit.jupiter.api.Assertions.assertTrue;

import run.ratchet.api.JobHandle;
import run.ratchet.api.event.JobCompletedEvent;
import run.ratchet.tck.api.RatchetTckRuntime;
import run.ratchet.tck.api.TckJobs;
import jakarta.inject.Inject;
import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Base contract: lifecycle events fired by a Jakarta-EE runtime MUST be observable via CDI
 * {@code @Observes}. The probe-side observation (which the runtime adapter uses) is necessary but
 * not sufficient — application code subscribes via standard CDI events, and that pathway must work
 * too.
 *
 * <p>This contract verifies a single representative event ({@link JobCompletedEvent}) flows through
 * the CDI dispatcher. Implementations that pass it are expected (but not separately verified here)
 * to fire the rest of the {@code run.ratchet.api.event.*} family the same way.
 *
 * <p>Subclasses provide an Arquillian {@code @Deployment} that bundles this contract package
 * (including {@link CdiEventCollector}) and the implementation's {@link RatchetTckRuntime} adapter.
 */
public abstract class AbstractCdiEventContract {

  @Inject protected CdiEventCollector collector;

  protected abstract RatchetTckRuntime runtime();

  protected Duration defaultTimeout() {
    return Duration.ofSeconds(10);
  }

  /** How long to wait for the CDI observer to receive an event after the probe sees COMPLETED. */
  protected Duration cdiObserverTimeout() {
    return Duration.ofSeconds(2);
  }

  @AfterEach
  void clearAfterEach() {
    runtime().clear();
    collector.reset();
    TckJobs.resetAll();
  }

  @Test
  void jobCompletedEventReachesCdiObserver() {
    JobHandle handle = runtime().scheduler().enqueueNow(TckJobs::noop);
    runtime().probe().track(handle);

    assertTrue(
        runtime().probe().awaitCompleted(handle, defaultTimeout()),
        "Probe must observe COMPLETED before asserting on the CDI observer pathway");

    assertTrue(
        collector.awaitJobId(handle.id(), cdiObserverTimeout()),
        "CDI @Observes JobCompletedEvent must receive the event for job "
            + handle.id()
            + " — the probe saw COMPLETED so the runtime did publish, but the CDI dispatcher "
            + "did not deliver to a managed observer.");
  }
}
