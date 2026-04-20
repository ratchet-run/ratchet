package test.jpms.consumer;

// Positive imports — must compile under JPMS module-path resolution.
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.event.JobCompletedEvent;
import run.ratchet.api.exception.JobTimeoutException;
import run.ratchet.spi.ClassPolicy;
import run.ratchet.spi.RetryPolicy;

/**
 * Probe class that intentionally references types from every package {@code ratchet-api}'s {@code
 * module-info.java} declares as exported. The compile success of this file IS the verification: if
 * any export is removed or renamed, this file will fail to compile and the Maven build will break.
 */
@SuppressWarnings("unused")
public final class JpmsConsumerProbe {

  private JpmsConsumerProbe() {
    // Probe class — never instantiated.
  }

  /**
   * Reference every imported type so the compile actually pulls them in (some compilers will
   * silently strip unused imports without flagging them).
   */
  static Class<?>[] references() {
    return new Class<?>[] {
      JobHandle.class,
      JobSchedulerService.class,
      JobCompletedEvent.class,
      JobTimeoutException.class,
      ClassPolicy.class,
      RetryPolicy.class,
    };
  }
}
