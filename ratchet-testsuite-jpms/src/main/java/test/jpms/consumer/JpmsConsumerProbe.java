package test.jpms.consumer;

// Positive imports — must compile under JPMS module-path resolution.
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.event.JobCompletedEvent;
import run.ratchet.api.exception.JobTimeoutException;
import run.ratchet.spi.ClassPolicy;
import run.ratchet.spi.RetryPolicy;

// Negative test — uncomment to confirm JPMS encapsulation prevents access to internal RI types.
// Verified manually 2026-04-08: uncommenting produces compile error
//   "package run.ratchet.ri.core is not visible
//    (package run.ratchet.ri.core is declared in the unnamed module, but module
//    ratchet.testsuite.jpms does not read it)"
// import run.ratchet.ri.core.JobTask;

/**
 * Probe class that intentionally references types from every package {@code ratchet-api}'s {@code
 * module-info.java} declares as exported. The compile success of this file IS the verification: if
 * any export is removed or renamed, this file will fail to compile and the Maven build will break.
 *
 * <p>Negative coverage: the commented-out {@code JobTask} import above must remain commented in
 * source control. To run the negative test manually, uncomment it and run {@code mvn -pl
 * ratchet-testsuite-jpms compile} — the build must fail with a "not visible" error pointing at the
 * {@code ratchet.ri} module. (Automating the negative test requires fork+grep against javac stderr,
 * which is fragile across JDK versions; the manual smoke test is sufficient and the commented
 * import documents the expected behavior.)
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
