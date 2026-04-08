package run.ratchet.testsuite.app;

import java.util.HashMap;
import java.util.Map;
import org.jboss.logging.MDC;

/**
 * Snapshots the JBoss Logging {@link MDC} state at execution time so tests can assert on the
 * Ratchet-owned MDC keys ({@code jobId}, {@code node}, {@code jobCreator}) populated by {@code
 * JobMdcContext}.
 *
 * <p>Used by {@code LoggingMdcIT}.
 */
public class MdcCapturingJob {

  private static volatile Map<String, Object> capturedMdc;

  public static void execute() {
    Map<String, Object> live = MDC.getMap();
    capturedMdc = live == null ? new HashMap<>() : new HashMap<>(live);
  }

  public static Map<String, Object> getCapturedMdc() {
    return capturedMdc;
  }

  public static void reset() {
    capturedMdc = null;
  }
}
