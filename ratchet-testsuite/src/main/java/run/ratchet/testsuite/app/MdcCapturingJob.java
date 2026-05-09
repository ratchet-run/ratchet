package run.ratchet.testsuite.app;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
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
  private static final AtomicBoolean STARTED = new AtomicBoolean(false);
  private static volatile long sleepMs = 5_000;

  public static void execute() {
    captureMdc();
  }

  public static void executeAndFail() {
    captureMdc();
    throw new RuntimeException("Intentional MDC capture failure");
  }

  public static void executeSlow() throws InterruptedException {
    captureMdc();
    Thread.sleep(sleepMs);
  }

  public static void setSleepMs(long ms) {
    sleepMs = ms;
  }

  public static boolean hasStarted() {
    return STARTED.get();
  }

  private static void captureMdc() {
    STARTED.set(true);
    Map<String, Object> live = MDC.getMap();
    capturedMdc = live == null ? new HashMap<>() : new HashMap<>(live);
  }

  public static Map<String, Object> getCapturedMdc() {
    return capturedMdc;
  }

  public static void reset() {
    capturedMdc = null;
    STARTED.set(false);
    sleepMs = 5_000;
  }
}
