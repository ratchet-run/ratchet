package run.ratchet.ri.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link RatchetConfiguration#validateNumericEnvVars()}.
 *
 * <p>Verifies that a numeric env var set to an unparseable value:
 *
 * <ul>
 *   <li>Does NOT fail construction — silent fallback to defaults is intentional resilience.
 *   <li>Emits a startup WARN log naming the offending env var so operators see the signal.
 *   <li>Causes the corresponding getter to return the hard-coded default instead.
 * </ul>
 *
 * <p>Uses system properties rather than real env vars because {@link
 * RatchetConfiguration#validateNumericEnvVars()} checks both ({@code System.getenv} first, then
 * {@code System.getProperty} as a fallback). The env var path cannot be stubbed from unit tests
 * without a library like junit-pioneer; the sysprop path is trivially testable and covers the same
 * validator logic.
 *
 * <p><b>Logging backend assumption.</b> This test attaches a JUL handler to capture WARN output. It
 * relies on JBoss Logging's fallback behavior: when no other logging backend (JBoss LogManager,
 * SLF4J, Log4j2) is on the test classpath, JBoss Logging delegates to {@code java.util.logging}. If
 * a future change adds an alternate backend to the {@code ratchet} test classpath, this
 * capturing approach will silently stop working and the WARN assertion will fail. The
 * single-backend assumption is the cheapest way to avoid pulling a logging-test library into a
 * module that deliberately has no logging backend dependency.
 */
class RatchetConfigurationTest {

  private static final String BAD_POLLER_BATCH_SIZE = "banana";
  private static final String BAD_KEY_PREFERRED = "RATCHET_POLLER_BATCH_SIZE";
  private static final String BAD_KEY_LEGACY = "POLLER_BATCH_SIZE";

  private CapturingHandler handler;
  private Logger julLogger;

  @BeforeEach
  void attachLogHandler() {
    // Proactively clear the sysprops up front — not just in @AfterEach — so a leaked value from
    // another test (or an environment that pre-sets these) can't cause missingEnvVarEmitsNoWarn
    // to fail spuriously.
    System.clearProperty(BAD_KEY_PREFERRED);
    System.clearProperty(BAD_KEY_LEGACY);

    // Ensure JUL handler defaults are initialized before we touch the configured logger.
    LogManager.getLogManager();
    julLogger = Logger.getLogger(RatchetConfiguration.class.getName());
    julLogger.setLevel(Level.ALL);
    handler = new CapturingHandler();
    handler.setLevel(Level.ALL);
    julLogger.addHandler(handler);
  }

  @AfterEach
  void cleanUp() {
    if (handler != null) {
      julLogger.removeHandler(handler);
    }
    System.clearProperty(BAD_KEY_PREFERRED);
    System.clearProperty(BAD_KEY_LEGACY);
  }

  @Test
  void invalidEnvVarEmitsWarnAndFallsBackToDefault() {
    // Set the unparseable value on the preferred (RATCHET_*) sysprop. The validator reads
    // System.getenv first and System.getProperty as a fallback, so the validator picks this up.
    System.setProperty(BAD_KEY_PREFERRED, BAD_POLLER_BATCH_SIZE);

    RatchetConfiguration config = new RatchetConfiguration();
    config.validateNumericEnvVars();

    // Behavioral assertion: silent fallback is intentional. The getter must still return the
    // hard-coded default (50 per the constructor).
    assertEquals(
        Integer.valueOf(50),
        config.getPollerBatchSize(),
        "Unparseable env var must fall back to the hard-coded default");

    // Observability assertion: at least one WARN log naming RATCHET_POLLER_BATCH_SIZE=banana.
    boolean sawWarn =
        handler.records.stream()
            .filter(r -> r.getLevel().intValue() >= Level.WARNING.intValue())
            .map(LogRecord::getMessage)
            .anyMatch(
                msg ->
                    msg != null
                        && msg.contains(BAD_KEY_PREFERRED)
                        && msg.contains(BAD_POLLER_BATCH_SIZE));
    assertTrue(
        sawWarn,
        "Expected a WARN naming "
            + BAD_KEY_PREFERRED
            + "="
            + BAD_POLLER_BATCH_SIZE
            + "; captured records: "
            + handler.summary());
  }

  @Test
  void validEnvVarEmitsNoWarn() {
    // A valid numeric value should not produce any WARN.
    System.setProperty(BAD_KEY_PREFERRED, "25");

    RatchetConfiguration config = new RatchetConfiguration();
    config.validateNumericEnvVars();

    // No WARN about our key specifically.
    boolean sawWarnForOurKey =
        handler.records.stream()
            .filter(r -> r.getLevel().intValue() >= Level.WARNING.intValue())
            .map(LogRecord::getMessage)
            .anyMatch(msg -> msg != null && msg.contains(BAD_KEY_PREFERRED));
    assertTrue(!sawWarnForOurKey, "Valid numeric value must not produce a WARN");

    // Getter returns the configured value (via parseIntOrDefault reading the sysprop).
    assertEquals(Integer.valueOf(25), config.getPollerBatchSize());
  }

  @Test
  void missingEnvVarEmitsNoWarn() {
    // No env var or sysprop set — validator should silently walk the list without warning.
    // The getter returns the default.
    RatchetConfiguration config = new RatchetConfiguration();
    config.validateNumericEnvVars();

    boolean sawAnyWarn =
        handler.records.stream().anyMatch(r -> r.getLevel().intValue() >= Level.WARNING.intValue());
    assertTrue(!sawAnyWarn, "Unset env vars must not produce WARN");
    assertEquals(Integer.valueOf(50), config.getPollerBatchSize());
  }

  /** Minimal JUL handler that collects all log records for assertions. */
  private static final class CapturingHandler extends Handler {
    final List<LogRecord> records = new ArrayList<>();

    @Override
    public void publish(LogRecord record) {
      records.add(record);
    }

    @Override
    public void flush() {}

    @Override
    public void close() {}

    String summary() {
      StringBuilder sb = new StringBuilder("[");
      for (LogRecord r : records) {
        sb.append(r.getLevel()).append(": ").append(r.getMessage()).append("; ");
      }
      return sb.append("]").toString();
    }
  }
}
