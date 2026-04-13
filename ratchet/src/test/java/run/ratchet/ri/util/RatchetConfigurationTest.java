package run.ratchet.ri.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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

// Tests for RatchetConfiguration#validateNumericEnvVars() using sysprops as env-var substitutes.
class RatchetConfigurationTest {

  private static final String BAD_POLLER_BATCH_SIZE = "banana";
  private static final String BAD_KEY_PREFERRED = "RATCHET_POLLER_BATCH_SIZE";
  private static final String BAD_KEY_LEGACY = "POLLER_BATCH_SIZE";

  private CapturingHandler handler;
  private Logger julLogger;

  @BeforeEach
  void attachLogHandler() {
    System.clearProperty(BAD_KEY_PREFERRED);
    System.clearProperty(BAD_KEY_LEGACY);

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
    System.setProperty(BAD_KEY_PREFERRED, BAD_POLLER_BATCH_SIZE);

    RatchetConfiguration config = new RatchetConfiguration();
    config.validateNumericEnvVars();

    assertEquals(
        Integer.valueOf(50),
        config.getPollerBatchSize(),
        "Unparseable env var must fall back to the hard-coded default");

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
    System.setProperty(BAD_KEY_PREFERRED, "25");

    RatchetConfiguration config = new RatchetConfiguration();
    config.validateNumericEnvVars();

    boolean sawWarnForOurKey =
        handler.records.stream()
            .filter(r -> r.getLevel().intValue() >= Level.WARNING.intValue())
            .map(LogRecord::getMessage)
            .anyMatch(msg -> msg != null && msg.contains(BAD_KEY_PREFERRED));
    assertFalse(sawWarnForOurKey, "Valid numeric value must not produce a WARN");

    assertEquals(Integer.valueOf(25), config.getPollerBatchSize());
  }

  @Test
  void missingEnvVarEmitsNoWarn() {
    RatchetConfiguration config = new RatchetConfiguration();
    config.validateNumericEnvVars();

    boolean sawAnyWarn =
        handler.records.stream().anyMatch(r -> r.getLevel().intValue() >= Level.WARNING.intValue());
    assertFalse(sawAnyWarn, "Unset env vars must not produce WARN");
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
