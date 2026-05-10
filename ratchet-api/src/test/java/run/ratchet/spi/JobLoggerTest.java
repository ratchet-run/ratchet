package run.ratchet.spi;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class JobLoggerTest {

  @Test
  void disabledFormatVariantDoesNotFormatArguments() {
    DisabledDebugLogger logger = new DisabledDebugLogger();

    assertDoesNotThrow(() -> logger.debug("expensive {}", new ThrowingToString()));
    assertEquals(0, logger.messages);
  }

  @Test
  void enabledFormatVariantFormatsAndLogsMessage() {
    CapturingLogger logger = new CapturingLogger();

    logger.info("job {} attempt {}", "alpha", 2);

    assertEquals("job alpha attempt 2", logger.message);
  }

  private static final class DisabledDebugLogger extends CapturingLogger {
    private int messages;

    @Override
    public void debug(String message) {
      messages++;
    }

    @Override
    public boolean isDebugEnabled() {
      return false;
    }
  }

  private static class CapturingLogger implements JobLogger {
    private String message;

    @Override
    public void info(String message) {
      this.message = message;
    }

    @Override
    public void debug(String message) {
      this.message = message;
    }

    @Override
    public void warn(String message) {
      this.message = message;
    }

    @Override
    public void error(String message) {
      this.message = message;
    }

    @Override
    public void trace(String message) {
      this.message = message;
    }
  }

  private static final class ThrowingToString {
    @Override
    public String toString() {
      throw new AssertionError("argument should not be formatted");
    }
  }
}
