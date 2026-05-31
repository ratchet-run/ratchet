/*
 * Copyright 2026 Ratchet Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
