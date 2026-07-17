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
package run.ratchet.ri.cdi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import jakarta.enterprise.inject.AmbiguousResolutionException;
import jakarta.enterprise.inject.spi.Bean;
import jakarta.enterprise.inject.spi.BeanManager;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import run.ratchet.ri.security.CallerPrincipalProvider;
import run.ratchet.spi.PayloadEncryption;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class SpiBindingLoggerTest {

  @Mock private BeanManager beanManager;

  @Test
  void nullBeanManager_noArgConstructor_neverLogs() {
    SpiBindingLogger logger = new SpiBindingLogger();

    CapturingHandler handler = attachHandler();
    try {
      logger.logBindings(new Object());
    } finally {
      detachHandler(handler);
    }

    assertTrue(handler.records.isEmpty());
  }

  @Test
  void unsatisfiedSeams_areLoggedAsNone() {
    SpiBindingLogger logger = new SpiBindingLogger(beanManager);

    CapturingHandler handler = attachHandler();
    try {
      logger.logBindings(new Object());
    } finally {
      detachHandler(handler);
    }

    String line = handler.onlyMessage();
    assertTrue(line.startsWith("Ratchet SPI bindings: "), line);
    assertTrue(line.contains("CallerPrincipalProvider=<none>"), line);
    assertTrue(line.contains("PayloadEncryption=<none>"), line);
  }

  @Test
  void resolvedSeam_logsWinningImplementationClassName() {
    Bean<?> winner = mockBean(CallerPrincipalProvider.class);
    Set<Bean<?>> beans = Set.of(winner);
    doReturn(beans).when(beanManager).getBeans(CallerPrincipalProvider.class);
    doReturn(winner).when(beanManager).resolve(beans);

    SpiBindingLogger logger = new SpiBindingLogger(beanManager);
    CapturingHandler handler = attachHandler();
    try {
      logger.logBindings(new Object());
    } finally {
      detachHandler(handler);
    }

    assertTrue(
        handler
            .onlyMessage()
            .contains("CallerPrincipalProvider=run.ratchet.ri.security.CallerPrincipalProvider"));
  }

  @Test
  void ambiguousSeam_logsAmbiguousMarker() {
    Set<Bean<?>> beans = Set.of(mockBean(CallerPrincipalProvider.class), mockBean(Object.class));
    doReturn(beans).when(beanManager).getBeans(CallerPrincipalProvider.class);
    doThrow(new AmbiguousResolutionException("two beans")).when(beanManager).resolve(beans);

    SpiBindingLogger logger = new SpiBindingLogger(beanManager);
    CapturingHandler handler = attachHandler();
    try {
      logger.logBindings(new Object());
    } finally {
      detachHandler(handler);
    }

    assertTrue(handler.onlyMessage().contains("CallerPrincipalProvider=<ambiguous>"));
  }

  @Test
  void resolutionFailure_isCaught_andRecordedAsError() {
    doThrow(new IllegalStateException("boom"))
        .when(beanManager)
        .getBeans(CallerPrincipalProvider.class);

    SpiBindingLogger logger = new SpiBindingLogger(beanManager);
    CapturingHandler handler = attachHandler();
    try {
      logger.logBindings(new Object());
    } finally {
      detachHandler(handler);
    }

    assertTrue(handler.onlyMessage().contains("CallerPrincipalProvider=<error>"));
  }

  @Test
  void payloadEncryption_listsEveryInstalledEngine_sortedByClassName() {
    Set<Bean<?>> beans = Set.of(mockBean(EngineB.class), mockBean(EngineA.class));
    doReturn(beans).when(beanManager).getBeans(PayloadEncryption.class);

    SpiBindingLogger logger = new SpiBindingLogger(beanManager);
    CapturingHandler handler = attachHandler();
    try {
      logger.logBindings(new Object());
    } finally {
      detachHandler(handler);
    }

    assertTrue(
        handler
            .onlyMessage()
            .contains(
                "PayloadEncryption=" + EngineA.class.getName() + "+" + EngineB.class.getName()));
  }

  private static Bean<?> mockBean(Class<?> beanClass) {
    Bean<?> bean = mock(Bean.class);
    doReturn(beanClass).when(bean).getBeanClass();
    return bean;
  }

  private static CapturingHandler attachHandler() {
    Logger logger = Logger.getLogger(SpiBindingLogger.class.getName());
    logger.setLevel(Level.ALL);
    CapturingHandler handler = new CapturingHandler();
    logger.addHandler(handler);
    return handler;
  }

  private static void detachHandler(CapturingHandler handler) {
    Logger.getLogger(SpiBindingLogger.class.getName()).removeHandler(handler);
  }

  /** Collects log records so a test can assert on the single emitted line. */
  private static final class CapturingHandler extends Handler {
    private final CopyOnWriteArrayList<LogRecord> records = new CopyOnWriteArrayList<>();

    @Override
    public void publish(LogRecord record) {
      records.add(record);
    }

    @Override
    public void flush() {}

    @Override
    public void close() {}

    /**
     * Returns the single "Ratchet SPI bindings: ..." summary line, ignoring any WARN records the
     * failure-path tests also trigger on this same logger.
     */
    String onlyMessage() {
      var summaryLines =
          records.stream()
              .map(LogRecord::getMessage)
              .filter(m -> m.startsWith("Ratchet SPI bindings: "))
              .toList();
      assertEquals(1, summaryLines.size());
      return summaryLines.get(0);
    }
  }

  /** Marker type standing in for an installed encryption engine's bean class. */
  private static final class EngineA {}

  /** Marker type standing in for a second installed encryption engine's bean class. */
  private static final class EngineB {}
}
