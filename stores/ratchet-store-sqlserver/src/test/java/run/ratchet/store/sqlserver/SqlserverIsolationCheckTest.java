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
package run.ratchet.store.sqlserver;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceException;
import jakarta.persistence.Query;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.RatchetOptions;
import run.ratchet.api.RatchetOptions.IsolationCheckMode;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.util.IsolationCheck;
import run.ratchet.store.util.IsolationCheckFailedException;

class SqlserverIsolationCheckTest {

  private static final String RCSI_PROBE_SQL =
      """
      SELECT CASE is_read_committed_snapshot_on WHEN 1 THEN 'on' ELSE 'off' END
      FROM sys.databases WHERE name = DB_NAME()
      """;

  private EntityManager em;
  private Query isolation;
  private Query rcsi;

  @BeforeEach
  void setUp() {
    em = mock(EntityManager.class);
    isolation = mock(Query.class);
    rcsi = mock(Query.class);
    when(em.createNativeQuery(SqlserverJobStoreImpl.ISOLATION_PROBE_SQL)).thenReturn(isolation);
    when(em.createNativeQuery(RCSI_PROBE_SQL)).thenReturn(rcsi);
    when(isolation.getSingleResult()).thenReturn("read committed");
    when(rcsi.getSingleResult()).thenReturn("off");
  }

  @Test
  void rcsiOffFailsWithRemediation() {
    IsolationCheckFailedException failure =
        assertThrows(
            IsolationCheckFailedException.class,
            () -> store(IsolationCheckMode.FAIL).checkIsolationLevel());

    assertRcsiRemediation(failure.getMessage());
  }

  @Test
  void rcsiOffWarnsAndContinues() {
    try (LogCapture logs = new LogCapture()) {
      assertDoesNotThrow(() -> store(IsolationCheckMode.WARN).checkIsolationLevel());

      List<LogRecord> warnings =
          logs.records.stream().filter(r -> r.getLevel().equals(Level.WARNING)).toList();
      assertEquals(1, warnings.size());
      assertRcsiRemediation(warnings.get(0).getMessage());
    }
  }

  @Test
  void disableSkipsBothProbes() {
    store(IsolationCheckMode.DISABLE).checkIsolationLevel();

    verify(em, never()).createNativeQuery(anyString());
  }

  @Test
  void throwingRcsiProbeLogsDetectionFailureAndContinues() {
    when(rcsi.getSingleResult()).thenThrow(new PersistenceException("probe unavailable"));
    try (LogCapture logs = new LogCapture()) {
      assertDoesNotThrow(() -> store(IsolationCheckMode.FAIL).checkIsolationLevel());

      assertTrue(
          logs.records.stream()
              .anyMatch(
                  r ->
                      r.getLevel().equals(Level.FINE)
                          && r.getMessage()
                              .contains("Could not check SQL Server READ_COMMITTED_SNAPSHOT")));
      assertTrue(logs.records.stream().noneMatch(r -> r.getLevel().equals(Level.WARNING)));
    }
    verify(rcsi).getSingleResult();
  }

  @Test
  void rcsiOnPassesAfterIsolationProbe() {
    when(rcsi.getSingleResult()).thenReturn("on");

    assertDoesNotThrow(() -> store(IsolationCheckMode.FAIL).checkIsolationLevel());

    var probes = inOrder(em, isolation, rcsi);
    probes.verify(em).createNativeQuery(SqlserverJobStoreImpl.ISOLATION_PROBE_SQL);
    probes.verify(isolation).getSingleResult();
    probes.verify(em).createNativeQuery(RCSI_PROBE_SQL);
    probes.verify(rcsi).getSingleResult();
  }

  @Test
  void unsupportedIsolationFailsBeforeRcsiProbe() {
    when(isolation.getSingleResult()).thenReturn("serializable");

    assertThrows(
        IsolationCheckFailedException.class,
        () -> store(IsolationCheckMode.FAIL).checkIsolationLevel());

    verify(em, never()).createNativeQuery(RCSI_PROBE_SQL);
  }

  @Test
  void factoryRejectsRcsiOff() {
    IsolationCheckFailedException failure =
        assertThrows(
            IsolationCheckFailedException.class,
            () ->
                SqlserverJobStoreFactory.create(
                    em, RatchetOptions.defaults(), mock(MetricsCollector.class)));

    assertRcsiRemediation(failure.getMessage());
  }

  @Test
  void factoryAcceptsRcsiOn() {
    when(rcsi.getSingleResult()).thenReturn("on");

    assertDoesNotThrow(
        () ->
            SqlserverJobStoreFactory.create(
                em, RatchetOptions.defaults(), mock(MetricsCollector.class)));

    verify(rcsi).getSingleResult();
  }

  private SqlserverJobStoreImpl store(IsolationCheckMode mode) {
    return new SqlserverJobStoreImpl(
        () -> em,
        mock(MetricsCollector.class),
        RatchetOptions.builder().store(s -> s.isolationCheckMode(mode)).build());
  }

  private static void assertRcsiRemediation(String message) {
    assertTrue(message.contains("SQL Server READ_COMMITTED_SNAPSHOT is 'off'"));
    assertTrue(message.contains("ALTER DATABASE [<db>] SET READ_COMMITTED_SNAPSHOT ON"));
    assertTrue(message.contains("WITH ROLLBACK IMMEDIATE"));
    assertTrue(message.contains("exclusive access"));
    assertTrue(message.contains("Row-versioning is required"));
  }

  private static final class LogCapture implements AutoCloseable {
    private final Logger logger = Logger.getLogger(IsolationCheck.class.getName());
    private final Level originalLevel = logger.getLevel();
    private final boolean originalUseParentHandlers = logger.getUseParentHandlers();
    private final List<LogRecord> records = new ArrayList<>();
    private final Handler handler =
        new Handler() {
          @Override
          public void publish(LogRecord record) {
            records.add(record);
          }

          @Override
          public void flush() {}

          @Override
          public void close() {}
        };

    private LogCapture() {
      logger.setLevel(Level.ALL);
      logger.setUseParentHandlers(false);
      logger.addHandler(handler);
    }

    @Override
    public void close() {
      logger.removeHandler(handler);
      logger.setLevel(originalLevel);
      logger.setUseParentHandlers(originalUseParentHandlers);
    }
  }
}
