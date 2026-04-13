package run.ratchet.store.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceException;
import jakarta.persistence.Query;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class IsolationCheckTest {

  private EntityManager em;
  private Query query8;
  private Query query57;

  @BeforeEach
  void setUp() {
    em = mock(EntityManager.class);
    query8 = mock(Query.class);
    query57 = mock(Query.class);
    when(em.createNativeQuery("SELECT @@SESSION.transaction_isolation")).thenReturn(query8);
    when(em.createNativeQuery("SELECT @@SESSION.tx_isolation")).thenReturn(query57);
    when(em.createNativeQuery("SHOW transaction_isolation")).thenReturn(query8);
    System.clearProperty(IsolationCheck.SYSTEM_PROPERTY);
  }

  @AfterEach
  void tearDown() {
    System.clearProperty(IsolationCheck.SYSTEM_PROPERTY);
  }

  @Test
  void currentModeDefaultsToWarn() {
    assertEquals(IsolationCheck.Mode.WARN, IsolationCheck.currentMode());
  }

  @Test
  void currentModeRespectsExplicitFail() {
    System.setProperty(IsolationCheck.SYSTEM_PROPERTY, "fail");
    assertEquals(IsolationCheck.Mode.FAIL, IsolationCheck.currentMode());
  }

  @Test
  void currentModeRespectsDisableSynonyms() {
    for (String value : List.of("disable", "off", "false", "DISABLE", " disable ")) {
      System.setProperty(IsolationCheck.SYSTEM_PROPERTY, value);
      assertEquals(IsolationCheck.Mode.DISABLE, IsolationCheck.currentMode(), "value=" + value);
    }
  }

  @Test
  void currentModeFallsBackToWarnForUnknownValue() {
    System.setProperty(IsolationCheck.SYSTEM_PROPERTY, "yelling");
    assertEquals(IsolationCheck.Mode.WARN, IsolationCheck.currentMode());
  }

  @Test
  void verifyPassesOnExactMatch() {
    when(query8.getSingleResult()).thenReturn("READ-COMMITTED");
    IsolationCheck.verifyReadCommitted(
        em, "MySQL", List.of("SELECT @@SESSION.transaction_isolation"), "READ-COMMITTED", "fix");
    // No exception, no fall-through.
    verify(em).createNativeQuery("SELECT @@SESSION.transaction_isolation");
    verifyNoMoreInteractions(em);
  }

  @Test
  void verifyPassesOnNormalizedMatchAcrossPunctuation() {
    when(query8.getSingleResult()).thenReturn("read committed");
    IsolationCheck.verifyReadCommitted(
        em, "PostgreSQL", List.of("SHOW transaction_isolation"), "READ-COMMITTED", "fix");
  }

  @Test
  void verifyFallsBackToSecondQueryWhenFirstThrows() {
    when(query8.getSingleResult())
        .thenThrow(new PersistenceException("Unknown system variable 'transaction_isolation'"));
    when(query57.getSingleResult()).thenReturn("READ-COMMITTED");

    IsolationCheck.verifyReadCommitted(
        em,
        "MySQL",
        List.of("SELECT @@SESSION.transaction_isolation", "SELECT @@SESSION.tx_isolation"),
        "READ-COMMITTED",
        "fix");

    verify(em).createNativeQuery("SELECT @@SESSION.transaction_isolation");
    verify(em).createNativeQuery("SELECT @@SESSION.tx_isolation");
  }

  @Test
  void verifySkipsWhenAllQueriesThrow() {
    when(query8.getSingleResult()).thenThrow(new PersistenceException("boom"));
    when(query57.getSingleResult()).thenThrow(new PersistenceException("boom"));

    // Should not throw — detection failure is permissive.
    IsolationCheck.verifyReadCommitted(
        em,
        "MySQL",
        List.of("SELECT @@SESSION.transaction_isolation", "SELECT @@SESSION.tx_isolation"),
        "READ-COMMITTED",
        "fix");
  }

  @Test
  void verifyWarnsOnMismatchInWarnMode() {
    when(query8.getSingleResult()).thenReturn("REPEATABLE-READ");
    // No exception expected — WARN mode logs and continues.
    IsolationCheck.verifyReadCommitted(
        em, "MySQL", List.of("SELECT @@SESSION.transaction_isolation"), "READ-COMMITTED", "fix");
  }

  @Test
  void verifyThrowsOnMismatchInFailMode() {
    System.setProperty(IsolationCheck.SYSTEM_PROPERTY, "fail");
    when(query8.getSingleResult()).thenReturn("SERIALIZABLE");

    IsolationCheckFailedException ex =
        assertThrows(
            IsolationCheckFailedException.class,
            () ->
                IsolationCheck.verifyReadCommitted(
                    em,
                    "PostgreSQL",
                    List.of("SHOW transaction_isolation"),
                    "READ-COMMITTED",
                    "use read committed"));

    String msg = ex.getMessage();
    org.junit.jupiter.api.Assertions.assertTrue(msg.contains("PostgreSQL"));
    org.junit.jupiter.api.Assertions.assertTrue(msg.contains("SERIALIZABLE"));
    org.junit.jupiter.api.Assertions.assertTrue(msg.contains("use read committed"));
  }

  @Test
  void verifySkipsCompletelyWhenDisabled() {
    System.setProperty(IsolationCheck.SYSTEM_PROPERTY, "disable");
    // Even if the underlying connection is in SERIALIZABLE, disable should short-circuit.
    IsolationCheck.verifyReadCommitted(
        em, "MySQL", List.of("SELECT @@SESSION.transaction_isolation"), "READ-COMMITTED", "fix");
    // Critical: disable means the query is NEVER issued. No interaction with the EM.
    verify(em, org.mockito.Mockito.never()).createNativeQuery(anyString());
  }

  @Test
  void verifySkipsCompletelyWhenDisabledEvenInFailMode() {
    // disable wins over fail.
    System.setProperty(IsolationCheck.SYSTEM_PROPERTY, "disable");
    when(query8.getSingleResult()).thenReturn("SERIALIZABLE");
    // Should not throw.
    IsolationCheck.verifyReadCommitted(
        em, "MySQL", List.of("SELECT @@SESSION.transaction_isolation"), "READ-COMMITTED", "fix");
  }

  @Test
  void verifySkipsWhenQueryReturnsNull() {
    when(query8.getSingleResult()).thenReturn(null);
    // Treat as detection failure → no throw, no warn.
    IsolationCheck.verifyReadCommitted(
        em, "MySQL", List.of("SELECT @@SESSION.transaction_isolation"), "READ-COMMITTED", "fix");
  }

  @Test
  void verifyHandlesNonStringResults() {
    // Some drivers return Boolean/Integer/etc. — toString() must work.
    when(query8.getSingleResult())
        .thenReturn(
            new Object() {
              @Override
              public String toString() {
                return "READ-COMMITTED";
              }
            });
    IsolationCheck.verifyReadCommitted(
        em, "MySQL", List.of("SELECT @@SESSION.transaction_isolation"), "READ-COMMITTED", "fix");
  }
}
