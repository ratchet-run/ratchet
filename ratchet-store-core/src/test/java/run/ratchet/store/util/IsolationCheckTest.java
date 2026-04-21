package run.ratchet.store.util;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import run.ratchet.api.RatchetOptions;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceException;
import jakarta.persistence.Query;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

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
    IsolationCheck.verifyReadCommitted(
        em,
        "MySQL",
        List.of("SELECT @@SESSION.transaction_isolation"),
        "READ-COMMITTED",
        "fix",
        RatchetOptions.IsolationCheckMode.WARN);
  }

  @Test
  void verifyThrowsOnMismatchInFailMode() {
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
    Assertions.assertTrue(msg.contains("PostgreSQL"));
    Assertions.assertTrue(msg.contains("SERIALIZABLE"));
    Assertions.assertTrue(msg.contains("use read committed"));
  }

  @Test
  void verifySkipsCompletelyWhenDisabled() {
    IsolationCheck.verifyReadCommitted(
        em,
        "MySQL",
        List.of("SELECT @@SESSION.transaction_isolation"),
        "READ-COMMITTED",
        "fix",
        RatchetOptions.IsolationCheckMode.DISABLE);
    verify(em, Mockito.never()).createNativeQuery(anyString());
  }

  @Test
  void verifySkipsCompletelyWhenDisabledEvenInFailMode() {
    when(query8.getSingleResult()).thenReturn("SERIALIZABLE");
    IsolationCheck.verifyReadCommitted(
        em,
        "MySQL",
        List.of("SELECT @@SESSION.transaction_isolation"),
        "READ-COMMITTED",
        "fix",
        RatchetOptions.IsolationCheckMode.DISABLE);
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
