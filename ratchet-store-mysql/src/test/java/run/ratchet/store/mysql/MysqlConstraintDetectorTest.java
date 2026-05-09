package run.ratchet.store.mysql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.mysql.cj.exceptions.CommunicationsException;
import java.sql.SQLException;
import org.junit.jupiter.api.Test;

class MysqlConstraintDetectorTest {

  private final MysqlConstraintDetector detector = new MysqlConstraintDetector();

  @Test
  void extractsConstraintNameThroughWrappers() {
    Exception wrapped =
        new RuntimeException(
            "hibernate",
            new SQLException(
                "Duplicate entry 'abc' for key 'scheduler_business_key_reservation.PRIMARY'"));

    assertEquals("scheduler_business_key_reservation.PRIMARY", detector.constraintName(wrapped));
  }

  @Test
  void returnsNullWhenConstraintNameIsAbsent() {
    Exception wrapped =
        new RuntimeException("hibernate", new SQLException("Duplicate entry 'abc'"));

    assertNull(detector.constraintName(wrapped));
  }

  @Test
  void detectsDuplicateKeyThroughWrappers() {
    Exception wrapped =
        new RuntimeException(
            "hibernate", new SQLException("Duplicate entry 'abc' for key 'uq_job_key'"));

    assertTrue(detector.isDuplicateKey(wrapped));
  }

  @Test
  void ignoresNonDuplicateSqlExceptions() {
    Exception wrapped =
        new RuntimeException(
            "hibernate", new SQLException("Cannot delete or update a parent row", "23000"));

    assertFalse(detector.isDuplicateKey(wrapped));
  }

  @Test
  void detectsDuplicateBusinessKeyFromReservationConstraint() {
    Exception wrapped =
        new RuntimeException(
            "hibernate",
            new SQLException(
                "Duplicate entry 'order-1' for key 'scheduler_business_key_reservation.PRIMARY'"));

    assertTrue(detector.isDuplicateBusinessKey(wrapped));
  }

  @Test
  void ignoresDuplicateKeyFromOtherConstraintAsBusinessKey() {
    Exception wrapped =
        new RuntimeException(
            "hibernate", new SQLException("Duplicate entry 'abc' for key 'uq_job_key'"));

    assertFalse(detector.isDuplicateBusinessKey(wrapped));
  }

  @Test
  void detectsDeadlockByErrorCodeThroughWrappers() {
    Exception wrapped =
        new RuntimeException(
            "hibernate", new SQLException("transaction rolled back", "40001", 1213));

    assertTrue(detector.isDeadlock(wrapped));
  }

  @Test
  void detectsLockWaitTimeoutByMessageThroughWrappers() {
    Exception wrapped =
        new RuntimeException(
            "hibernate",
            new SQLException("Lock wait timeout exceeded; try restarting transaction"));

    assertTrue(detector.isDeadlock(wrapped));
  }

  @Test
  void ignoresNonDeadlockSqlExceptions() {
    Exception wrapped = new RuntimeException("hibernate", new SQLException("syntax", "42000"));

    assertFalse(detector.isDeadlock(wrapped));
  }

  @Test
  void detectsSqlState08PrefixThroughWrappers() {
    assertTrue(detector.isTransientConnectionFailure(wrappedSqlState("08S01")));
    assertTrue(detector.isTransientConnectionFailure(wrappedSqlState("08P01")));
    assertTrue(detector.isTransientConnectionFailure(wrappedSqlState("08999")));
  }

  @Test
  void ignoresSqlStatesThatDoNotStartWith08() {
    assertFalse(detector.isTransientConnectionFailure(wrappedSqlState("00801")));
  }

  @Test
  void detectsMySqlConnectionErrorCodeThroughWrappers() {
    Exception wrapped =
        new RuntimeException("hibernate", new SQLException("server gone away", "HY000", 2006));

    assertTrue(detector.isTransientConnectionFailure(wrapped));
  }

  @Test
  void detectsCommunicationsExceptionByClassNameThroughWrappers() {
    Exception wrapped = new RuntimeException("hibernate", new CommunicationsException("reset"));

    assertTrue(detector.isTransientConnectionFailure(wrapped));
  }

  @Test
  void ignoresNonTransientSqlExceptions() {
    Exception wrapped = new RuntimeException("hibernate", new SQLException("syntax", "42000"));

    assertFalse(detector.isTransientConnectionFailure(wrapped));
  }

  private static Exception wrappedSqlState(String sqlState) {
    return new RuntimeException("jpa", new SQLException("connection lost", sqlState));
  }
}
