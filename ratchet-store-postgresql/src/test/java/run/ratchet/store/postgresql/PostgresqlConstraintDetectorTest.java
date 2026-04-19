package run.ratchet.store.postgresql;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import org.junit.jupiter.api.Test;

class PostgresqlConstraintDetectorTest {

  private final PostgresqlConstraintDetector detector = new PostgresqlConstraintDetector();

  @Test
  void detectsConnectionStateThroughWrappers() {
    Exception wrapped = new RuntimeException("jpa", new SQLException("connection lost", "08006"));

    assertTrue(detector.isTransientConnectionFailure(wrapped));
  }

  @Test
  void detectsAdminShutdownThroughWrappers() {
    Exception wrapped = new RuntimeException("jpa", new SQLException("shutdown", "57P01"));

    assertTrue(detector.isTransientConnectionFailure(wrapped));
  }

  @Test
  void detectsRecoverableSqlExceptions() {
    Exception wrapped = new RuntimeException("jpa", new SQLRecoverableException("recoverable"));

    assertTrue(detector.isTransientConnectionFailure(wrapped));
  }

  @Test
  void ignoresNonTransientSqlExceptions() {
    Exception wrapped = new RuntimeException("jpa", new SQLException("unique", "23505"));

    assertFalse(detector.isTransientConnectionFailure(wrapped));
  }
}
