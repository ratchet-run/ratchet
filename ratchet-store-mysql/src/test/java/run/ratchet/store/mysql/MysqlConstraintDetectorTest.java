package run.ratchet.store.mysql;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.mysql.cj.exceptions.CommunicationsException;
import java.sql.SQLException;
import org.junit.jupiter.api.Test;

class MysqlConstraintDetectorTest {

  private final MysqlConstraintDetector detector = new MysqlConstraintDetector();

  @Test
  void detectsSqlState08ThroughWrappers() {
    Exception wrapped = new RuntimeException("jpa", new SQLException("connection lost", "08S01"));

    assertTrue(detector.isTransientConnectionFailure(wrapped));
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
}
