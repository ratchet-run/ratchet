package run.ratchet.store.postgresql;

import run.ratchet.store.ConstraintDetector;
import java.sql.SQLException;

/** PostgreSQL-specific constraint violation detector. */
public class PostgresqlConstraintDetector implements ConstraintDetector {

  private static final String SQLSTATE_UNIQUE_VIOLATION = "23505";
  private static final String SQLSTATE_DEADLOCK = "40P01";

  private static SQLException findSqlException(Throwable t) {
    while (t != null) {
      if (t instanceof SQLException sql) {
        return sql;
      }
      t = t.getCause();
    }
    return null;
  }

  @Override
  public String constraintName(Exception e) {
    SQLException sql = findSqlException(e);
    if (sql == null) {
      return null;
    }
    // PostgreSQL includes constraint name in the error message:
    // "duplicate key value violates unique constraint \"constraint_name\""
    String message = sql.getMessage();
    if (message == null) {
      return null;
    }
    int start = message.indexOf('"');
    if (start >= 0) {
      int end = message.indexOf('"', start + 1);
      if (end > start) {
        return message.substring(start + 1, end);
      }
    }
    return null;
  }

  @Override
  public boolean isDuplicateKey(Exception e) {
    SQLException sql = findSqlException(e);
    if (sql == null) {
      return false;
    }
    String sqlState = sql.getSQLState();
    if (SQLSTATE_UNIQUE_VIOLATION.equals(sqlState)) {
      return true;
    }
    String message = sql.getMessage();
    return message != null && message.contains("duplicate key value violates unique constraint");
  }

  @Override
  public boolean isDeadlock(Exception e) {
    SQLException sql = findSqlException(e);
    if (sql == null) {
      return false;
    }
    return SQLSTATE_DEADLOCK.equals(sql.getSQLState());
  }
}
