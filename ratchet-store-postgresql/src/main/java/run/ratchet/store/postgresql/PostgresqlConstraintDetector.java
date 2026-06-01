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
package run.ratchet.store.postgresql;

import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientException;
import run.ratchet.store.ConstraintDetector;

/** PostgreSQL-specific constraint violation detector. */
class PostgresqlConstraintDetector implements ConstraintDetector {

  private static final String SQLSTATE_UNIQUE_VIOLATION = "23505";
  private static final String SQLSTATE_DEADLOCK = "40P01";
  private static final String SQLSTATE_SERIALIZATION_FAILURE = "40001";
  private static final String SQLSTATE_ADMIN_SHUTDOWN = "57P01";
  private static final String SQLSTATE_CRASH_SHUTDOWN = "57P02";
  private static final String SQLSTATE_CANNOT_CONNECT_NOW = "57P03";

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
    return SQLSTATE_DEADLOCK.equals(sql.getSQLState())
        || SQLSTATE_SERIALIZATION_FAILURE.equals(sql.getSQLState());
  }

  @Override
  public boolean isTransientConnectionFailure(Exception e) {
    Throwable current = e;
    while (current != null) {
      if (current instanceof SQLTransientException || current instanceof SQLRecoverableException) {
        return true;
      }
      if (current instanceof SQLException sql) {
        String sqlState = sql.getSQLState();
        if ((sqlState != null && sqlState.startsWith("08"))
            || SQLSTATE_ADMIN_SHUTDOWN.equals(sqlState)
            || SQLSTATE_CRASH_SHUTDOWN.equals(sqlState)
            || SQLSTATE_CANNOT_CONNECT_NOW.equals(sqlState)) {
          return true;
        }
      }
      current = current.getCause();
    }
    return false;
  }
}
