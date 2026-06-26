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

import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientException;
import run.ratchet.store.ConstraintDetector;

/** SQL Server-specific constraint violation detector. */
class SqlserverConstraintDetector implements ConstraintDetector {

  // SQL Server vendor error codes (mssql-jdbc surfaces these via SQLException.getErrorCode()).
  private static final int ERROR_UNIQUE_CONSTRAINT = 2627; // PRIMARY KEY / UNIQUE constraint
  private static final int ERROR_UNIQUE_INDEX = 2601; // duplicate key in a unique index
  private static final int ERROR_DEADLOCK = 1205;
  private static final int ERROR_SNAPSHOT_CONFLICT = 3960; // snapshot isolation update conflict

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
    // SQL Server names the constraint in single quotes:
    // "Violation of UNIQUE KEY constraint 'uk_idempotency_key'. Cannot insert duplicate key ..."
    String message = sql.getMessage();
    if (message == null) {
      return null;
    }
    int marker = message.indexOf("constraint '");
    // Opening quote: the one in "constraint '" when present, else the first quote in the message.
    int start = marker >= 0 ? message.indexOf('\'', marker) : message.indexOf('\'');
    if (start >= 0) {
      int end = message.indexOf('\'', start + 1);
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
    if (sql.getErrorCode() == ERROR_UNIQUE_CONSTRAINT || sql.getErrorCode() == ERROR_UNIQUE_INDEX) {
      return true;
    }
    String message = sql.getMessage();
    return message != null
        && (message.contains("Violation of UNIQUE KEY constraint")
            || message.contains("Violation of PRIMARY KEY constraint")
            || message.contains("Cannot insert duplicate key"));
  }

  @Override
  public boolean isDeadlock(Exception e) {
    SQLException sql = findSqlException(e);
    if (sql == null) {
      return false;
    }
    return sql.getErrorCode() == ERROR_DEADLOCK
        || sql.getErrorCode() == ERROR_SNAPSHOT_CONFLICT
        || "40001".equals(sql.getSQLState());
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
        if (sqlState != null && sqlState.startsWith("08")) {
          return true;
        }
      }
      current = current.getCause();
    }
    return false;
  }
}
