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
package run.ratchet.store.oracle;

import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientException;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import run.ratchet.store.ConstraintDetector;

/** Oracle-specific constraint-violation and transient-fault detector. */
class OracleConstraintDetector implements ConstraintDetector {

  // ORA-00001: unique constraint (SCHEMA.CONSTRAINT_NAME) violated
  private static final Pattern CONSTRAINT_NAME_PATTERN =
      Pattern.compile("unique constraint \\(([^)]+)\\)");

  private static final int UNIQUE_VIOLATION_ERROR_CODE = 1; // ORA-00001
  private static final int DEADLOCK_ERROR_CODE = 60; // ORA-00060
  private static final int CANNOT_SERIALIZE_ERROR_CODE = 8177; // ORA-08177

  // Connection / communication failures that are safe to retry on a fresh connection.
  private static final Set<Integer> TRANSIENT_ERROR_CODES =
      Set.of(
          28, // ORA-00028 session killed
          1033, // ORA-01033 init/shutdown in progress
          1089, // ORA-01089 immediate shutdown in progress
          3113, // ORA-03113 end-of-file on communication channel
          3114, // ORA-03114 not connected to ORACLE
          12170, // ORA-12170 TNS connect timeout
          12514, // ORA-12514 listener does not currently know of service
          12521, // ORA-12521 listener does not currently know of instance
          12537, // ORA-12537 TNS connection closed
          12541, // ORA-12541 no listener
          12570, // ORA-12570 TNS packet reader failure
          17002); // ojdbc: network adapter could not establish the connection

  @Override
  public String constraintName(Exception e) {
    Throwable current = e;
    while (current != null) {
      String msg = current.getMessage();
      if (msg != null) {
        Matcher matcher = CONSTRAINT_NAME_PATTERN.matcher(msg);
        if (matcher.find()) {
          String name = matcher.group(1);
          // Strip the SCHEMA. prefix Oracle prepends and lower-case the name: Oracle reports
          // unquoted identifiers upper-case, but the shared isDuplicate* checks match lower-case.
          int dot = name.lastIndexOf('.');
          if (dot >= 0) {
            name = name.substring(dot + 1);
          }
          return name.toLowerCase();
        }
      }
      current = current.getCause();
    }
    return null;
  }

  @Override
  public boolean isDuplicateKey(Exception e) {
    Throwable current = e;
    while (current != null) {
      if (current instanceof SQLException sql
          && sql.getErrorCode() == UNIQUE_VIOLATION_ERROR_CODE) {
        return true;
      }
      String msg = current.getMessage();
      if (msg != null && msg.contains("ORA-00001")) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }

  @Override
  public boolean isDeadlock(Exception e) {
    Throwable current = e;
    while (current != null) {
      if (current instanceof SQLException sql
          && (sql.getErrorCode() == DEADLOCK_ERROR_CODE
              || sql.getErrorCode() == CANNOT_SERIALIZE_ERROR_CODE)) {
        return true;
      }
      String msg = current.getMessage();
      if (msg != null && (msg.contains("ORA-00060") || msg.contains("ORA-08177"))) {
        return true;
      }
      current = current.getCause();
    }
    return false;
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
            || TRANSIENT_ERROR_CODES.contains(sql.getErrorCode())) {
          return true;
        }
      }
      current = current.getCause();
    }
    return false;
  }
}
