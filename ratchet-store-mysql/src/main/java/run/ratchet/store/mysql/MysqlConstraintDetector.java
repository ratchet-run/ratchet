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
package run.ratchet.store.mysql;

import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientException;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import run.ratchet.store.ConstraintDetector;

/** MySQL-specific constraint violation detector. */
class MysqlConstraintDetector implements ConstraintDetector {

  private static final Pattern CONSTRAINT_NAME_PATTERN = Pattern.compile("for key '([^']+)'");
  private static final int DEADLOCK_ERROR_CODE = 1213;
  private static final int LOCK_WAIT_TIMEOUT_ERROR_CODE = 1205;
  private static final int CONNECTION_REFUSED_ERROR_CODE = 2002;
  private static final int CONNECTION_FAILED_ERROR_CODE = 2003;
  private static final int SERVER_GONE_AWAY_ERROR_CODE = 2006;
  private static final int LOST_CONNECTION_ERROR_CODE = 2013;
  private static final Set<String> COMMUNICATIONS_EXCEPTIONS =
      Set.of(
          "com.mysql.cj.exceptions.CommunicationsException",
          "com.mysql.cj.exceptions.CJCommunicationsException",
          "com.mysql.cj.jdbc.exceptions.CommunicationsException");

  @Override
  public String constraintName(Exception e) {
    Throwable current = e;
    while (current != null) {
      String msg = current.getMessage();
      if (msg != null) {
        Matcher matcher = CONSTRAINT_NAME_PATTERN.matcher(msg);
        if (matcher.find()) {
          return matcher.group(1);
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
      String msg = current.getMessage();
      if (msg != null && msg.contains("Duplicate entry")) {
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
              || sql.getErrorCode() == LOCK_WAIT_TIMEOUT_ERROR_CODE)) {
        return true;
      }
      String msg = current.getMessage();
      if (msg != null
          && (msg.contains("Deadlock found") || msg.contains("Lock wait timeout exceeded"))) {
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
      if (COMMUNICATIONS_EXCEPTIONS.contains(current.getClass().getName())) {
        return true;
      }
      if (current instanceof SQLException sql) {
        String sqlState = sql.getSQLState();
        if ((sqlState != null && sqlState.startsWith("08"))
            || sql.getErrorCode() == CONNECTION_REFUSED_ERROR_CODE
            || sql.getErrorCode() == CONNECTION_FAILED_ERROR_CODE
            || sql.getErrorCode() == SERVER_GONE_AWAY_ERROR_CODE
            || sql.getErrorCode() == LOST_CONNECTION_ERROR_CODE) {
          return true;
        }
      }
      current = current.getCause();
    }
    return false;
  }
}
