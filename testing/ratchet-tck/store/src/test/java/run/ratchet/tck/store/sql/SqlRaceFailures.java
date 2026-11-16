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
package run.ratchet.tck.store.sql;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientConnectionException;
import java.util.Locale;

/** Shared failure classification for SQL contention tests. */
final class SqlRaceFailures {
  private SqlRaceFailures() {}

  static boolean isLockFailure(Throwable failure) {
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      if (cause instanceof SQLException sql) {
        if (sql.getErrorCode() == 1213
            || sql.getErrorCode() == 1205
            || sql.getErrorCode() == 1222
            || "40P01".equals(sql.getSQLState())) return true;
      }
      String message = cause.getMessage();
      if (message != null
          && (message.contains("ORA-00060")
              || message.toLowerCase(Locale.ROOT).contains("lock wait timeout"))) return true;
    }
    return false;
  }

  /** The innermost message, which for a deadlock carries the database's own detail. */
  static String rootMessage(Throwable failure) {
    Throwable root = failure;
    while (root.getCause() != null) root = root.getCause();
    return String.valueOf(root.getMessage()).replace('\n', ' ');
  }

  /**
   * A failure to reach the database: a JDBC connection exception, SQLState class 08, or an I/O
   * error under the driver (Oracle's listener refusals, such as ORA-12516, are I/O errors).
   */
  static boolean isConnectionFailure(Throwable failure) {
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      if (cause instanceof SQLTransientConnectionException
          || cause instanceof SQLNonTransientConnectionException
          || cause instanceof SQLRecoverableException
          || cause instanceof IOException) return true;
      if (cause instanceof SQLException sql) {
        String state = sql.getSQLState();
        if (state != null && state.startsWith("08")) return true;
      }
    }
    return false;
  }
}
