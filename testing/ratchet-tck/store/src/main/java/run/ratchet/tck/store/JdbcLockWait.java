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
package run.ratchet.tck.store;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;

/** Observes database lock waits through a separate autocommit JDBC connection. */
public final class JdbcLockWait {

  private JdbcLockWait() {}

  @FunctionalInterface
  public interface ConnectionFactory {
    Connection open() throws SQLException;
  }

  /** Returns true when the count query finds a blocked session, or fails after the timeout. */
  public static boolean awaitBlockedSession(
      ConnectionFactory connections, String query, Duration timeout) {
    long deadline = System.nanoTime() + timeout.toNanos();
    try (var connection = connections.open()) {
      connection.setAutoCommit(true);
      try (var statement = connection.createStatement()) {
        do {
          try (var result = statement.executeQuery(query)) {
            if (result.next() && result.getLong(1) > 0) {
              return true;
            }
          }
          // MySQL refreshes its INNODB_TRX cache only after 100 ms without a read. A faster poll
          // keeps returning the first snapshot, so this interval must stay above 100 ms.
          Thread.sleep(250);
        } while (System.nanoTime() < deadline);
      }
    } catch (SQLException e) {
      throw new AssertionError("Could not query database lock waits: " + query, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError("Interrupted while waiting for a database lock wait", e);
    }
    throw new AssertionError(
        "No session blocked on a database lock within " + timeout + ": " + query);
  }
}
