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

import java.sql.SQLException;

/** Retries test cleanup transactions that collide with background scheduler maintenance. */
public final class SqlCleanupRetry {
  private SqlCleanupRetry() {}

  /** The action must start a fresh transaction on every invocation. */
  public static void run(Runnable cleanupTransaction) {
    for (int attempt = 1; ; attempt++) {
      try {
        cleanupTransaction.run();
        return;
      } catch (RuntimeException failure) {
        if (attempt == 5 || !isSerializationFailure(failure)) {
          throw failure;
        }
        try {
          Thread.sleep(50L * attempt);
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException("Interrupted while retrying test cleanup", failure);
        }
      }
    }
  }

  private static boolean isSerializationFailure(Throwable failure) {
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      if (cause instanceof SQLException sql && "40001".equals(sql.getSQLState())) {
        return true;
      }
    }
    return false;
  }
}
