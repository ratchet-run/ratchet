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
package run.ratchet.store.util;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.store.ConstraintDetector;

class TransientStoreExceptionsTest {

  @Test
  void wrapsDeadlockWithDialectLabelAndOperation() {
    RuntimeException cause = new RuntimeException("deadlock");

    RatchetTransientStoreException wrapped =
        TransientStoreExceptions.translateOrNull("MySQL", detector(true, false), "save job", cause);

    assertSame(cause, wrapped.getCause());
    assertTrue(wrapped.getMessage().contains("MySQL"));
    assertTrue(wrapped.getMessage().contains("save job"));
  }

  @Test
  void wrapsTransientConnectionFailure() {
    RuntimeException cause = new RuntimeException("connection reset");

    RatchetTransientStoreException wrapped =
        TransientStoreExceptions.translateOrNull(
            "PostgreSQL", detector(false, true), "find job", cause);

    assertSame(cause, wrapped.getCause());
  }

  @Test
  void returnsNullForNonTransientFailure() {
    assertNull(
        TransientStoreExceptions.translateOrNull(
            "MongoDB", detector(false, false), "op", new RuntimeException("boom")));
  }

  private static ConstraintDetector detector(boolean deadlock, boolean transientConnection) {
    return new ConstraintDetector() {
      @Override
      public String constraintName(Exception e) {
        return null;
      }

      @Override
      public boolean isDuplicateKey(Exception e) {
        return false;
      }

      @Override
      public boolean isDeadlock(Exception e) {
        return deadlock;
      }

      @Override
      public boolean isTransientConnectionFailure(Exception e) {
        return transientConnection;
      }
    };
  }
}
