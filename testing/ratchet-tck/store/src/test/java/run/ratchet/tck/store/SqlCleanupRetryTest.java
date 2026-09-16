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

import static org.junit.jupiter.api.Assertions.*;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class SqlCleanupRetryTest {
  @Test
  void retriesDeadlockWithANewInvocation() {
    AtomicInteger attempts = new AtomicInteger();
    SqlCleanupRetry.run(
        () -> {
          if (attempts.incrementAndGet() == 1) {
            throw new IllegalStateException(new SQLException("deadlock victim", "40001", 1205));
          }
        });
    assertEquals(2, attempts.get());
  }

  @Test
  void propagatesNonTransientFailuresWithoutRetry() {
    AtomicInteger attempts = new AtomicInteger();
    var failure = new IllegalStateException(new SQLException("missing table", "42S02"));
    assertSame(
        failure,
        assertThrows(
            IllegalStateException.class,
            () ->
                SqlCleanupRetry.run(
                    () -> {
                      attempts.incrementAndGet();
                      throw failure;
                    })));
    assertEquals(1, attempts.get());
  }

  @Test
  void exhaustedRetriesPropagateTheOriginalFailure() {
    AtomicInteger attempts = new AtomicInteger();
    var failure = new IllegalStateException(new SQLException("deadlock victim", "40001", 1205));
    assertSame(
        failure,
        assertThrows(
            IllegalStateException.class,
            () ->
                SqlCleanupRetry.run(
                    () -> {
                      attempts.incrementAndGet();
                      throw failure;
                    })));
    assertEquals(5, attempts.get());
  }
}
