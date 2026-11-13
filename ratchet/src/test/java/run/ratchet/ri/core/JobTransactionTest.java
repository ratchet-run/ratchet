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
package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import org.junit.jupiter.api.Test;

class JobTransactionTest {
  @Test
  void markerIsReentrantAndClearedOnReturn() throws Exception {
    JobTransaction carrier = new JobTransaction();
    assertFalse(JobTransaction.active());
    assertEquals(
        "done",
        carrier.run(
            () -> {
              assertTrue(JobTransaction.active());
              carrier.run(
                  () -> {
                    assertTrue(JobTransaction.active());
                    return null;
                  });
              assertTrue(JobTransaction.active());
              return "done";
            }));
    assertFalse(JobTransaction.active());
  }

  @Test
  void checkedExceptionIsUnwrappedAndNestedMarkerRestored() throws Exception {
    JobTransaction carrier = new JobTransaction();
    IOException failure = new IOException("body");
    carrier.run(
        () -> {
          assertSame(
              failure,
              assertThrows(
                  IOException.class,
                  () ->
                      carrier.run(
                          () -> {
                            throw failure;
                          })));
          assertTrue(JobTransaction.active());
          return null;
        });
    assertSame(
        failure,
        assertThrows(
            IOException.class,
            () ->
                carrier.run(
                    () -> {
                      throw failure;
                    })));
    assertFalse(JobTransaction.active());
    assertThrows(
        AssertionError.class,
        () ->
            carrier.run(
                () -> {
                  throw new AssertionError();
                }));
    assertFalse(JobTransaction.active());
  }
}
