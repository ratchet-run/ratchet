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
package run.ratchet.ri.payload;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import run.ratchet.store.entity.JobPayload;

class RuntimeArgumentsTest {

  private static JobPayload payload(List<Object> args, List<Integer> indexes) {
    return new JobPayload("Target", "m", "(Ljava/lang/Object;)V", true, args, indexes);
  }

  @Test
  void bindFillsOnlyIndexedSlots() {
    JobPayload bound =
        RuntimeArguments.bind(
            payload(Arrays.asList("stored", null, null), Arrays.asList(null, 1, null)),
            List.of("ctx", "error"),
            "test");
    assertEquals(Arrays.asList("stored", "error", null), bound.args());
  }

  @Test
  void bindReturnsPayloadUnchangedWithoutIndexes() {
    JobPayload original = payload(List.of("a"), null);
    assertSame(original, RuntimeArguments.bind(original, List.of("ctx"), "test"));
  }

  @Test
  void bindRejectsIndexListOfDifferentSize() {
    IllegalStateException error =
        assertThrows(
            IllegalStateException.class,
            () ->
                RuntimeArguments.bind(
                    payload(Arrays.asList(null, null), List.of(0)), List.of("ctx"), "Job 7"));
    assertTrue(error.getMessage().contains("Job 7 has 1 runtime argument indexes for 2 arguments"));
  }

  @Test
  void bindReportsMissingRuntimeValueWithCorrectPlural() {
    IllegalStateException one =
        assertThrows(
            IllegalStateException.class,
            () ->
                RuntimeArguments.bind(
                    payload(Arrays.asList((Object) null), List.of(1)), List.of("ctx"), "cb"));
    assertTrue(one.getMessage().endsWith("but only 1 is supplied"));
    IllegalStateException two =
        assertThrows(
            IllegalStateException.class,
            () ->
                RuntimeArguments.bind(
                    payload(Arrays.asList((Object) null), List.of(2)),
                    List.of("ctx", "err"),
                    "cb"));
    assertTrue(two.getMessage().endsWith("but only 2 are supplied"));
  }
}
