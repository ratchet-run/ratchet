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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import run.ratchet.api.KeyRange;

class PartitionedBatchBuilderTest {
  private DefaultBatchBuilder builder() {
    return new DefaultBatchBuilder("ranges", ignored -> null);
  }

  public static void process(KeyRange range) {}

  public static void item(String value) {}

  @Test
  void rejectsInvalidPartitionConfiguration() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            builder()
                .forEachPartition(
                    " ", List.of(new KeyRange(0, 1)), PartitionedBatchBuilderTest::process));
    assertThrows(
        IllegalArgumentException.class,
        () -> builder().forEachPartition("set", List.of(), PartitionedBatchBuilderTest::process));
    assertThrows(
        IllegalArgumentException.class,
        () -> builder().forEachPartition("set", null, PartitionedBatchBuilderTest::process));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            builder()
                .forEachPartition(
                    "set",
                    Arrays.asList(new KeyRange(0, 1), null),
                    PartitionedBatchBuilderTest::process));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            builder()
                .forEachPartition(
                    "set",
                    List.of(new KeyRange(0, 3), new KeyRange(2, 4)),
                    PartitionedBatchBuilderTest::process));
  }

  @Test
  void rejectsNonAsciiControlAndOverlongPartitionKeys() {
    for (String key : List.of("konto-år", "a\u0001b", "a".repeat(37))) {
      assertThrows(
          IllegalArgumentException.class,
          () ->
              builder()
                  .forEachPartition(
                      key, List.of(new KeyRange(0, 1)), PartitionedBatchBuilderTest::process));
    }
  }

  @Test
  void accepts36CharacterAsciiPartitionKey() {
    var state = builder();
    String key = "a".repeat(36);
    state.forEachPartition(key, List.of(new KeyRange(0, 1)), PartitionedBatchBuilderTest::process);
    assertEquals(key, state.partitionKey());
  }

  @Test
  void validatesReopenAndPreservesInputOrder() {
    var state = builder();
    var narrowed =
        state.forEachPartition(
            " set ",
            List.of(new KeyRange(3, 4), new KeyRange(0, 3)),
            PartitionedBatchBuilderTest::process);
    assertEquals("set", state.partitionKey());
    assertEquals(3, state.partitions().get(0).from());
    narrowed.reopen(List.of(1, 1));
    assertEquals(1, state.reopened().size());
    assertThrows(IllegalArgumentException.class, () -> narrowed.reopen(List.of(-1)));
    assertThrows(IllegalArgumentException.class, () -> narrowed.reopen(List.of(2)));
    assertThrows(
        IllegalArgumentException.class, () -> narrowed.reopen(Arrays.asList((Integer) null)));
  }

  @Test
  void rejectsMixingEvenThroughRetainedPlainReference() {
    var state = builder();
    state.forEachPartition(
        "set", List.of(new KeyRange(0, 1)), PartitionedBatchBuilderTest::process);
    assertThrows(
        IllegalStateException.class,
        () -> state.forEach(List.of("x"), PartitionedBatchBuilderTest::item));
    assertThrows(IllegalStateException.class, () -> state.reducing(0L, Long::sum));
    assertThrows(
        IllegalStateException.class,
        () ->
            state.forEachPartition(
                "other", List.of(new KeyRange(0, 1)), PartitionedBatchBuilderTest::process));
    assertThrows(
        IllegalStateException.class, () -> state.forEachInvocation(List.of(), ignored -> null));
    var invocation = builder();
    invocation.forEachInvocation(List.of(), ignored -> null);
    assertThrows(
        IllegalStateException.class,
        () ->
            invocation.forEachPartition(
                "set", List.of(new KeyRange(0, 1)), PartitionedBatchBuilderTest::process));
    var plain = builder();
    plain.forEach(List.of("x"), PartitionedBatchBuilderTest::item);
    assertThrows(
        IllegalStateException.class,
        () ->
            plain.forEachPartition(
                "set", List.of(new KeyRange(0, 1)), PartitionedBatchBuilderTest::process));
    var reducing = builder();
    reducing.reducing(0L, Long::sum);
    assertThrows(
        IllegalStateException.class,
        () ->
            reducing.forEachPartition(
                "set", List.of(new KeyRange(0, 1)), PartitionedBatchBuilderTest::process));
  }
}
