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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.Serializable;
import java.util.function.Function;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobHandle;

class BatchBuilderIdempotencyKeyTest {

  @Test
  void plainBatchNormalizesAndStoresKey() {
    DefaultBatchBuilder builder = new DefaultBatchBuilder("batch", ignored -> null);
    assertKeyNormalization(builder, builder::withIdempotencyKey, builder::idempotencyKey);
  }

  @Test
  void streamingBatchNormalizesAndStoresKey() {
    DefaultStreamingBatchBuilder<String> builder =
        new DefaultStreamingBatchBuilder<>("batch", null);
    assertKeyNormalization(builder, builder::withIdempotencyKey, builder::idempotencyKey);
  }

  @Test
  void invocationBatchDelegatesKeyToBatchState() {
    DefaultBatchBuilder state = new DefaultBatchBuilder("batch", ignored -> null);
    DefaultInvocationBatchBuilder builder = new DefaultInvocationBatchBuilder(state);
    assertKeyNormalization(builder, builder::withIdempotencyKey, state::idempotencyKey);
  }

  @Test
  void invocationStreamingBatchDelegatesKeyToStreamingState() {
    RecordingStreamingSubmitter submitter = new RecordingStreamingSubmitter();
    DefaultInvocationStreamingBatchBuilder<String> builder =
        new DefaultInvocationStreamingBatchBuilder<>("batch", submitter);
    builder.start();
    assertKeyNormalization(
        builder, builder::withIdempotencyKey, () -> submitter.state.idempotencyKey());
  }

  private static void assertKeyNormalization(
      Object builder, Function<String, ?> setter, Supplier<String> storedKey) {
    assertNull(storedKey.get());
    assertSame(builder, setter.apply("  batch-42 "));
    assertEquals("batch-42", storedKey.get());
    String maximumKey = "k".repeat(36);
    setter.apply(" " + maximumKey + " ");
    assertEquals(maximumKey, storedKey.get());
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> setter.apply("k".repeat(37)));
    assertEquals("Idempotency key must be at most 36 characters, got 37", exception.getMessage());
    assertEquals(maximumKey, storedKey.get());
    assertThrows(IllegalArgumentException.class, () -> setter.apply("batch-é"));
    assertEquals(maximumKey, storedKey.get());
    setter.apply(null);
    assertNull(storedKey.get());
    setter.apply("batch-42");
    setter.apply(" \t\n");
    assertNull(storedKey.get());
  }

  private static class RecordingStreamingSubmitter implements StreamingBatchSubmitter {
    private DefaultStreamingBatchBuilder<?> state;

    @Override
    public <T extends Serializable> JobHandle submit(DefaultStreamingBatchBuilder<T> builder) {
      state = builder;
      return null;
    }
  }
}
