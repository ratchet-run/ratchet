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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.util.List;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

class DefaultJobBuilderTest {

  @Test
  void withIdempotencyKeyNormalizesAndPreservesGeneratedKeyForNullOrBlank() {
    DefaultJobBuilder builder = newBuilder();
    String generated = builder.idempotencyKey();
    builder.withIdempotencyKey(null);
    builder.withIdempotencyKey(" \t");
    assertEquals(generated, builder.idempotencyKey());
    builder.withIdempotencyKey("  order-42 ");
    assertEquals("order-42", builder.idempotencyKey());
    builder.withIdempotencyKey(null);
    builder.withIdempotencyKey(" ");
    assertEquals("order-42", builder.idempotencyKey());
  }

  @Test
  void withIdempotencyKeyChecksTrimmedLength() {
    DefaultJobBuilder builder = newBuilder();
    String maximumKey = "k".repeat(36);
    builder.withIdempotencyKey(" " + maximumKey + " ");
    assertEquals(maximumKey, builder.idempotencyKey());
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> builder.withIdempotencyKey("k".repeat(37)));
    assertEquals("Idempotency key must be at most 36 characters, got 37", exception.getMessage());
    assertEquals(maximumKey, builder.idempotencyKey());
  }

  @Test
  void withBusinessKeyAcceptsMaximumPortableKeyWithoutTruncation() {
    DefaultJobBuilder builder = newBuilder();
    String key = "k".repeat(255);

    builder.withBusinessKey(key);

    assertEquals(key, builder.businessKey());
  }

  @Test
  void withBusinessKeyRejectsKeyLongerThanPortableLimit() {
    DefaultJobBuilder builder = newBuilder();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> builder.withBusinessKey("k".repeat(256)));

    assertEquals("Business key must be at most 255 characters, got 256", exception.getMessage());
  }

  @Test
  void withBusinessKeyRejectsUnicodeInsteadOfLeavingConversionToTheStore() {
    DefaultJobBuilder builder = newBuilder();

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> builder.withBusinessKey("invoice-😀"));

    assertEquals(
        "Business key must contain only printable ASCII characters (U+0020-U+007E)",
        exception.getMessage());
  }

  @Test
  void withBusinessKeyTreatsAKeyEmptiedByTrimmingAsAbsent() {
    DefaultJobBuilder builder = newBuilder();

    builder.withBusinessKey("\u0000");

    assertNull(builder.businessKey());
  }

  @Test
  void whenResultStoresExplicitPriority() {
    DefaultJobBuilder builder = newBuilder();

    builder.whenResult(value -> true, () -> {}, 7);

    assertEquals(7, builder.workflowBranches().get(0).condition().priority());
  }

  @Test
  void withResourceAcceptsOneHundredPrintableAsciiCharactersAndTrims() {
    DefaultJobBuilder builder = newBuilder();
    String name = "r".repeat(100);

    builder.withResource("  " + name + " ");

    assertEquals(name, builder.resourceName());
  }

  @Test
  void withResourceStillTreatsNullOrBlankAsNoResource() {
    DefaultJobBuilder builder = newBuilder();
    builder.withResource("gpu");

    builder.withResource(" \t");
    assertNull(builder.resourceName());
    builder.withResource("gpu");
    builder.withResource(null);
    assertNull(builder.resourceName());
  }

  @Test
  void withResourceRejectsNamesNoStoreCanHoldTheSameWay() {
    // The limit table's key is VARCHAR(100), and Oracle and SQL Server count it in bytes or in a
    // code page, so the job and batch builders apply the business-key style rule at the call.
    DefaultBatchBuilder batch = new DefaultBatchBuilder("batch", null);
    DefaultStreamingBatchBuilder<String> streaming = new DefaultStreamingBatchBuilder<>("s", null);
    for (var withResource :
        List.<Consumer<String>>of(
            newBuilder()::withResource, batch::withResource, streaming::withResource)) {
      assertEquals(
          "Resource name must be at most 100 characters, got 101",
          assertThrows(IllegalArgumentException.class, () -> withResource.accept("r".repeat(101)))
              .getMessage());
      assertEquals(
          "Resource name must contain only printable ASCII characters (U+0020-U+007E)",
          assertThrows(IllegalArgumentException.class, () -> withResource.accept("gp\u00fc"))
              .getMessage());
    }
  }

  private static DefaultJobBuilder newBuilder() {
    return (DefaultJobBuilder) DefaultJobBuilder.create(ignored -> null, () -> {}, Duration.ZERO);
  }
}
