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
import org.junit.jupiter.api.Test;

class DefaultJobBuilderTest {

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

  private static DefaultJobBuilder newBuilder() {
    return (DefaultJobBuilder) DefaultJobBuilder.create(ignored -> null, () -> {}, Duration.ZERO);
  }
}
