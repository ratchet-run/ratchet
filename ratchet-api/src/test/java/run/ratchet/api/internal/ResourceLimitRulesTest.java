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
package run.ratchet.api.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import run.ratchet.api.ResourceLimit;

class ResourceLimitRulesTest {

  @Test
  void nameIsTrimmedAndNullOrBlankIsAbsent() {
    assertEquals("gpu", ResourceLimitRules.normalizeName("  gpu \t"));
    assertNull(ResourceLimitRules.normalizeName(null));
    assertNull(ResourceLimitRules.normalizeName(" \t"));
    // Not blank by isBlank(), but trim() strips every character below U+0021.
    assertNull(ResourceLimitRules.normalizeName("\u0000"));
  }

  @Test
  void nameMayBeOneHundredPrintableAsciiCharacters() {
    String longest = "r".repeat(100);
    assertEquals(longest, ResourceLimitRules.normalizeName(longest));
    assertEquals("a b~!", ResourceLimitRules.normalizeName("a b~!"));
  }

  @Test
  void nameLongerThanOneHundredCharactersIsRejected() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> ResourceLimitRules.normalizeName("r".repeat(101)));
    assertEquals("Resource name must be at most 100 characters, got 101", exception.getMessage());
  }

  @Test
  void nameOutsidePrintableAsciiIsRejected() {
    for (String name : new String[] {"gpü", "gpu\u0001pool", "gpu-😀"}) {
      IllegalArgumentException exception =
          assertThrows(
              IllegalArgumentException.class, () -> ResourceLimitRules.normalizeName(name));
      assertEquals(
          "Resource name must contain only printable ASCII characters (U+0020-U+007E)",
          exception.getMessage());
    }
  }

  @Test
  void normalizeTrimsNameAndDescription() {
    ResourceLimit normalized =
        ResourceLimitRules.normalize(
            new ResourceLimit(" gpu ", 3, Duration.ofMillis(1500), "  GPU pool "));

    assertEquals(new ResourceLimit("gpu", 3, Duration.ofMillis(1500), "GPU pool"), normalized);
  }

  @Test
  void normalizeTurnsBlankDescriptionIntoNone() {
    assertNull(
        ResourceLimitRules.normalize(ResourceLimit.of("gpu", 1).withDescription("  "))
            .description());
  }

  @Test
  void normalizeAcceptsTheBoundaryValues() {
    ResourceLimit limit =
        new ResourceLimit(
            "r".repeat(100), 1, Duration.ofMillis(Integer.MAX_VALUE), "d".repeat(255));
    assertEquals(limit, ResourceLimitRules.normalize(limit));
    assertEquals(
        Duration.ZERO,
        ResourceLimitRules.normalize(ResourceLimit.of("gpu", 1).withRetryDelay(Duration.ZERO))
            .retryDelay());
  }

  @Test
  void normalizeRejectsNullLimitAndBlankName() {
    assertEquals(
        "Resource limit must not be null",
        assertThrows(IllegalArgumentException.class, () -> ResourceLimitRules.normalize(null))
            .getMessage());
    assertEquals(
        "Resource name must not be blank",
        assertThrows(
                IllegalArgumentException.class,
                () -> ResourceLimitRules.normalize(ResourceLimit.of("  ", 1)))
            .getMessage());
  }

  @Test
  void normalizeRejectsMaxConcurrentBelowOne() {
    for (int max : new int[] {0, -1}) {
      IllegalArgumentException exception =
          assertThrows(
              IllegalArgumentException.class,
              () -> ResourceLimitRules.normalize(ResourceLimit.of("gpu", max)));
      assertEquals(
          "Resource limit maxConcurrent must be at least 1, got " + max, exception.getMessage());
    }
  }

  @Test
  void normalizeRejectsRetryDelayOutsideZeroToIntMaxMillis() {
    for (Duration delay :
        new Duration[] {
          Duration.ofMillis(-1), Duration.ofMillis(Integer.MAX_VALUE + 1L), Duration.ofDays(36500)
        }) {
      IllegalArgumentException exception =
          assertThrows(
              IllegalArgumentException.class,
              () -> ResourceLimitRules.normalize(ResourceLimit.of("gpu", 1).withRetryDelay(delay)));
      assertEquals(
          "Resource limit retryDelay must be between 0 and 2147483647 ms, got " + delay,
          exception.getMessage());
    }
  }

  @Test
  void normalizeRejectsLongOrNonAsciiDescription() {
    assertEquals(
        "Resource description must be at most 255 characters, got 256",
        assertThrows(
                IllegalArgumentException.class,
                () ->
                    ResourceLimitRules.normalize(
                        ResourceLimit.of("gpu", 1).withDescription("d".repeat(256))))
            .getMessage());
    assertEquals(
        "Resource description must contain only printable ASCII characters (U+0020-U+007E)",
        assertThrows(
                IllegalArgumentException.class,
                () ->
                    ResourceLimitRules.normalize(
                        ResourceLimit.of("gpu", 1).withDescription("Größe")))
            .getMessage());
  }

  @Test
  void normalizeChecksTheNameRule() {
    assertThrows(
        IllegalArgumentException.class,
        () -> ResourceLimitRules.normalize(ResourceLimit.of("r".repeat(101), 1)));
  }
}
