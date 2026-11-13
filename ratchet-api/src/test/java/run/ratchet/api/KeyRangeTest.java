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
package run.ratchet.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.RandomAccess;
import org.junit.jupiter.api.Test;

class KeyRangeTest {
  @Test
  void rejectsInvalidBoundsAndSizes() {
    assertThrows(IllegalArgumentException.class, () -> new KeyRange(1, 1));
    assertThrows(IllegalArgumentException.class, () -> new KeyRange(2, 1));
    assertThrows(IllegalArgumentException.class, () -> KeyRange.split(0, 10, 0));
    assertThrows(IllegalArgumentException.class, () -> KeyRange.split(0, 10, -1));
    assertThrows(IllegalArgumentException.class, () -> KeyRange.split(2, 1, 1));
  }

  @Test
  void splitsEvenAndUnevenRanges() {
    assertEquals(List.of(new KeyRange(0, 2), new KeyRange(2, 4)), KeyRange.split(0, 4, 2));
    assertEquals(
        List.of(new KeyRange(0, 2), new KeyRange(2, 4)).hashCode(),
        KeyRange.split(0, 4, 2).hashCode());
    assertEquals(
        List.of(new KeyRange(0, 2), new KeyRange(2, 4)), KeyRange.split(0, 4, 2).stream().toList());
    assertEquals(List.of(new KeyRange(-1, 2), new KeyRange(2, 4)), KeyRange.split(-1, 4, 3));
  }

  @Test
  void overflowDoesNotWrap() {
    assertThrows(
        ArithmeticException.class, () -> KeyRange.split(Long.MAX_VALUE - 1, Long.MAX_VALUE, 2));
  }

  @Test
  void largeSplitsAreLazyAndBounded() {
    List<KeyRange> ranges = KeyRange.split(0, 100_000_000, 1);
    assertEquals(100_000_000, ranges.size());
    assertTrue(ranges instanceof RandomAccess);
    assertEquals(new KeyRange(99_999_999, 100_000_000), ranges.get(99_999_999));
    assertThrows(UnsupportedOperationException.class, () -> ranges.set(0, new KeyRange(1, 2)));
    assertThrows(IllegalArgumentException.class, () -> KeyRange.split(0, Long.MAX_VALUE, 1));
  }
}
