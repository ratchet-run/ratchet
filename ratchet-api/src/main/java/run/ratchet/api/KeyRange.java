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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * A key range, from inclusive to exclusive.
 *
 * @param from inclusive lower bound
 * @param to exclusive upper bound
 */
@Incubating
public record KeyRange(long from, long to) implements Serializable {
  /** Rejects empty or reversed ranges. */
  public KeyRange {
    if (from >= to) throw new IllegalArgumentException("from must be less than to");
  }

  /** Returns the inclusive lower bound for bean serializers. */
  @Incubating
  public long getFrom() {
    return from;
  }

  /** Returns the exclusive upper bound for bean serializers. */
  @Incubating
  public long getTo() {
    return to;
  }

  /**
   * Splits a range into consecutive ranges of at most size keys. Overflow throws
   * ArithmeticException.
   */
  @Incubating
  public static List<KeyRange> split(long from, long to, long size) {
    if (from >= to || size <= 0) throw new IllegalArgumentException("Invalid range or size");
    List<KeyRange> ranges = new ArrayList<>();
    for (long start = from; start < to; ) {
      long end = Math.min(Math.addExact(start, size), to);
      ranges.add(new KeyRange(start, end));
      start = end;
    }
    return List.copyOf(ranges);
  }
}
