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
import java.math.BigInteger;
import java.util.AbstractList;
import java.util.List;
import java.util.Objects;
import java.util.RandomAccess;

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
   * ArithmeticException. Returns an immutable, random-access list without materializing its
   * elements. Counts greater than Integer.MAX_VALUE throw IllegalArgumentException.
   */
  @Incubating
  public static List<KeyRange> split(long from, long to, long size) {
    if (from >= to || size <= 0) {
      throw new IllegalArgumentException("Invalid range or size");
    }
    BigInteger width = BigInteger.valueOf(to).subtract(BigInteger.valueOf(from));
    BigInteger step = BigInteger.valueOf(size);
    BigInteger count = width.add(step).subtract(BigInteger.ONE).divide(step);
    if (count.compareTo(BigInteger.valueOf(Integer.MAX_VALUE)) > 0) {
      throw new IllegalArgumentException("Too many ranges");
    }
    int length = count.intValueExact();
    long lastStart =
        BigInteger.valueOf(from)
            .add(step.multiply(count.subtract(BigInteger.ONE)))
            .longValueExact();
    // Preserve the documented overflow check even when the final range would be truncated.
    Math.addExact(lastStart, size);
    return new Ranges(from, to, size, length);
  }

  private static final class Ranges extends AbstractList<KeyRange> implements RandomAccess {
    private final long from;
    private final long to;
    private final long step;
    private final int length;

    private Ranges(long from, long to, long step, int length) {
      this.from = from;
      this.to = to;
      this.step = step;
      this.length = length;
    }

    @Override
    public KeyRange get(int index) {
      Objects.checkIndex(index, length);
      long start =
          BigInteger.valueOf(from)
              .add(BigInteger.valueOf(step).multiply(BigInteger.valueOf(index)))
              .longValueExact();
      return new KeyRange(start, Math.min(Math.addExact(start, step), to));
    }

    @Override
    public int size() {
      return length;
    }
  }
}
