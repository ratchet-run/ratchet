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
package run.ratchet.store.oracle;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * Helpers for binding {@link Instant} cutoffs against Oracle's {@code TIMESTAMP(6)} columns.
 *
 * <p>A persisted Instant is floored to microsecond precision by the column. Oracle's JDBC driver,
 * however, binds an Instant or Timestamp parameter at full nanosecond precision and compares it
 * literally, so a cutoff taken from the same Instant as a stored row misses an exclusive ({@code
 * <}) or inclusive-lower ({@code >=}) boundary by the sub-microsecond remainder. This only surfaces
 * on a nanosecond-resolution clock (Linux); macOS returns microseconds, which hides it. The MySQL
 * and PostgreSQL drivers floor the bind to the column scale for us; Oracle does not.
 *
 * <p>Flooring the bound value to microseconds restores the boundary behaviour the stores share.
 */
final class OracleTimestamps {

  private OracleTimestamps() {}

  /** Floors an Instant to microsecond precision to match a {@code TIMESTAMP(6)} column. */
  static Instant floorMicros(Instant value) {
    return value == null ? null : value.truncatedTo(ChronoUnit.MICROS);
  }

  /**
   * Binds an Instant cutoff as a microsecond-precision {@link Timestamp} for native comparisons.
   */
  static Timestamp microTimestamp(Instant value) {
    return value == null ? null : Timestamp.from(floorMicros(value));
  }
}
