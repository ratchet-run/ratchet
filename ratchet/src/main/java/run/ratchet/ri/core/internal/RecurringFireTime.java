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
package run.ratchet.ri.core.internal;

import com.cronutils.model.time.ExecutionTime;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Optional;

/** Calculates cron fire times with each local wall-clock time occurring at most once. */
public final class RecurringFireTime {

  private RecurringFireTime() {}

  /**
   * Returns the next execution strictly after {@code base}, skipping the later offset of an overlap
   * when that local time's earlier occurrence is at or before {@code base}.
   *
   * @param executionTime parsed cron schedule
   * @param base instant from which to advance, either the previous fire or the current time
   * @param zone time zone in which to evaluate the cron
   * @return the next fire instant, or empty if the schedule is exhausted
   */
  public static Optional<Instant> nextExecution(
      ExecutionTime executionTime, Instant base, ZoneId zone) {
    Optional<ZonedDateTime> next = executionTime.nextExecution(base.atZone(zone));
    while (next.isPresent()) {
      ZonedDateTime candidate = next.get();
      Instant earlier = candidate.withEarlierOffsetAtOverlap().toInstant();
      if (earlier.equals(candidate.toInstant()) || earlier.isAfter(base)) {
        return Optional.of(candidate.toInstant());
      }
      // Keep the original base for the comparison: several cron matches may lie in the overlap.
      next = executionTime.nextExecution(candidate);
    }
    return Optional.empty();
  }
}
