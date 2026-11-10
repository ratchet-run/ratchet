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

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class RecurringFireTimeTest {

  private static final CronParser PARSER =
      new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ));

  @ParameterizedTest
  @CsvSource({
    "America/Chicago, 2026-11-01T06:30:00Z, 2026-11-02T07:30:00Z",
    "Europe/London, 2026-10-25T00:30:00Z, 2026-10-26T01:30:00Z"
  })
  void dailyOverlapFreshCalculationMatchesChainedExecutions(
      String zoneName, String firstFire, String followingFire) {
    ZoneId zone = ZoneId.of(zoneName);
    ExecutionTime schedule = schedule("0 30 1 ? * *");
    Instant first = Instant.parse(firstFire);
    Instant following = Instant.parse(followingFire);
    List<Instant> chained = chain(schedule, zone, first.minusSeconds(3600), 2);
    assertEquals(List.of(first, following), chained);

    for (Instant base :
        List.of(
            first.minusSeconds(1800),
            first.minusNanos(1),
            first,
            first.plusSeconds(900),
            first.plusSeconds(1800),
            first.plusSeconds(3599),
            first.plusSeconds(3600),
            first.plusSeconds(3601))) {
      assertFreshMatchesChain(schedule, zone, base, chained);
    }
  }

  @ParameterizedTest
  @CsvSource({"America/Chicago, 2026-11-01T06:00:00Z", "Europe/London, 2026-10-25T00:00:00Z"})
  void quarterHourlyOverlapFiresEachLocalTimeOnce(String zoneName, String firstFire) {
    ZoneId zone = ZoneId.of(zoneName);
    ExecutionTime schedule = schedule("0 */15 * ? * *");
    Instant first = Instant.parse(firstFire);
    List<Instant> chained = chain(schedule, zone, first.minusSeconds(60), 7);
    assertEquals(
        List.of(
            first,
            first.plusSeconds(900),
            first.plusSeconds(1800),
            first.plusSeconds(2700),
            first.plusSeconds(7200),
            first.plusSeconds(8100),
            first.plusSeconds(9000)),
        chained);

    // Check fresh submissions throughout both offsets, including exact scheduled instants.
    for (int minute = -1; minute <= 121; minute++) {
      assertFreshMatchesChain(schedule, zone, first.plusSeconds(minute * 60L), chained);
    }
  }

  @Test
  void springForwardFreshCalculationMatchesChainedExecutions() {
    ZoneId zone = ZoneId.of("America/Chicago");
    ExecutionTime schedule = schedule("0 30 2 ? * *");
    List<Instant> chained = chain(schedule, zone, Instant.parse("2026-03-07T08:00:00Z"), 2);
    assertEquals(
        List.of(Instant.parse("2026-03-07T08:30:00Z"), Instant.parse("2026-03-09T07:30:00Z")),
        chained);
    for (String base :
        List.of(
            "2026-03-07T08:15:00Z",
            "2026-03-08T07:45:00Z",
            "2026-03-08T08:15:00Z",
            "2026-03-08T09:00:00Z")) {
      assertFreshMatchesChain(schedule, zone, Instant.parse(base), chained);
    }
  }

  @Test
  void overlapCanExhaustFiniteCronAfterSkippingLaterOffset() {
    assertEquals(
        Optional.empty(),
        RecurringFireTime.nextExecution(
            schedule("0 30 1 1 11 ? 2026"),
            Instant.parse("2026-11-01T06:45:00Z"),
            ZoneId.of("America/Chicago")));
  }

  private static ExecutionTime schedule(String expression) {
    return ExecutionTime.forCron(PARSER.parse(expression));
  }

  private static List<Instant> chain(ExecutionTime schedule, ZoneId zone, Instant base, int count) {
    List<Instant> fires = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      base = RecurringFireTime.nextExecution(schedule, base, zone).orElseThrow();
      fires.add(base);
    }
    return fires;
  }

  private static void assertFreshMatchesChain(
      ExecutionTime schedule, ZoneId zone, Instant base, List<Instant> chained) {
    Instant expected =
        chained.stream().filter(fire -> fire.isAfter(base)).findFirst().orElseThrow();
    assertEquals(
        Optional.of(expected),
        RecurringFireTime.nextExecution(schedule, base, zone),
        "Fresh next fire differs from the chain at " + base + " in " + zone);
  }
}
