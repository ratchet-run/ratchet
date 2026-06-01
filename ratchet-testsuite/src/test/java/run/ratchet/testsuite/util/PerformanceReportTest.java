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
package run.ratchet.testsuite.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

class PerformanceReportTest {

  @Test
  void roundTripsJson() {
    PerformanceReport report =
        new PerformanceReport("throughput.small", 100, 2500, 40.123, 3, 17, 29);

    PerformanceReport parsed = PerformanceReport.fromJson(report.toJson());

    assertEquals(new PerformanceReport("throughput.small", 100, 2500, 40.12, 3, 17, 29), parsed);
  }

  @Test
  void fromJsonReturnsNullForNullBlankAndMalformedJson() {
    assertNull(PerformanceReport.fromJson(null));
    assertNull(PerformanceReport.fromJson(""));
    assertNull(PerformanceReport.fromJson("   "));
    assertNull(PerformanceReport.fromJson("{not json}"));
    assertNull(PerformanceReport.fromJson("{\"scenario\":\"missing-fields\"}"));
  }

  @Test
  void fromJsonReturnsNullForMalformedNumericFields() {
    assertNull(
        PerformanceReport.fromJson(
            "{\"scenario\":\"bad-job-count\",\"jobCount\":999999999999999999999,"
                + "\"totalTimeMs\":1,\"throughputJobsPerSec\":1.0,"
                + "\"p50Ms\":1,\"p95Ms\":1,\"p99Ms\":1}"));
    assertNull(
        PerformanceReport.fromJson(
            "{\"scenario\":\"bad-throughput\",\"jobCount\":1,"
                + "\"totalTimeMs\":1,\"throughputJobsPerSec\":1.2.3,"
                + "\"p50Ms\":1,\"p95Ms\":1,\"p99Ms\":1}"));
    assertNull(
        PerformanceReport.fromJson(
            "{\"scenario\":\"bad-p99\",\"jobCount\":1,"
                + "\"totalTimeMs\":1,\"throughputJobsPerSec\":1.0,"
                + "\"p50Ms\":1,\"p95Ms\":1,\"p99Ms\":999999999999999999999}"));
  }

  @Test
  void fromJsonParsesNumericBoundaries() {
    String json =
        "{\"scenario\":\"max-values\",\"jobCount\":"
            + Integer.MAX_VALUE
            + ",\"totalTimeMs\":"
            + Long.MAX_VALUE
            + ",\"throughputJobsPerSec\":1.7976931348623157E308,"
            + "\"p50Ms\":"
            + Long.MAX_VALUE
            + ",\"p95Ms\":"
            + Long.MAX_VALUE
            + ",\"p99Ms\":"
            + Long.MAX_VALUE
            + "}";

    PerformanceReport parsed = PerformanceReport.fromJson(json);

    assertEquals(
        new PerformanceReport(
            "max-values",
            Integer.MAX_VALUE,
            Long.MAX_VALUE,
            Double.MAX_VALUE,
            Long.MAX_VALUE,
            Long.MAX_VALUE,
            Long.MAX_VALUE),
        parsed);
  }

  @Test
  void toSummaryLineUsesStableFormat() {
    PerformanceReport report =
        new PerformanceReport("queue.latency.p95", 1200, 34567, 34.567, 12, 345, 6789);

    assertEquals(
        String.format(
            "%-40s  %6d jobs  %8.1f jobs/s  p50=%4dms  p95=%4dms  p99=%4dms  total=%6dms",
            "queue.latency.p95", 1200, 34.567, 12, 345, 6789, 34567),
        report.toSummaryLine());
  }
}
