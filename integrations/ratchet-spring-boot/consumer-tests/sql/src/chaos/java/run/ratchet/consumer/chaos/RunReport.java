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
package run.ratchet.consumer.chaos;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;

/** Writes failure evidence beside the child JVM logs. */
public final class RunReport {
  public static void assertClean(
      String scenario,
      String store,
      FaultTimeline timeline,
      Ledger ledger,
      List<Oracle.Violation> violations)
      throws Exception {
    if (violations.isEmpty()) return;
    Path path =
        Path.of("target", "chaos-reports", scenario + "-" + System.currentTimeMillis() + ".json");
    Files.createDirectories(path.getParent());
    var jobs = violations.stream().map(Oracle.Violation::jobId).toList();
    var report = new LinkedHashMap<String, Object>();
    report.put("scenario", scenario);
    report.put("store", store);
    report.put("faultTimeline", timeline.entries());
    report.put("violations", violations);
    report.put(
        "attempts",
        ledger.attempts().stream()
            .filter(a -> jobs.contains(a.jobId()) || jobs.contains(null))
            .toList());
    report.put(
        "commits",
        ledger.commits().stream()
            .filter(c -> jobs.contains(c.jobId()) || jobs.contains(null))
            .toList());
    new ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(path.toFile(), report);
    throw new AssertionError("Chaos violations; report: " + path + " " + violations);
  }
}
