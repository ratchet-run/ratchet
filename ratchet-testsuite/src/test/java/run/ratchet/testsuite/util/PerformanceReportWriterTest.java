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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PerformanceReportWriterTest {

  @TempDir Path buildDir;

  @AfterEach
  void clearBuildDirectoryProperty() {
    System.clearProperty("project.build.directory");
  }

  @Test
  void writeClassFragmentCreatesOutputDirectoryAndClearsBufferedReports() throws Exception {
    System.setProperty("project.build.directory", buildDir.toString());
    PerformanceReportWriter writer = new PerformanceReportWriter("postgresql");
    PerformanceReport report = report("throughput", 100, 50.0);

    writer.addReport(report);
    writer.writeClassFragment("ThroughputIT");

    Path fragment = buildDir.resolve("performance-results/postgresql-ThroughputIT.json");
    assertTrue(Files.isRegularFile(fragment));
    assertEquals(report.toJson() + "\n", Files.readString(fragment));

    writer.writeClassFragment("ThroughputIT");
    assertEquals(report.toJson() + "\n", Files.readString(fragment));
  }

  @Test
  void writeClassFragmentAppendsSubsequentReports() throws Exception {
    System.setProperty("project.build.directory", buildDir.toString());
    PerformanceReportWriter writer = new PerformanceReportWriter("mysql");
    PerformanceReport first = report("first", 10, 10.0);
    PerformanceReport second = report("second", 20, 20.0);

    writer.addReport(first);
    writer.writeClassFragment("QueueLatencyIT");
    writer.addReport(second);
    writer.writeClassFragment("QueueLatencyIT");

    Path fragment = buildDir.resolve("performance-results/mysql-QueueLatencyIT.json");
    assertEquals(first.toJson() + "\n" + second.toJson() + "\n", Files.readString(fragment));
  }

  @Test
  void writeAggregateReportReturnsWhenOutputDirectoryIsMissing() {
    System.setProperty("project.build.directory", buildDir.toString());

    PerformanceReportWriter.writeAggregateReport("missing");

    assertFalse(Files.exists(buildDir.resolve("performance-results")));
  }

  @Test
  void writeAggregateReportCombinesMatchingFragmentsOnly() throws Exception {
    System.setProperty("project.build.directory", buildDir.toString());
    Path outputDir = buildDir.resolve("performance-results");
    Files.createDirectories(outputDir);
    PerformanceReport first = report("first", 100, 100.0);
    PerformanceReport second = report("second", 200, 200.0);
    Files.writeString(outputDir.resolve("postgresql-A.json"), first.toJson() + "\n");
    Files.writeString(outputDir.resolve("postgresql-B.json"), second.toJson() + "\n");
    Files.writeString(
        outputDir.resolve("mysql-A.json"), report("other-db", 1, 1.0).toJson() + "\n");
    Files.writeString(outputDir.resolve("postgresql-results.json"), report("old", 1, 1.0).toJson());

    String stdout = captureStdout(() -> PerformanceReportWriter.writeAggregateReport("postgresql"));

    Path combined = outputDir.resolve("postgresql-results.json");
    String json = Files.readString(combined);
    assertTrue(json.contains("\"database\": \"postgresql\""));
    assertTrue(json.contains(first.toJson()));
    assertTrue(json.contains(second.toJson()));
    assertFalse(json.contains("other-db"));
    assertFalse(json.contains("old"));
    assertTrue(stdout.contains(first.toSummaryLine()));
    assertTrue(stdout.contains(second.toSummaryLine()));
  }

  @Test
  void writeAggregateReportSkipsMalformedFragmentEntries() throws Exception {
    System.setProperty("project.build.directory", buildDir.toString());
    Path outputDir = buildDir.resolve("performance-results");
    Files.createDirectories(outputDir);
    PerformanceReport valid = report("valid", 50, 25.0);
    Files.writeString(
        outputDir.resolve("postgresql-A.json"),
        valid.toJson() + "\n" + "{\"scenario\":\"broken\"\n");

    PerformanceReportWriter.writeAggregateReport("postgresql");

    String json = Files.readString(outputDir.resolve("postgresql-results.json"));
    assertTrue(json.contains(valid.toJson()));
    assertFalse(json.contains("broken"));
  }

  private static PerformanceReport report(String scenarioName, int jobCount, double throughput) {
    return new PerformanceReport(scenarioName, jobCount, 1_000, throughput, 10, 20, 30);
  }

  private static String captureStdout(Runnable runnable) {
    PrintStream originalOut = System.out;
    ByteArrayOutputStream captured = new ByteArrayOutputStream();
    System.setOut(new PrintStream(captured, true, StandardCharsets.UTF_8));
    try {
      runnable.run();
    } finally {
      System.setOut(originalOut);
    }
    return captured.toString(StandardCharsets.UTF_8);
  }
}
