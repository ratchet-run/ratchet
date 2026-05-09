package run.ratchet.testsuite.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Accumulates {@link PerformanceReport} instances and writes them as fragment files per test class.
 * Writer instances own only their in-memory class-local reports. The static aggregation step owns
 * the process-wide merge because it is invoked after all performance classes have written their
 * fragments.
 *
 * <p>Fragment files are written to {@code target/performance-results/} with the naming convention
 * {@code {dbType}-{className}.json}. The aggregated report is written to {@code
 * {dbType}-results.json}.
 */
public class PerformanceReportWriter {

  private static final Logger log = Logger.getLogger(PerformanceReportWriter.class.getName());

  private final List<PerformanceReport> reports = new ArrayList<>();
  private final String dbType;

  public PerformanceReportWriter(String dbType) {
    this.dbType = dbType;
  }

  /**
   * Reads all fragment files for the given database type, prints a consolidated summary table to
   * stdout, and writes the combined JSON report.
   *
   * @param dbType the database type (e.g., "postgresql", "mysql")
   */
  public static void writeAggregateReport(String dbType) {
    Path outputDir = getOutputDir();
    if (!Files.isDirectory(outputDir)) {
      return;
    }

    List<PerformanceReport> allReports = new ArrayList<>();
    try (Stream<Path> fragments =
        Files.list(outputDir)
            .filter(p -> p.getFileName().toString().startsWith(dbType + "-"))
            .filter(p -> p.getFileName().toString().endsWith(".json"))
            .filter(p -> !p.getFileName().toString().equals(dbType + "-results.json"))
            .sorted()) {
      for (Path fragment : fragments.toList()) {
        List<String> lines = Files.readAllLines(fragment);
        for (String line : lines.stream().filter(l -> !l.isBlank()).toList()) {
          PerformanceReport report = PerformanceReport.fromJson(line);
          if (report == null) {
            log.warning("Skipping malformed performance fragment entry in " + fragment);
          } else {
            allReports.add(report);
          }
        }
      }
    } catch (IOException e) {
      log.warning("Performance fragment read error: " + e.getMessage());
      return;
    }

    if (allReports.isEmpty()) {
      return;
    }

    // Print consolidated summary table
    System.out.println();
    System.out.println(
        "═══════════════════════════════════════════════════════════"
            + "═══════════════════════════════════════════");
    System.out.println("  Ratchet Performance Results — " + dbType.toUpperCase());
    System.out.println(
        "═══════════════════════════════════════════════════════════"
            + "═══════════════════════════════════════════");
    for (PerformanceReport report : allReports) {
      System.out.println("  " + report.toSummaryLine());
    }
    System.out.println(
        "═══════════════════════════════════════════════════════════"
            + "═══════════════════════════════════════════");
    System.out.println();

    // Write combined JSON report
    try {
      Path combinedFile = outputDir.resolve(dbType + "-results.json");
      // language=JSON
      String template =
          """
          {
            "database": "%s",
            "timestamp": "%s",
            "results": [
          %s
            ]
          }""";
      String json =
          template.formatted(
              dbType,
              Instant.now(),
              allReports.stream()
                  .map(PerformanceReport::toJson)
                  .map(e -> "    " + e)
                  .collect(Collectors.joining(",\n")));
      Files.writeString(combinedFile, json);
      log.info(
          "Combined performance report written to "
              + combinedFile
              + " ("
              + allReports.size()
              + " results)");
    } catch (IOException e) {
      log.warning("Combined report write error: " + e.getMessage());
    }
  }

  private static Path getOutputDir() {
    return Path.of(System.getProperty("project.build.directory", "target"), "performance-results");
  }

  public void addReport(PerformanceReport report) {
    reports.add(report);
  }

  /**
   * Writes this class's accumulated reports as a fragment file. Called from {@code @AfterEach} in
   * each performance test class.
   *
   * @param className the simple class name (used in the fragment filename)
   */
  public void writeClassFragment(String className) {
    if (reports.isEmpty()) {
      return;
    }
    Path outputDir = getOutputDir();
    try {
      Files.createDirectories(outputDir);
      Path fragmentFile = outputDir.resolve(dbType + "-" + className + ".json");
      String json =
          reports.stream().map(PerformanceReport::toJson).collect(Collectors.joining("\n")) + "\n";
      Files.writeString(fragmentFile, json, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
      log.info(
          "Performance fragment written to " + fragmentFile + " (" + reports.size() + " results)");
      reports.clear();
    } catch (IOException e) {
      log.warning("Performance fragment write error: " + e.getMessage());
    }
  }
}
