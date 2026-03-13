package run.ratchet.testsuite.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Immutable result record for one performance test scenario. Captures throughput and latency
 * percentile data for reporting and baseline comparison.
 */
public record PerformanceReport(
    String scenarioName,
    int jobCount,
    long totalTimeMs,
    double throughputJobsPerSec,
    long p50Ms,
    long p95Ms,
    long p99Ms) {

  private static final Pattern JSON_PATTERN =
      Pattern.compile(
          "\"scenario\":\"([^\"]+)\""
              + ",\"jobCount\":(\\d+)"
              + ",\"totalTimeMs\":(\\d+)"
              + ",\"throughputJobsPerSec\":([\\d.]+)"
              + ",\"p50Ms\":(\\d+)"
              + ",\"p95Ms\":(\\d+)"
              + ",\"p99Ms\":(\\d+)");

  /** Returns a single-line summary suitable for stdout table output. */
  public String toSummaryLine() {
    return String.format(
        "%-40s  %6d jobs  %8.1f jobs/s  p50=%4dms  p95=%4dms  p99=%4dms  total=%6dms",
        scenarioName, jobCount, throughputJobsPerSec, p50Ms, p95Ms, p99Ms, totalTimeMs);
  }

  /** Returns a JSON representation for machine-readable output. */
  public String toJson() {
    return "{"
        + "\"scenario\":\""
        + scenarioName
        + "\","
        + "\"jobCount\":"
        + jobCount
        + ","
        + "\"totalTimeMs\":"
        + totalTimeMs
        + ","
        + "\"throughputJobsPerSec\":"
        + String.format("%.2f", throughputJobsPerSec)
        + ","
        + "\"p50Ms\":"
        + p50Ms
        + ","
        + "\"p95Ms\":"
        + p95Ms
        + ","
        + "\"p99Ms\":"
        + p99Ms
        + "}";
  }

  /**
   * Parses a {@link PerformanceReport} from its JSON representation produced by {@link #toJson()}.
   *
   * @param json the JSON string to parse
   * @return the parsed report, or {@code null} if the string does not match the expected format
   */
  public static PerformanceReport fromJson(String json) {
    if (json == null || json.isBlank()) {
      return null;
    }
    Matcher m = JSON_PATTERN.matcher(json);
    if (!m.find()) {
      return null;
    }
    return new PerformanceReport(
        m.group(1),
        Integer.parseInt(m.group(2)),
        Long.parseLong(m.group(3)),
        Double.parseDouble(m.group(4)),
        Long.parseLong(m.group(5)),
        Long.parseLong(m.group(6)),
        Long.parseLong(m.group(7)));
  }
}
