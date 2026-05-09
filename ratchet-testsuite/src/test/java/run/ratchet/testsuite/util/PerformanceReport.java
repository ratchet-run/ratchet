package run.ratchet.testsuite.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Result record for one performance scenario. */
public record PerformanceReport(
    String scenarioName,
    int jobCount,
    long totalTimeMs,
    double throughputJobsPerSec,
    long p50Ms,
    long p95Ms,
    long p99Ms) {

  // language=RegExp
  private static final Pattern JSON_PATTERN =
      Pattern.compile(
          """
          "scenario":"([^"]+)"\
          ,"jobCount":(\\d+)\
          ,"totalTimeMs":(\\d+)\
          ,"throughputJobsPerSec":(\\d+(?:\\.\\d+)?(?:[Ee][+-]?\\d+)?)\
          ,"p50Ms":(\\d+)\
          ,"p95Ms":(\\d+)\
          ,"p99Ms":(\\d+)""");

  public static PerformanceReport fromJson(String json) {
    if (json == null || json.isBlank()) {
      return null;
    }
    Matcher m = JSON_PATTERN.matcher(json);
    if (!m.find()) {
      return null;
    }
    try {
      return new PerformanceReport(
          m.group(1),
          Integer.parseInt(m.group(2)),
          Long.parseLong(m.group(3)),
          Double.parseDouble(m.group(4)),
          Long.parseLong(m.group(5)),
          Long.parseLong(m.group(6)),
          Long.parseLong(m.group(7)));
    } catch (NumberFormatException e) {
      return null;
    }
  }

  public String toSummaryLine() {
    return String.format(
        "%-40s  %6d jobs  %8.1f jobs/s  p50=%4dms  p95=%4dms  p99=%4dms  total=%6dms",
        scenarioName, jobCount, throughputJobsPerSec, p50Ms, p95Ms, p99Ms, totalTimeMs);
  }

  public String toJson() {
    // language=JSON
    String template =
        """
        {"scenario":"%s","jobCount":%d,"totalTimeMs":%d,"throughputJobsPerSec":%s,\
        "p50Ms":%d,"p95Ms":%d,"p99Ms":%d}""";
    return template.formatted(
        scenarioName,
        jobCount,
        totalTimeMs,
        String.format("%.2f", throughputJobsPerSec),
        p50Ms,
        p95Ms,
        p99Ms);
  }
}
