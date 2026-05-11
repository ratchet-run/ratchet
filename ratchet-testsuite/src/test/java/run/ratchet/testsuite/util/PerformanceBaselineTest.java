package run.ratchet.testsuite.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PerformanceBaselineTest {

  @Test
  void constructorThrowsWhenBaselineResourceIsUnreadable() {
    ClassLoader original = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(new UnreadableBaselineClassLoader(original));
    try {
      assertThrows(
          UncheckedIOException.class, () -> new PerformanceBaseline("mysql", 0.10, "unused"));
    } finally {
      Thread.currentThread().setContextClassLoader(original);
    }
  }

  @Test
  void throughputComparisonRejectsNanAndInfinity() {
    PerformanceBaseline baseline = new PerformanceBaseline("missing", 0.10, "perf-baselines");

    assertThrows(
        AssertionError.class, () -> baseline.assertWithinTolerance("jobsPerSecond", Double.NaN));
    assertThrows(
        AssertionError.class,
        () -> baseline.assertWithinTolerance("jobsPerSecond", Double.POSITIVE_INFINITY));
  }

  @Test
  void latencyComparisonRejectsNanAndInfinity() {
    PerformanceBaseline baseline = new PerformanceBaseline("missing", 0.10, "perf-baselines");

    assertThrows(
        AssertionError.class, () -> baseline.assertLatencyWithinTolerance("p95", Double.NaN));
    assertThrows(
        AssertionError.class,
        () -> baseline.assertLatencyWithinTolerance("p95", Double.NEGATIVE_INFINITY));
  }

  @Test
  void writeRecordedBaselinesWritesMetricsAndPreservesExistingValues(@TempDir Path targetDir)
      throws IOException {
    String originalBuildDir = System.getProperty("project.build.directory");
    System.setProperty("project.build.directory", targetDir.toString());
    try {
      Path outputDir = targetDir.resolve("perf-baselines");
      Files.createDirectories(outputDir);
      Path outputFile = outputDir.resolve("mysql-baselines.properties");
      Files.writeString(outputFile, "existingMetric=7.00\n");

      PerformanceBaseline baseline =
          new PerformanceBaseline("mysql", 0.10, "missing-perf-baselines");
      baseline.assertWithinTolerance("jobsPerSecond", 123.456);
      baseline.assertLatencyWithinTolerance("p95", 42.0);

      baseline.writeRecordedBaselines();

      assertTrue(Files.exists(outputFile), "Baseline file should be written");
      Properties written = new Properties();
      try (InputStream in = Files.newInputStream(outputFile)) {
        written.load(in);
      }
      assertEquals("7.00", written.getProperty("existingMetric"));
      assertEquals("123.46", written.getProperty("jobsPerSecond"));
      assertEquals("42.00", written.getProperty("p95"));
    } finally {
      if (originalBuildDir == null) {
        System.clearProperty("project.build.directory");
      } else {
        System.setProperty("project.build.directory", originalBuildDir);
      }
    }
  }

  private static final class UnreadableBaselineClassLoader extends ClassLoader {

    private UnreadableBaselineClassLoader(ClassLoader parent) {
      super(parent);
    }

    @Override
    public InputStream getResourceAsStream(String name) {
      if ("unused/mysql-baselines.properties".equals(name)) {
        return new InputStream() {
          @Override
          public int read() throws IOException {
            throw new IOException("synthetic read failure");
          }
        };
      }
      return super.getResourceAsStream(name);
    }
  }
}
