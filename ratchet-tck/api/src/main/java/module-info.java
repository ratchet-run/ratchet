module run.ratchet.tck.api {
  requires transitive run.ratchet.api;
  requires run.ratchet.tck.util;
  requires org.junit.jupiter.api;

  exports run.ratchet.tck.api;

  provides org.junit.platform.launcher.TestExecutionListener with
      run.ratchet.tck.api.ApiConformanceReportExtension;
}
