module run.ratchet.tck.jakarta {
  requires transitive run.ratchet.api;
  requires transitive run.ratchet.tck.api;
  requires run.ratchet.tck.util;
  requires jakarta.cdi;
  requires jakarta.inject;
  requires jakarta.transaction;
  requires org.junit.jupiter.api;

  exports run.ratchet.tck.jakarta;

  provides org.junit.platform.launcher.TestExecutionListener with
      run.ratchet.tck.jakarta.JakartaConformanceReportExtension;
}
