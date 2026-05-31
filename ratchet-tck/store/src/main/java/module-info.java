module run.ratchet.tck.store {
  requires run.ratchet.tck.util;
  requires run.ratchet.api;
  requires run.ratchet.store.core;
  requires jakarta.persistence;
  requires jakarta.transaction;
  requires java.sql;
  requires org.junit.jupiter.api;
  requires org.junit.platform.launcher;

  exports run.ratchet.tck.store;
  exports run.ratchet.tck.store.schema;

  provides org.junit.platform.launcher.TestExecutionListener with
      run.ratchet.tck.store.ConformanceReportExtension;
}
