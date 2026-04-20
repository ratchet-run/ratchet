module run.ratchet.micrometer {
  requires run.ratchet.api;
  requires jakarta.annotation;
  requires jakarta.cdi;
  requires jakarta.inject;
  requires micrometer.core;
  requires org.jboss.logging;

  exports run.ratchet.micrometer;

  opens run.ratchet.micrometer;
}
