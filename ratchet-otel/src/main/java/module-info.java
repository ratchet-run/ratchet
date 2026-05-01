module run.ratchet.otel {
  requires run.ratchet.api;
  requires jakarta.annotation;
  requires jakarta.cdi;
  requires jakarta.inject;
  requires io.opentelemetry.api;
  requires io.opentelemetry.context;

  exports run.ratchet.otel;

  opens run.ratchet.otel;
}
