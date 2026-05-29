package run.ratchet.ri.cdi;

import jakarta.enterprise.context.ApplicationScoped;
import run.ratchet.spi.TracingCollector;

/** Default no-op {@link TracingCollector} for deployments without a tracing integration. */
@ApplicationScoped
class NoOpTracingCollector implements TracingCollector {}
