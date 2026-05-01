package run.ratchet.ri.cdi;

import run.ratchet.spi.TracingCollector;
import jakarta.enterprise.context.ApplicationScoped;

/** Default no-op {@link TracingCollector} for deployments without a tracing integration. */
@ApplicationScoped
public class NoOpTracingCollector implements TracingCollector {}
