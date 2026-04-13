package run.ratchet.api;

/**
 * Pre-configured circuit breaker profiles for common use cases, referenced from {@link
 * CircuitBreakerProtected} annotations.
 *
 * <ul>
 *   <li><b>DEFAULT</b> — General internal services (50% failure threshold, 100-call window)
 *   <li><b>FAST</b> — Quick failure detection (50% threshold, 20-call window, 10s wait)
 *   <li><b>CRITICAL</b> — High-availability services (75% threshold, 200-call window, 60s wait)
 *   <li><b>EXTERNAL_API</b> — Third-party integrations (60% threshold, 50-call window, 60s wait)
 * </ul>
 */
@Incubating
public enum CircuitBreakerProfile {
  DEFAULT,
  FAST,
  CRITICAL,
  EXTERNAL_API
}
