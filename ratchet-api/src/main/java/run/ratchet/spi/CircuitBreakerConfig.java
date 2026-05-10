package run.ratchet.spi;

import run.ratchet.api.Incubating;

/**
 * Runtime configuration for one circuit breaker profile.
 *
 * @param failureRateThreshold percentage of failed calls, from {@code 0.0} to {@code 100.0}, that
 *     opens the breaker once {@code minimumCalls} has been reached
 * @param slidingWindowSize number of recent calls kept in the failure-rate window
 * @param waitDurationMs time in milliseconds an open breaker waits before moving to half-open
 * @param permittedCallsInHalfOpen number of trial calls allowed while half-open
 * @param minimumCalls minimum calls required before the failure-rate threshold can open the breaker
 */
@Incubating
public record CircuitBreakerConfig(
    float failureRateThreshold,
    int slidingWindowSize,
    long waitDurationMs,
    int permittedCallsInHalfOpen,
    int minimumCalls) {}
