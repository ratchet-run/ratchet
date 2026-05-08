package run.ratchet.spi;

import run.ratchet.api.Incubating;

/** Runtime configuration for one circuit breaker profile. */
@Incubating
public record CircuitBreakerConfig(
    float failureRateThreshold,
    int slidingWindowSize,
    long waitDurationMs,
    int permittedCallsInHalfOpen,
    int minimumCalls) {}
