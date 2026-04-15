package run.ratchet.spi;

import run.ratchet.api.Incubating;

/** Initial settings used to create an adaptive polling delay strategy. */
@Incubating
public record PollingConfig(
    long burstDelayMs,
    long minDelayMs,
    long maxDelayMs,
    long deepIdleDelayMs,
    long deepIdleThresholdMs,
    int idleThreshold,
    int batchSize) {}
