package run.ratchet.spi;

import run.ratchet.api.Incubating;

/**
 * Initial settings used to create an adaptive polling delay strategy.
 *
 * @param burstDelayMs short delay used immediately after wakeups
 * @param minDelayMs minimum steady-state poll delay
 * @param maxDelayMs maximum backoff delay before deep idle
 * @param deepIdleDelayMs delay used after the poller has been idle long enough
 * @param deepIdleThresholdMs idle time required before entering deep idle
 * @param idleThreshold number of idle polls before increasing delay
 * @param batchSize target claim batch size used to interpret full vs partial polls
 */
@Incubating
public record PollingConfig(
    long burstDelayMs,
    long minDelayMs,
    long maxDelayMs,
    long deepIdleDelayMs,
    long deepIdleThresholdMs,
    int idleThreshold,
    int batchSize) {}
