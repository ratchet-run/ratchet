package run.ratchet.spi;

import run.ratchet.api.Incubating;

/**
 * Stateful adaptive polling delay policy.
 *
 * <p>The RI calls {@link #getCurrentDelay()} before sleeping, {@link #onWakeup()} when local or
 * cluster wakeups arrive, {@link #recordPollResult(int, long)} after each poll cycle, and {@link
 * #updateSystemLoadFactor(double)} as executor utilization changes. Implementations may keep
 * internal mutable state and must be thread-safe.
 */
@Incubating
public interface PollingDelayStrategy {

  /**
   * Returns the current poll delay in milliseconds.
   *
   * @return non-negative delay before the next poll
   */
  long getCurrentDelay();

  /**
   * Records an external wakeup signal that should reduce or reset the next poll delay.
   *
   * <p>Wakeups may be delivered concurrently with poll-result updates.
   */
  void onWakeup();

  /**
   * Records the result of a poll cycle.
   *
   * @param jobCount number of jobs claimed during the poll; never negative
   * @param pollStartTime epoch millisecond timestamp captured when the poll began
   * @return the next poll delay in milliseconds
   */
  long recordPollResult(int jobCount, long pollStartTime);

  /**
   * Updates the observed executor utilization.
   *
   * @param avgUtilization average utilization from {@code 0.0} to {@code 1.0}
   */
  void updateSystemLoadFactor(double avgUtilization);

  /**
   * Returns whether the strategy is currently in its deepest idle-delay state.
   *
   * @return {@code true} when idle long enough to use the deep-idle delay
   */
  boolean isInDeepIdle();
}
