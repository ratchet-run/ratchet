package run.ratchet.spi;

import run.ratchet.api.Incubating;

/** Stateful adaptive polling delay policy. */
@Incubating
public interface PollingDelayStrategy {

  long getCurrentDelay();

  void onWakeup();

  long recordPollResult(int jobCount, long pollStartTime);

  void updateSystemLoadFactor(double avgUtilization);

  boolean isInDeepIdle();
}
