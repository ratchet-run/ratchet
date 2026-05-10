package run.ratchet.spi;

import run.ratchet.api.Incubating;

/** Creates the polling delay strategy used by the RI poller. */
@Incubating
public interface PollingStrategyProvider {

  /**
   * Creates a polling delay strategy for one poller.
   *
   * @param config immutable initial polling settings; never {@code null}
   * @return new stateful polling delay strategy; never {@code null}
   */
  PollingDelayStrategy create(PollingConfig config);
}
