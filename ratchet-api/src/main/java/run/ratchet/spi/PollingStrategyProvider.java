package run.ratchet.spi;

import run.ratchet.api.Incubating;

/** Creates the polling delay strategy used by the RI poller. */
@Incubating
public interface PollingStrategyProvider {

  PollingDelayStrategy create(PollingConfig config);
}
