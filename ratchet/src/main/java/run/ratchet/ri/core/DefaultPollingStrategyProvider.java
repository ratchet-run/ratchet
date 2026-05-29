package run.ratchet.ri.core;

import jakarta.enterprise.context.ApplicationScoped;
import run.ratchet.ri.core.internal.PollingStrategy;
import run.ratchet.spi.PollingConfig;
import run.ratchet.spi.PollingDelayStrategy;
import run.ratchet.spi.PollingStrategyProvider;

/** Default provider for the RI adaptive polling strategy. */
@ApplicationScoped
public class DefaultPollingStrategyProvider implements PollingStrategyProvider {

  @Override
  public PollingDelayStrategy create(PollingConfig config) {
    return new PollingStrategy(
        config.burstDelayMs(),
        config.minDelayMs(),
        config.maxDelayMs(),
        config.deepIdleDelayMs(),
        config.deepIdleThresholdMs(),
        config.idleThreshold(),
        config.batchSize());
  }
}
