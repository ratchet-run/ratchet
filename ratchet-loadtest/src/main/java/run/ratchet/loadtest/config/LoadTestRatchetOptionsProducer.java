package run.ratchet.loadtest.config;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import run.ratchet.api.RatchetOptions;
import run.ratchet.api.RatchetOptionsFactory;

@ApplicationScoped
public class LoadTestRatchetOptionsProducer {

  @Produces
  @ApplicationScoped
  public RatchetOptions ratchetOptions() {
    return RatchetOptionsFactory.fromEnvironment();
  }
}
