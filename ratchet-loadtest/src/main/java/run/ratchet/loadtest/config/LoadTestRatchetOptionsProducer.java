package run.ratchet.loadtest.config;

import run.ratchet.api.RatchetOptions;
import run.ratchet.api.RatchetOptionsFactory;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

@ApplicationScoped
public class LoadTestRatchetOptionsProducer {

  @Produces
  @ApplicationScoped
  public RatchetOptions ratchetOptions() {
    return RatchetOptionsFactory.fromEnvironment();
  }
}
