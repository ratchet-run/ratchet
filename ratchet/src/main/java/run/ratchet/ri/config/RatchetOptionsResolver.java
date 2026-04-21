package run.ratchet.ri.config;

import run.ratchet.api.RatchetOptions;
import run.ratchet.api.RatchetOptionsFactory;
import run.ratchet.spi.RatchetConfigSource;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.spi.DeploymentException;
import jakarta.inject.Inject;

/** Resolves application-provided Ratchet options or builds them from the default source chain. */
@ApplicationScoped
public class RatchetOptionsResolver {

  private final Instance<RatchetOptions> options;
  private final Instance<RatchetConfigSource> configSources;

  protected RatchetOptionsResolver() {
    this.options = null;
    this.configSources = null;
  }

  public RatchetOptionsResolver(Instance<RatchetOptions> options) {
    this.options = options;
    this.configSources = null;
  }

  @Inject
  public RatchetOptionsResolver(
      Instance<RatchetOptions> options, Instance<RatchetConfigSource> configSources) {
    this.options = options;
    this.configSources = configSources;
  }

  public RatchetOptions get() {
    if (options == null || options.isUnsatisfied()) {
      return RatchetOptionsFactory.fromFallbackSources(configSources);
    }
    if (options.isAmbiguous()) {
      throw new DeploymentException(
          "Multiple unqualified RatchetOptions beans found. Produce exactly one @ApplicationScoped"
              + " RatchetOptions bean for the application.");
    }
    return options.get();
  }
}
