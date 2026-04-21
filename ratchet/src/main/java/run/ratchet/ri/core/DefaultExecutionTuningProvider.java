package run.ratchet.ri.core;

import run.ratchet.api.RatchetOptions;
import run.ratchet.ri.config.RatchetOptionsResolver;
import run.ratchet.spi.ExecutionTuningProvider;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/** Default execution tuning backed by CDI-provided Ratchet options. */
@ApplicationScoped
public class DefaultExecutionTuningProvider implements ExecutionTuningProvider {

  private final RatchetOptions options;

  protected DefaultExecutionTuningProvider() {
    this.options = null;
  }

  @Inject
  public DefaultExecutionTuningProvider(RatchetOptionsResolver optionsResolver) {
    this(optionsResolver.get());
  }

  public DefaultExecutionTuningProvider(RatchetOptions options) {
    this.options = options;
  }

  @Override
  public boolean useVirtualThreads() {
    return options.execution().useVirtualThreads();
  }

  @Override
  public int maxConcurrency(String executionTypeName, int defaultValue) {
    return options.execution().maxConcurrency(executionTypeName, defaultValue);
  }

  @Override
  public int virtualThreadLimit(String executionTypeName, int defaultValue) {
    return options.execution().virtualThreadLimit(executionTypeName, defaultValue);
  }
}
