package run.ratchet.ri.core;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Objects;
import run.ratchet.api.RatchetOptions;
import run.ratchet.spi.ExecutionTuningProvider;

/** Default execution tuning backed by CDI-provided Ratchet options. */
@ApplicationScoped
public class DefaultExecutionTuningProvider implements ExecutionTuningProvider {

  private final RatchetOptions options;

  protected DefaultExecutionTuningProvider() {
    this.options = null;
  }

  @Inject
  public DefaultExecutionTuningProvider(RatchetOptions options) {
    this.options = Objects.requireNonNull(options, "options must not be null");
  }

  @Override
  public boolean useVirtualThreads() {
    return options().execution().useVirtualThreads();
  }

  @Override
  public int maxConcurrency(String executionTypeName, int defaultValue) {
    return options().execution().maxConcurrency(executionTypeName, defaultValue);
  }

  @Override
  public int virtualThreadLimit(String executionTypeName, int defaultValue) {
    return options().execution().virtualThreadLimit(executionTypeName, defaultValue);
  }

  private RatchetOptions options() {
    if (options == null) {
      throw new IllegalStateException("RatchetOptions were not injected");
    }
    return options;
  }
}
