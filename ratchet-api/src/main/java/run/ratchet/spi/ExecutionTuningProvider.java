package run.ratchet.spi;

import run.ratchet.api.Incubating;

/** Supplies execution limits without exposing RI-only execution type enums to API consumers. */
@Incubating
public interface ExecutionTuningProvider {

  boolean useVirtualThreads();

  int maxConcurrency(String executionTypeName, int defaultValue);

  int virtualThreadLimit(String executionTypeName, int defaultValue);
}
