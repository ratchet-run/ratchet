package run.ratchet.spi;

import run.ratchet.api.Incubating;

/** Supplies execution limits without exposing RI-only execution type enums to API consumers. */
@Incubating
public interface ExecutionTuningProvider {

  /**
   * Returns whether the executor should prefer virtual threads.
   *
   * @return {@code true} to use virtual threads when the runtime supports them
   */
  boolean useVirtualThreads();

  /**
   * Returns the maximum platform-thread concurrency for an execution type.
   *
   * @param executionTypeName public execution type name such as {@code SINGLE} or {@code
   *     BATCH_CHILD}
   * @param defaultValue value Ratchet would use if the provider does not override the type
   * @return effective maximum concurrency; should be positive
   */
  int maxConcurrency(String executionTypeName, int defaultValue);

  /**
   * Returns the virtual-thread concurrency limit for an execution type.
   *
   * @param executionTypeName public execution type name such as {@code SINGLE} or {@code
   *     BATCH_CHILD}
   * @param defaultValue value Ratchet would use if the provider does not override the type
   * @return effective virtual-thread limit; should be positive
   */
  int virtualThreadLimit(String executionTypeName, int defaultValue);
}
