package run.ratchet.store.dto;

/** Snapshot of resource permit status for concurrency monitoring. */
public record ResourceStatus(
    String resourceName, int maxConcurrent, int activePermits, int available) {

  public boolean isFull() {
    return activePermits >= maxConcurrent;
  }
}
