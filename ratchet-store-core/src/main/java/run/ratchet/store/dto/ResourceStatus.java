package run.ratchet.store.dto;

/**
 * Snapshot of resource permit status for concurrency monitoring.
 *
 * @param resourceName the name of the resource
 * @param maxConcurrent the configured maximum concurrent permits
 * @param activePermits the number of currently held permits
 * @param available the number of permits currently available
 */
public record ResourceStatus(
    String resourceName, int maxConcurrent, int activePermits, int available) {

  public boolean isFull() {
    return activePermits >= maxConcurrent;
  }
}
