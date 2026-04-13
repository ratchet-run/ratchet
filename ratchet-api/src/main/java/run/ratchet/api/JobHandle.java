package run.ratchet.api;

/**
 * Lightweight receipt for a submitted job, providing access to its unique identifier.
 *
 * @see JobBuilder#submit()
 * @see BatchBuilder#submit()
 * @see RecurringJobBuilder#submit()
 */
@FunctionalInterface
public interface JobHandle {

  /**
   * @return the unique job identifier assigned by the scheduler
   */
  long id();
}
