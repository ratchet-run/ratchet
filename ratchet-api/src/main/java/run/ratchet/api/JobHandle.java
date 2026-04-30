package run.ratchet.api;

import java.util.UUID;

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
   * @return the unique UUIDv7 job identifier assigned by the scheduler
   */
  UUID id();
}
