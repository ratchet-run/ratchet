package run.ratchet.api.event;

import java.io.Serial;
import java.io.Serializable;
import java.time.Instant;
import run.ratchet.api.Incubating;

/**
 * Fired exactly once per successful bulk cancel-by-tag operation when at least one job was
 * cancelled.
 *
 * <p>Standalone event sibling of the per-job cancellation hierarchy. Bulk cancellation does not
 * carry a single {@code jobId} / {@code businessKey} / {@code priority}, so this event does not
 * extend {@link AbstractJobSchedulerEvent} — observers pattern-matching on per-job fields would
 * fail on bulk events.
 *
 * @see run.ratchet.api.JobSchedulerService#cancelJobsByTag(String)
 * @see run.ratchet.api.JobSchedulerService#cancelRecurringJobsByTag(String)
 */
@Incubating
public class JobsBulkCancelledEvent implements Serializable {

  @Serial private static final long serialVersionUID = 1L;

  private final String tag;
  private final int count;
  private final Instant cancelledAt;

  /**
   * Creates a bulk-cancel event.
   *
   * @param tag tag used to select jobs for cancellation
   * @param count number of jobs successfully cancelled
   * @param cancelledAt instant when the bulk operation completed
   */
  public JobsBulkCancelledEvent(String tag, int count, Instant cancelledAt) {
    this.tag = EventContract.requireNonBlank(tag, "tag");
    this.count = EventContract.requirePositive(count, "count");
    this.cancelledAt = EventContract.requireNonNull(cancelledAt, "cancelledAt");
  }

  /** Returns the tag used to select jobs for cancellation. */
  public String getTag() {
    return tag;
  }

  /** Returns the number of jobs successfully cancelled. */
  public int getCount() {
    return count;
  }

  /** Returns the instant when the bulk operation completed. */
  public Instant getCancelledAt() {
    return cancelledAt;
  }
}
