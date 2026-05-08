package run.ratchet.api.event;

import java.io.Serial;
import java.io.Serializable;
import java.time.Instant;

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
public class JobsBulkCancelledEvent implements Serializable {

  @Serial private static final long serialVersionUID = 1L;

  private final String tag;
  private final int count;
  private final Instant cancelledAt;

  public JobsBulkCancelledEvent(String tag, int count, Instant cancelledAt) {
    this.tag = tag;
    this.count = count;
    this.cancelledAt = cancelledAt;
  }

  public String getTag() {
    return tag;
  }

  public int getCount() {
    return count;
  }

  public Instant getCancelledAt() {
    return cancelledAt;
  }
}
