package run.ratchet.testsuite.event;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import run.ratchet.api.event.JobsBulkCancelledEvent;

/**
 * CDI observer for {@link JobsBulkCancelledEvent}. The standalone (non-{@code
 * AbstractJobSchedulerEvent}) shape of the bulk event prevents the generic {@code EventCapture}
 * from picking it up — bulk events need a dedicated observer.
 */
@ApplicationScoped
public class BulkCancelEventCapture {

  private final CopyOnWriteArrayList<JobsBulkCancelledEvent> events = new CopyOnWriteArrayList<>();

  public void onEvent(@Observes JobsBulkCancelledEvent event) {
    events.add(event);
  }

  public List<JobsBulkCancelledEvent> getEvents() {
    return List.copyOf(events);
  }

  public void clear() {
    events.clear();
  }
}
