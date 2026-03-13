package run.ratchet.store.spi;

import run.ratchet.store.entity.DlqAlertEntity;
import java.time.Instant;

/** Dead Letter Queue alert persistence operations. */
public interface DlqAlertStore {

  /** Persists or updates a dead-letter alert record. */
  DlqAlertEntity saveDlqAlert(DlqAlertEntity alert);

  /** Returns whether a matching DLQ alert has already been emitted since the supplied cutoff. */
  boolean existsRecentDlqAlert(long jobId, String errorHash, Instant cutoff);
}
