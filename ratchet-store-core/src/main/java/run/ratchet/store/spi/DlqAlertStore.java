package run.ratchet.store.spi;

import run.ratchet.store.entity.DlqAlertEntity;
import java.time.Instant;

/** Dead Letter Queue alert persistence operations. */
public interface DlqAlertStore {

  DlqAlertEntity saveDlqAlert(DlqAlertEntity alert);

  boolean existsRecentDlqAlert(long jobId, String errorHash, Instant cutoff);
}
