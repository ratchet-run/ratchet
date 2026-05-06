package run.ratchet.store.spi;

import java.time.Instant;
import java.util.UUID;
import run.ratchet.api.Incubating;
import run.ratchet.store.entity.DlqAlertEntity;

/** Dead Letter Queue alert persistence operations. */
@Incubating
public interface DlqAlertStore {

  DlqAlertEntity saveDlqAlert(DlqAlertEntity alert);

  boolean existsRecentDlqAlert(UUID jobId, String errorHash, Instant cutoff);
}
