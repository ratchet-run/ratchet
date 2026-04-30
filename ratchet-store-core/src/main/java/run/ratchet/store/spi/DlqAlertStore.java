package run.ratchet.store.spi;

import run.ratchet.api.Incubating;
import run.ratchet.store.entity.DlqAlertEntity;
import java.time.Instant;
import java.util.UUID;

/** Dead Letter Queue alert persistence operations. */
@Incubating
public interface DlqAlertStore {

  DlqAlertEntity saveDlqAlert(DlqAlertEntity alert);

  boolean existsRecentDlqAlert(UUID jobId, String errorHash, Instant cutoff);
}
