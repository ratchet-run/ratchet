package run.ratchet.store.spi;

import java.time.Instant;
import java.util.UUID;
import run.ratchet.api.Incubating;
import run.ratchet.store.entity.DlqAlertEntity;

/** Dead Letter Queue alert persistence operations. */
@Incubating
public interface DlqAlertStore {

  /**
   * Persists a DLQ alert. Transaction attribute: {@code REQUIRED}.
   *
   * @param alert alert row to persist; never {@code null}
   * @return persisted alert (with generated identifiers populated); never {@code null}
   */
  DlqAlertEntity saveDlqAlert(DlqAlertEntity alert);

  /**
   * Checks recent alert suppression state. Transaction attribute: {@code SUPPORTS}.
   *
   * @param jobId job id whose alert history is being checked; never {@code null}
   * @param errorHash stable hash of the alerting error message (caller-computed dedupe key); never
   *     {@code null} or blank
   * @param cutoff window lower bound; only alerts at or after this instant count as "recent". Never
   *     {@code null}.
   * @return {@code true} when at least one alert for the same {@code jobId} / {@code errorHash}
   *     pair has been recorded at or after {@code cutoff}, {@code false} otherwise
   */
  boolean existsRecentDlqAlert(UUID jobId, String errorHash, Instant cutoff);
}
