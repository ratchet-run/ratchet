package run.ratchet.store.spi;

import java.time.Instant;
import run.ratchet.api.Incubating;
import run.ratchet.store.entity.JobLogEntity;

/** Per-job log storage operations. */
@Incubating
public interface JobLogStore {

  /**
   * Appends one per-job log line.
   *
   * @param log log entity to persist; never {@code null}
   */
  void appendLog(JobLogEntity log);

  /**
   * Deletes log lines older than the cutoff instant.
   *
   * @param cutoff exclusive upper bound; rows with timestamps before this instant are eligible for
   *     deletion
   * @return number of deleted log rows
   */
  int purgeLogsOlderThan(Instant cutoff);
}
