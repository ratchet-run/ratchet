package run.ratchet.store.spi;

import run.ratchet.api.Incubating;
import run.ratchet.store.entity.JobLogEntity;
import java.time.Instant;

/** Per-job log storage operations. */
@Incubating
public interface JobLogStore {

  /** Appends one log entry for a job. */
  void appendLog(JobLogEntity log);

  /** Deletes job log rows older than the cutoff and returns the number removed. */
  int purgeLogsOlderThan(Instant cutoff);
}
