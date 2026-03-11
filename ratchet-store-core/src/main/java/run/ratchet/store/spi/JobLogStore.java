package run.ratchet.store.spi;

import run.ratchet.store.entity.JobLogEntity;
import java.time.Instant;

/** Per-job log storage operations. */
public interface JobLogStore {

  void appendLog(JobLogEntity log);

  int purgeLogsOlderThan(Instant cutoff);
}
