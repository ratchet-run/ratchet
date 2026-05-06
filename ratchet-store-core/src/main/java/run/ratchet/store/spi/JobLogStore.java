package run.ratchet.store.spi;

import java.time.Instant;
import run.ratchet.api.Incubating;
import run.ratchet.store.entity.JobLogEntity;

/** Per-job log storage operations. */
@Incubating
public interface JobLogStore {

  void appendLog(JobLogEntity log);

  int purgeLogsOlderThan(Instant cutoff);
}
