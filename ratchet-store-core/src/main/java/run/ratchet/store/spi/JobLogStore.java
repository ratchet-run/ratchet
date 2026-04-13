package run.ratchet.store.spi;

import run.ratchet.api.Incubating;
import run.ratchet.store.entity.JobLogEntity;
import java.time.Instant;

/** Per-job log storage operations. */
@Incubating
public interface JobLogStore {

  void appendLog(JobLogEntity log);

  int purgeLogsOlderThan(Instant cutoff);
}
