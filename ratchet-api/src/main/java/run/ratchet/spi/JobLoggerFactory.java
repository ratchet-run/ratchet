package run.ratchet.spi;

import run.ratchet.api.Incubating;

/** Creates the {@link JobLogger} bound into {@code JobContext} for each execution. */
@Incubating
public interface JobLoggerFactory {

  JobLogger create(JobLoggerContext context);
}
