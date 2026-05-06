package run.ratchet.spi;

import java.util.Map;
import java.util.UUID;
import run.ratchet.api.Incubating;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;

/** Metadata available when creating a per-job logger. */
@Incubating
public record JobLoggerContext(
    UUID jobId,
    JobType jobType,
    JobPriority priority,
    String nodeId,
    String createdBy,
    Map<String, String> params) {

  public JobLoggerContext {
    params = params == null ? Map.of() : Map.copyOf(params);
  }
}
