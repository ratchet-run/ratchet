package run.ratchet.spi;

import run.ratchet.api.Incubating;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import java.util.Map;

/** Metadata available when creating a per-job logger. */
@Incubating
public record JobLoggerContext(
    long jobId,
    JobType jobType,
    JobPriority priority,
    String nodeId,
    String createdBy,
    Map<String, String> params) {

  public JobLoggerContext {
    params = params == null ? Map.of() : Map.copyOf(params);
  }
}
