package run.ratchet.spi;

import java.util.UUID;
import run.ratchet.api.Incubating;

/** Serializes job return values before they are stored on the job row. */
@Incubating
public interface ResultPersistenceStrategy {

  /**
   * Serializes a completed job result.
   *
   * @param jobId job whose result is being persisted
   * @param result returned value; may be {@code null}
   * @return serialized representation; never {@code null}
   */
  SerializedJobResult serialize(UUID jobId, Object result);
}
