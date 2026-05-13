package run.ratchet.spi;

import java.util.UUID;
import run.ratchet.api.Incubating;

/**
 * Serializes job return values before they are stored on the job row.
 *
 * @since 0.1
 */
@Incubating
public interface ResultPersistenceStrategy {

  /**
   * Serializes a completed job result.
   *
   * @param jobId job whose result is being persisted
   * @param result returned value; may be {@code null}
   * @return serialized representation; never {@code null}
   * @apiNote Returning {@code null} violates the SPI contract and will fail callers that persist
   *     the returned value without another null check.
   */
  SerializedJobResult serialize(UUID jobId, Object result);
}
