package run.ratchet.spi;

import java.util.UUID;
import run.ratchet.api.Incubating;

/** Serializes job return values before they are stored on the job row. */
@Incubating
public interface ResultPersistenceStrategy {

  SerializedJobResult serialize(UUID jobId, Object result);
}
