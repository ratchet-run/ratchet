package run.ratchet.spi;

import run.ratchet.api.Incubating;
import java.util.UUID;

/** Serializes job return values before they are stored on the job row. */
@Incubating
public interface ResultPersistenceStrategy {

  SerializedJobResult serialize(UUID jobId, Object result);
}
