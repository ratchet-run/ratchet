package run.ratchet.spi;

import run.ratchet.api.Incubating;

/** Serializes job return values before they are stored on the job row. */
@Incubating
public interface ResultPersistenceStrategy {

  SerializedJobResult serialize(long jobId, Object result);
}
