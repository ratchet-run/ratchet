package run.ratchet.spi;

import run.ratchet.api.Incubating;

/** Serialized representation of a completed job result for persistence. */
@Incubating
public record SerializedJobResult(String json, String type) {

  /**
   * Returns an empty serialized result.
   *
   * <p>Both {@code json} and {@code type} are {@code null}, which means no result value should be
   * persisted for the job.
   */
  public static SerializedJobResult empty() {
    return new SerializedJobResult(null, null);
  }
}
