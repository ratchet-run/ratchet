package run.ratchet.spi;

import run.ratchet.api.Incubating;

/** Serialized representation of a completed job result for persistence. */
@Incubating
public record SerializedJobResult(String json, String type) {

  public static SerializedJobResult empty() {
    return new SerializedJobResult(null, null);
  }
}
