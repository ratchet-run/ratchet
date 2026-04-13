package run.ratchet.spi;

import run.ratchet.api.Incubating;

/** Converts objects to and from byte arrays. Implementations must be thread-safe. */
@Incubating
public interface SerializationStrategy {

  /**
   * Serializes the given object into a byte array.
   *
   * @throws IllegalArgumentException if the object cannot be serialized
   */
  byte[] serialize(Object obj);

  /**
   * Deserializes a byte array into an instance of the specified type.
   *
   * @throws IllegalArgumentException if deserialization fails
   */
  <T> T deserialize(byte[] data, Class<T> type);
}
