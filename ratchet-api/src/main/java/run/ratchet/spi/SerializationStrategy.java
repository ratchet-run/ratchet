package run.ratchet.spi;

import run.ratchet.api.Incubating;

/**
 * Defines a strategy for serializing and deserializing objects. This interface allows for the
 * implementation of different serialization mechanisms, enabling flexibility in how objects are
 * converted to byte arrays and reconstructed from them.
 *
 * <p>Implementations of this interface must provide thread-safe behavior.
 *
 * <p>The interface defines two key operations: 1. {@code serialize}: Converts an object into a byte
 * array. 2. {@code deserialize}: Reconstructs an object from a byte array, with the type specified.
 *
 * <p>Marked {@link Incubating} — the serialization contract may evolve.
 */
@Incubating
public interface SerializationStrategy {

  /**
   * Serializes the given object into a byte array. Implementations of this method must ensure the
   * object can be accurately and reliably converted into a byte array representation, which can
   * later be deserialized back into the original object, maintaining data integrity.
   *
   * @param obj the object to be serialized; must not be null
   * @return a byte array representing the serialized form of the object
   * @throws IllegalArgumentException if the object cannot be serialized
   */
  byte[] serialize(Object obj);

  /**
   * Deserializes the given byte array into an object of the specified type. The method reconstructs
   * the object state from the provided byte array, ensuring the resulting object matches the type
   * specified.
   *
   * @param <T> the type of the object to be deserialized
   * @param data the byte array representing the serialized form of the object; must not be null
   * @param type the class of the object to be deserialized; must not be null
   * @return an object of type {@code T} reconstructed from the given byte array
   * @throws IllegalArgumentException if the input data is null, the type is null, or the
   *     deserialization process fails
   */
  <T> T deserialize(byte[] data, Class<T> type);
}
