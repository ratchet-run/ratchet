package run.ratchet.spi;

/** Strategy for serializing and deserializing job payloads. */
public interface SerializationStrategy {

  byte[] serialize(Object obj);

  <T> T deserialize(byte[] data, Class<T> type);
}
