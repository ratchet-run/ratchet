package run.ratchet.api;

import java.io.Serializable;

@FunctionalInterface
@SuppressWarnings("java:S112")
// Generic exception intentional for streaming batch functional interface
public interface SerializableCheckedConsumer<T> extends Serializable {

  void accept(T t) throws Exception;
}
