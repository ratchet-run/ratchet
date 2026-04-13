package run.ratchet.api;

import java.io.Serializable;

/**
 * Serializable {@link java.util.function.Consumer} variant that allows checked exceptions.
 *
 * @param <T> the type of the input to the operation
 */
@FunctionalInterface
@SuppressWarnings("java:S112")
// Generic exception intentional for streaming batch functional interface
public interface SerializableCheckedConsumer<T> extends Serializable {

  void accept(T t) throws Exception;
}
