package run.ratchet.spi;

import java.util.List;
import run.ratchet.api.Incubating;

/**
 * Serializable description of the target method Ratchet should invoke for a job.
 *
 * @param targetClass fully qualified target class name
 * @param methodName target method name
 * @param methodDescriptor JVM method descriptor
 * @param staticMethod whether the invocation targets a static method
 * @param arguments persisted invocation arguments; {@code null} is normalized to an empty list
 */
@Incubating
public record JobInvocation(
    String targetClass,
    String methodName,
    String methodDescriptor,
    boolean staticMethod,
    List<Object> arguments) {

  public JobInvocation {
    arguments = arguments == null ? List.of() : List.copyOf(arguments);
  }
}
