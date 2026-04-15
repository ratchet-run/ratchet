package run.ratchet.spi;

import run.ratchet.api.Incubating;
import java.util.List;

/** Serializable description of the target method Ratchet should invoke for a job. */
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
