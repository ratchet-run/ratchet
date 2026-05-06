package run.ratchet.spi;

import java.util.List;
import run.ratchet.api.Incubating;

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
