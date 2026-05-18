package run.ratchet.spi;

import run.ratchet.api.Incubating;

/**
 * Policy for controlling which classes may be deserialized during job payload restoration.
 *
 * <p>This interface is marked {@link Incubating} — the allow/deny API may be extended in future
 * releases without following the normal deprecation cycle.
 */
@Incubating
public interface ClassPolicy {

  /**
   * Returns whether a class name may be used during payload restoration or invocation.
   *
   * @param className fully qualified binary class name; {@code null} or blank input must be denied
   * @return {@code true} to allow the class, {@code false} to deny it
   */
  boolean isAllowed(String className);
}
