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
   * Determines whether a class is allowed based on its name.
   *
   * @param className the fully qualified name of the class to check
   * @return {@code true} if the specified class is allowed; otherwise, {@code false}
   */
  boolean isAllowed(String className);
}
