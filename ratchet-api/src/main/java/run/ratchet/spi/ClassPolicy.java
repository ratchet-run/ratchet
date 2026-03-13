package run.ratchet.spi;

/** Policy for controlling which classes may be deserialized during job payload restoration. */
public interface ClassPolicy {

  /**
   * Determines whether a class is allowed based on its name.
   *
   * @param className the fully qualified name of the class to check
   * @return {@code true} if the specified class is allowed; otherwise, {@code false}
   */
  boolean isAllowed(String className);
}
