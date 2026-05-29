package run.ratchet.ri.core;

/** Shared utilities for scheduler classes. */
public final class SchedulerUtils {

  private SchedulerUtils() {}

  /** Returns true if the throwable (or any cause) indicates the CDI context has been torn down. */
  public static boolean isCdiContextGone(Throwable t) {
    Throwable current = t;
    while (current != null) {
      String name = current.getClass().getName();
      if (name.contains("ContextNotActiveException") || name.contains("ContextNotAliveException")) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }
}
