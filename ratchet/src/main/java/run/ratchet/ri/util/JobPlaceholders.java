package run.ratchet.ri.util;

/** No-op methods for coordination-only jobs (e.g. batch parents). */
public final class JobPlaceholders {

  private JobPlaceholders() {
    throw new UnsupportedOperationException("Utility class - do not instantiate");
  }

  public static void noop() {}
}
