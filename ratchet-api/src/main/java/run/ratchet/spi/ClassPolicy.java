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

  boolean isAllowed(String className);
}
