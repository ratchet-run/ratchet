package run.ratchet.spi;

/** Policy for controlling which classes may be deserialized during job payload restoration. */
public interface ClassPolicy {

  boolean isAllowed(String className);
}
