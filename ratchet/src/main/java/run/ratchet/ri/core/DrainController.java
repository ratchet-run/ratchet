package run.ratchet.ri.core;

/**
 * SPI for graceful-shutdown drain control. Default implementation: {@link
 * run.ratchet.ri.core.internal.DefaultDrainController}.
 *
 * @apiNote Framework SPI consumed by ri.cdi.RatchetLifecycle and by ratchet-testsuite integration
 *     tests. Applications must not implement this interface.
 */
public interface DrainController {

  boolean isDraining();

  void setDraining(boolean value);
}
