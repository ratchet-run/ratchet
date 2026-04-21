package run.ratchet.store.spi;

import run.ratchet.api.Incubating;
import jakarta.persistence.EntityManager;

/**
 * Provides the {@link EntityManager} used by SQL store implementations.
 *
 * <p>Applications with multiple persistence units can override this SPI with a CDI alternative that
 * injects the desired {@code @PersistenceContext(unitName = "...")}. The store modules' default
 * providers keep the unnamed {@code @PersistenceContext} behavior.
 */
@Incubating
public interface RatchetEntityManagerProvider {

  EntityManager getEntityManager();
}
