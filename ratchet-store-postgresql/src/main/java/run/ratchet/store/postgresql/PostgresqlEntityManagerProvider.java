package run.ratchet.store.postgresql;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import run.ratchet.store.spi.RatchetEntityManagerProvider;

/** Default PostgreSQL EntityManager provider using the deployment's unnamed persistence context. */
@ApplicationScoped
class PostgresqlEntityManagerProvider implements RatchetEntityManagerProvider {

  @PersistenceContext private EntityManager em;

  @Override
  public EntityManager getEntityManager() {
    return em;
  }
}
