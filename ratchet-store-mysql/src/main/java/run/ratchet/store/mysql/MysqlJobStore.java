package run.ratchet.store.mysql;

import run.ratchet.spi.MetricsCollector;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;

/**
 * Public CDI entry point for the MySQL store.
 *
 * <p>The implementation lives in {@link MysqlJobStoreDelegate}; keeping this facade small preserves
 * the public type adopters inject while allowing the internal store to be decomposed further
 * without changing that entry point.
 */
@ApplicationScoped
@Transactional
public class MysqlJobStore extends MysqlJobStoreDelegate {

  public MysqlJobStore() {}

  /** Test-only constructor: bypasses CDI to wire EM + metrics directly. */
  MysqlJobStore(EntityManager em, MetricsCollector metricsCollector) {
    super(em, metricsCollector);
  }
}
