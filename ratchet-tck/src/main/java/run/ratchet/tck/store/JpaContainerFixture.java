package run.ratchet.tck.store;

import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.store.entity.JobStatus;
import run.ratchet.store.spi.JobStore;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.EntityTransaction;
import jakarta.persistence.Persistence;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.testcontainers.containers.JdbcDatabaseContainer;

/**
 * Shared Testcontainers + Hibernate fixture for JPA-backed TCK suites.
 *
 * <p>Subclasses supply a started {@link JdbcDatabaseContainer}, a Hibernate dialect name, a
 * persistence-unit name (PU lives in the dialect module's {@code src/test/resources/META-INF/
 * persistence.xml}), and a factory for the concrete {@link JobStore} given a plain {@link
 * EntityManager}.
 *
 * <p>Production stores are CDI-managed with {@code @Transactional} and per-thread
 * {@code @PersistenceContext} injection. Tests have neither. This fixture closes the gap with two
 * JDK proxies:
 *
 * <ul>
 *   <li>A thread-local {@link EntityManager} proxy — every call pulls the calling thread's own EM
 *       from a shared {@link EntityManagerFactory}, so concurrent contract tests ({@code
 *       ConcurrentTestRunner}) do not share a non-thread-safe EM.
 *   <li>A {@link JobStore} proxy — every store method call begins a new {@link EntityTransaction}
 *       if one is not already active, commits on success, rolls back on failure. This emulates the
 *       coarse-grained, one-tx-per-method contract that the production {@code @Transactional}
 *       class-level annotation provides.
 * </ul>
 *
 * <p>Cleanup between tests goes through {@link #cleanupStore()}, which the subclass implements by
 * truncating tables in dependency order.
 */
public abstract class JpaContainerFixture implements JobStoreContractFixture {

  private static final Map<String, EntityManagerFactory> EMF_CACHE = new ConcurrentHashMap<>();
  private static final MetricsCollector NO_OP_METRICS = new NoOpMetricsCollector();

  private final EntityManagerFactory emf;
  private final ThreadLocal<EntityManager> threadEm;
  private final EntityManager emProxy;
  private final JobStore storeProxy;

  protected JpaContainerFixture() {
    JdbcDatabaseContainer<?> container = container();
    if (!container.isRunning()) {
      throw new IllegalStateException(
          "Subclass must start the Testcontainers container before constructing the fixture");
    }
    String cacheKey = persistenceUnitName() + "@" + container.getJdbcUrl();
    this.emf = EMF_CACHE.computeIfAbsent(cacheKey, k -> createEntityManagerFactory(container));
    this.threadEm = ThreadLocal.withInitial(emf::createEntityManager);
    this.emProxy = newThreadLocalEmProxy();
    JobStore delegate = createStore(this.emProxy, NO_OP_METRICS);
    this.storeProxy = newTransactionalStoreProxy(delegate);
  }

  /** Started Testcontainers JDBC container. Subclass owns its lifecycle. */
  protected abstract JdbcDatabaseContainer<?> container();

  /** Fully-qualified Hibernate dialect class name. */
  protected abstract String hibernateDialect();

  /** Persistence-unit name as declared in the dialect module's {@code persistence.xml}. */
  protected abstract String persistenceUnitName();

  /** Instantiate the concrete store with a plain EM and a no-op metrics collector. */
  protected abstract JobStore createStore(EntityManager em, MetricsCollector metrics);

  /** Truncate/delete all rows across every ratchet table, in dependency order. */
  @Override
  public abstract void cleanupStore();

  @Override
  public final JobStore store() {
    return storeProxy;
  }

  @Override
  public JobEntity newPendingJob() {
    JobEntity job = new JobEntity();
    job.setStatus(JobStatus.PENDING);
    job.setScheduledTime(Instant.now());
    job.setJobType(JobExecutionType.SINGLE);
    job.setPriority(JobPriority.NORMAL);
    job.setBackoffPolicy(BackoffPolicy.NONE);
    job.setIdempotencyKey(UUID.randomUUID().toString());
    job.setPayload(new JobPayload("com.example.TestJob", "execute", "()V", false, List.of()));
    return job;
  }

  @Override
  public JobEntity newBatchParentJob() {
    JobEntity job = new JobEntity();
    job.setStatus(JobStatus.PENDING);
    job.setScheduledTime(Instant.now());
    job.setJobType(JobExecutionType.BATCH_PARENT);
    job.setPriority(JobPriority.NORMAL);
    job.setBackoffPolicy(BackoffPolicy.NONE);
    job.setIdempotencyKey(UUID.randomUUID().toString());
    job.setPayload(new JobPayload("com.example.BatchJob", "execute", "()V", false, List.of()));
    return job;
  }

  /** Run a unit of work inside a JPA transaction on the calling thread's EM. */
  protected final void runInTransaction(Runnable work) {
    EntityManager em = threadEm.get();
    EntityTransaction tx = em.getTransaction();
    boolean owner = !tx.isActive();
    if (owner) {
      tx.begin();
    }
    try {
      work.run();
      if (owner) {
        tx.commit();
      }
    } catch (RuntimeException e) {
      if (owner && tx.isActive()) {
        tx.rollback();
      }
      throw e;
    }
  }

  /** Execute raw SQL on a fresh JDBC connection from the container. For cleanupStore(). */
  protected final void executeNativeSql(String sql) {
    runInTransaction(() -> threadEm.get().createNativeQuery(sql).executeUpdate());
  }

  private EntityManagerFactory createEntityManagerFactory(JdbcDatabaseContainer<?> container) {
    Map<String, Object> overrides = new HashMap<>();
    overrides.put("jakarta.persistence.jdbc.url", container.getJdbcUrl());
    overrides.put("jakarta.persistence.jdbc.user", container.getUsername());
    overrides.put("jakarta.persistence.jdbc.password", container.getPassword());
    overrides.put("jakarta.persistence.jdbc.driver", container.getDriverClassName());
    overrides.put("hibernate.dialect", hibernateDialect());
    overrides.put("hibernate.hbm2ddl.auto", "none");
    overrides.put("hibernate.show_sql", "false");
    overrides.put("hibernate.format_sql", "false");
    overrides.put("hibernate.connection.provider_disables_autocommit", "false");
    return Persistence.createEntityManagerFactory(persistenceUnitName(), overrides);
  }

  private EntityManager newThreadLocalEmProxy() {
    return (EntityManager)
        Proxy.newProxyInstance(
            EntityManager.class.getClassLoader(),
            new Class<?>[] {EntityManager.class},
            (proxy, method, args) -> {
              EntityManager target = threadEm.get();
              try {
                return method.invoke(target, args);
              } catch (InvocationTargetException ite) {
                throw ite.getCause();
              }
            });
  }

  private JobStore newTransactionalStoreProxy(JobStore delegate) {
    return (JobStore)
        Proxy.newProxyInstance(
            JobStore.class.getClassLoader(),
            new Class<?>[] {JobStore.class},
            (proxy, method, args) -> {
              EntityManager em = threadEm.get();
              EntityTransaction tx = em.getTransaction();
              boolean owner = !tx.isActive();
              if (owner) {
                tx.begin();
              }
              try {
                Object result = method.invoke(delegate, args);
                if (owner && tx.isActive()) {
                  tx.commit();
                }
                return result;
              } catch (InvocationTargetException ite) {
                if (owner && tx.isActive()) {
                  tx.rollback();
                }
                throw ite.getCause();
              } catch (Throwable t) {
                if (owner && tx.isActive()) {
                  tx.rollback();
                }
                throw t;
              }
            });
  }

  /** No-op metrics collector reused by every JPA fixture instance. */
  private static final class NoOpMetricsCollector implements MetricsCollector {
    @Override
    public void jobStarted(long jobId, JobType type, JobPriority priority) {}

    @Override
    public void jobCompleted(long jobId, JobType type, long executionTimeMs) {}

    @Override
    public void jobFailed(long jobId, JobType type, Throwable cause, int attempt) {}

    @Override
    public void successFinalizationRetried(long jobId, JobType type) {}

    @Override
    public void successFinalizationMinimal(long jobId, JobType type) {}

    @Override
    public void successFinalizationStuck(long jobId, JobType type) {}

    @Override
    public void claimTransientFailure(String executionType) {}

    @Override
    public void jobsClaimed(String executionType, int claimedCount) {}

    @Override
    public void gateRejected(String executionType, String gateStatus) {}

    @Override
    public void localWakeup(String source) {}

    @Override
    public void clusterWakeupPublished(String transport, String outcome) {}

    @Override
    public void clusterWakeupReceived(String transport, String outcome) {}
  }
}
