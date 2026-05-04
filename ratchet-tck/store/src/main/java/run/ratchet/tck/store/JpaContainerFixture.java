package run.ratchet.tck.store;

import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.api.JobStatus;
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

/**
 * Shared Testcontainers + JPA fixture for store TCK suites.
 *
 * <p>Subclasses supply a started Testcontainers JDBC container, a map of provider-specific JPA
 * properties (e.g. {@code hibernate.dialect} for Hibernate-backed subclasses), a persistence-unit
 * name (PU lives in the dialect module's {@code src/test/resources/META-INF/persistence.xml}), and
 * a factory for the concrete {@link JobStore} given a plain {@link EntityManager}. This base class
 * intentionally avoids a compile-time Testcontainers type so the TCK jar can be a clean JPMS
 * module; concrete store test modules own their Testcontainers dependencies.
 *
 * <p>This base is JPA-provider agnostic: it only sets the standard {@code
 * jakarta.persistence.jdbc.*} overrides. Any Hibernate- or EclipseLink-specific keys are the
 * subclass's responsibility, supplied via {@link #jpaProperties()}.
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
    Object container = container();
    if (!containerBoolean(container, "isRunning")) {
      throw new IllegalStateException(
          "Subclass must start the Testcontainers container before constructing the fixture");
    }
    String cacheKey = persistenceUnitName() + "@" + containerString(container, "getJdbcUrl");
    this.emf = EMF_CACHE.computeIfAbsent(cacheKey, k -> createEntityManagerFactory(container));
    this.threadEm = ThreadLocal.withInitial(emf::createEntityManager);
    this.emProxy = newThreadLocalEmProxy();
    JobStore delegate = createStore(this.emProxy, NO_OP_METRICS);
    this.storeProxy = newTransactionalStoreProxy(delegate);
  }

  private static boolean containerBoolean(Object container, String methodName) {
    Object value = invokeContainerMethod(container, methodName);
    if (value instanceof Boolean result) {
      return result;
    }
    throw new IllegalStateException("Container method " + methodName + " did not return boolean");
  }

  private static String containerString(Object container, String methodName) {
    Object value = invokeContainerMethod(container, methodName);
    if (value instanceof String result) {
      return result;
    }
    throw new IllegalStateException("Container method " + methodName + " did not return String");
  }

  private static Object invokeContainerMethod(Object container, String methodName) {
    try {
      return container.getClass().getMethod(methodName).invoke(container);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(
          "Expected Testcontainers JDBC container method " + methodName, e);
    }
  }

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

  /** Started Testcontainers JDBC container. Subclass owns its lifecycle. */
  protected abstract Object container();

  /**
   * Provider-specific JPA properties merged into the EMF override map. Subclasses supply keys like
   * {@code hibernate.dialect} or {@code eclipselink.target-database}. The base reserves {@code
   * jakarta.persistence.jdbc.*} keys for JDBC connection wiring; subclass keys take precedence over
   * any base value if collisions occur.
   */
  protected abstract Map<String, Object> jpaProperties();

  /** Persistence-unit name as declared in the dialect module's {@code persistence.xml}. */
  protected abstract String persistenceUnitName();

  /** Instantiate the concrete store with a plain EM and a no-op metrics collector. */
  protected abstract JobStore createStore(EntityManager em, MetricsCollector metrics);

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

  private EntityManagerFactory createEntityManagerFactory(Object container) {
    Map<String, Object> overrides = new HashMap<>();
    overrides.put("jakarta.persistence.jdbc.url", containerString(container, "getJdbcUrl"));
    overrides.put("jakarta.persistence.jdbc.user", containerString(container, "getUsername"));
    overrides.put("jakarta.persistence.jdbc.password", containerString(container, "getPassword"));
    overrides.put(
        "jakarta.persistence.jdbc.driver", containerString(container, "getDriverClassName"));
    overrides.putAll(jpaProperties());
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
                  // Production @Transactional + container-managed PersistenceContext resets the
                  // L1 cache between method invocations. The thread-local EM used by this proxy
                  // would otherwise leak stale cached entities across method boundaries, so
                  // subsequent em.find() calls would miss server-side UPDATEs issued via native
                  // SQL. Clear after commit to match production semantics.
                  em.clear();
                }
                return result;
              } catch (InvocationTargetException ite) {
                if (owner && tx.isActive()) {
                  tx.rollback();
                  em.clear();
                }
                throw ite.getCause();
              } catch (Throwable t) {
                if (owner && tx.isActive()) {
                  tx.rollback();
                  em.clear();
                }
                throw t;
              }
            });
  }

  /** No-op metrics collector reused by every JPA fixture instance. */
  private static final class NoOpMetricsCollector implements MetricsCollector {
    @Override
    public void jobStarted(UUID jobId, JobType type, JobPriority priority) {}

    @Override
    public void jobCompleted(UUID jobId, JobType type, long executionTimeMs) {}

    @Override
    public void jobFailed(UUID jobId, JobType type, Throwable cause, int attempt) {}

    @Override
    public void successFinalizationRetried(UUID jobId, JobType type) {}

    @Override
    public void successFinalizationMinimal(UUID jobId, JobType type) {}

    @Override
    public void successFinalizationStuck(UUID jobId, JobType type) {}

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
