/*
 * Copyright 2026 Ratchet Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package run.ratchet.tck.store;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.EntityTransaction;
import jakarta.persistence.Persistence;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobStatus;
import run.ratchet.api.JobType;
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.store.spi.ArchiveStore;
import run.ratchet.store.spi.BatchStore;
import run.ratchet.store.spi.JobAnalyticsStore;
import run.ratchet.store.spi.JobAuditStore;
import run.ratchet.store.spi.JobExtensionStore;
import run.ratchet.store.spi.JobQueryStore;
import run.ratchet.store.spi.JobStore;
import run.ratchet.store.spi.LockStore;
import run.ratchet.store.spi.RecurringJobStore;
import run.ratchet.store.spi.ResourcePermitStore;
import run.ratchet.store.spi.SignalStore;
import run.ratchet.store.spi.WorkflowConditionStore;

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
  // A method that fails with a transient store exception (e.g. a SQL Server deadlock victim) is
  // retried on a fresh transaction, emulating a production transient-retry interceptor.
  private static final int MAX_TRANSACTION_ATTEMPTS = 5;

  private final EntityManagerFactory emf;
  private final ThreadLocal<EntityManager> threadEm;
  private final EntityManager emProxy;
  private final JobStore storeProxy;

  private static Instant dueScheduledTime() {
    // DB-backed claim paths compare scheduled_time to the database clock, not the JVM clock.
    return Instant.now().minusSeconds(1);
  }

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

  /** Open a raw JDBC connection to the same container used by this fixture. */
  public final Connection openConnection() throws SQLException {
    Object container = container();
    return DriverManager.getConnection(
        containerString(container, "getJdbcUrl"),
        containerString(container, "getUsername"),
        containerString(container, "getPassword"));
  }

  @Override
  public JobEntity newPendingJob() {
    JobEntity job = new JobEntity();
    job.setStatus(JobStatus.PENDING);
    job.setScheduledTime(dueScheduledTime());
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
    job.setScheduledTime(dueScheduledTime());
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

  /**
   * Run a unit of work inside a JPA transaction on the calling thread's EM. Public so that
   * concurrency contracts living in concrete store-test modules (different package than this
   * fixture) can hold a transaction open across a barrier — see {@link
   * AbstractJpaRecurringClaimConcurrencyContract}.
   */
  public final void runInTransaction(Runnable work) {
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
        em.clear();
      }
    } catch (RuntimeException e) {
      if (owner && tx.isActive()) {
        tx.rollback();
        em.clear();
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
            new Class<?>[] {
              JobStore.class,
              RecurringJobStore.class,
              BatchStore.class,
              WorkflowConditionStore.class,
              SignalStore.class,
              ResourcePermitStore.class,
              LockStore.class,
              ArchiveStore.class,
              JobQueryStore.class,
              JobAnalyticsStore.class,
              JobAuditStore.class,
              JobExtensionStore.class
            },
            (proxy, method, args) -> {
              // capability() must return a view that still routes through this transactional proxy,
              // not the bare delegate — otherwise a capability contract would call the underlying
              // store outside any transaction. Gate on the delegate's real advertisement so a
              // core-only store still reports a capability as absent.
              if ("capability".equals(method.getName()) && args != null && args.length == 1) {
                Class<?> type = (Class<?>) args[0];
                Object delegated = method.invoke(delegate, args);
                if (delegated instanceof java.util.Optional<?> opt
                    && opt.isPresent()
                    && type.isInstance(proxy)) {
                  return java.util.Optional.of(type.cast(proxy));
                }
                return java.util.Optional.empty();
              }
              EntityManager em = threadEm.get();
              EntityTransaction tx = em.getTransaction();
              boolean owner = !tx.isActive();
              // A nested (non-owner) call runs inside the caller's transaction and cannot retry.
              if (!owner) {
                return invokeUnwrapping(delegate, method, args);
              }
              // Emulate a production @Transactional method behind a transient-retry interceptor:
              // each attempt is its own transaction, and a transient store failure (a SQL Server
              // deadlock victim is the realistic case — lock-based engines can deadlock where the
              // MVCC stores never do) is retried on a fresh transaction. MVCC stores never throw
              // this, so the retry loop is a single pass for them.
              RatchetTransientStoreException lastTransient = null;
              for (int attempt = 1; attempt <= MAX_TRANSACTION_ATTEMPTS; attempt++) {
                tx.begin();
                try {
                  Object result = invokeUnwrapping(delegate, method, args);
                  tx.commit();
                  // Production @Transactional + container-managed PersistenceContext resets the
                  // L1 cache between method invocations. The thread-local EM used by this proxy
                  // would otherwise leak stale cached entities across method boundaries, so
                  // subsequent em.find() calls would miss server-side UPDATEs issued via native
                  // SQL. Clear after commit to match production semantics.
                  em.clear();
                  return result;
                } catch (RatchetTransientStoreException transient_) {
                  if (tx.isActive()) {
                    tx.rollback();
                  }
                  em.clear();
                  lastTransient = transient_;
                } catch (Throwable t) {
                  if (tx.isActive()) {
                    tx.rollback();
                  }
                  em.clear();
                  throw t;
                }
              }
              throw lastTransient;
            });
  }

  private static Object invokeUnwrapping(
      Object delegate, java.lang.reflect.Method method, Object[] args) throws Throwable {
    try {
      return method.invoke(delegate, args);
    } catch (InvocationTargetException ite) {
      throw ite.getCause();
    }
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
