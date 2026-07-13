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
package run.ratchet.compatibility;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityTransaction;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import org.hibernate.SessionFactory;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.testcontainers.containers.JdbcDatabaseContainer;

/** One JDBC store runtime whose Ratchet classes are isolated from every other version. */
final class IsolatedJdbcRuntime implements AutoCloseable {

  private static final String API_JAR = "ratchet-api-0.1.1.jar";
  private static final String STORE_CORE_JAR = "ratchet-store-core-0.1.1.jar";

  private final RatchetClassLoader classLoader;
  private final StandardServiceRegistry serviceRegistry;
  private final SessionFactory sessionFactory;
  private final EntityManager entityManager;
  private final Object store;
  private final Class<?> jobStoreClass;
  private final Class<?> jobEntityClass;
  private final Class<?> jobPayloadClass;
  private final String codeSource;

  static IsolatedJdbcRuntime published011(
      StoreDialect dialect, Path runtimeDirectory, JdbcDatabaseContainer<?> container)
      throws Exception {
    URL[] urls =
        new URL[] {
          required(runtimeDirectory.resolve(API_JAR)).toUri().toURL(),
          required(runtimeDirectory.resolve(STORE_CORE_JAR)).toUri().toURL(),
          required(runtimeDirectory.resolve(dialect.releasedJarName())).toUri().toURL()
        };
    return new IsolatedJdbcRuntime(dialect, urls, container);
  }

  static IsolatedJdbcRuntime currentSnapshot(
      StoreDialect dialect, Path reactorRoot, JdbcDatabaseContainer<?> container) throws Exception {
    URL[] urls =
        new URL[] {
          required(reactorRoot.resolve("ratchet-api/target/classes")).toUri().toURL(),
          required(reactorRoot.resolve("stores/ratchet-store-core/target/classes")).toUri().toURL(),
          required(reactorRoot.resolve(dialect.currentClasses())).toUri().toURL()
        };
    return new IsolatedJdbcRuntime(dialect, urls, container);
  }

  private IsolatedJdbcRuntime(
      StoreDialect dialect, URL[] runtimeUrls, JdbcDatabaseContainer<?> container)
      throws Exception {
    this.classLoader = new RatchetClassLoader(runtimeUrls, getClass().getClassLoader());
    ClassLoader previous = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(classLoader);
      this.jobEntityClass = classLoader.loadClass("run.ratchet.store.entity.JobEntity");
      this.jobPayloadClass = classLoader.loadClass("run.ratchet.store.entity.JobPayload");
      this.jobStoreClass = classLoader.loadClass("run.ratchet.store.spi.JobStore");

      this.serviceRegistry =
          new StandardServiceRegistryBuilder()
              .applySetting("jakarta.persistence.jdbc.url", container.getJdbcUrl())
              .applySetting("jakarta.persistence.jdbc.user", container.getUsername())
              .applySetting("jakarta.persistence.jdbc.password", container.getPassword())
              .applySetting("jakarta.persistence.jdbc.driver", container.getDriverClassName())
              .applySetting("hibernate.hbm2ddl.auto", "none")
              .applySetting("hibernate.show_sql", "false")
              .applySetting("hibernate.format_sql", "false")
              .applySetting("hibernate.connection.provider_disables_autocommit", "false")
              .applySetting("hibernate.connection.isolation", "2")
              .build();
      this.sessionFactory =
          new MetadataSources(serviceRegistry)
              .addAnnotatedClass(jobEntityClass)
              .buildMetadata()
              .buildSessionFactory();
      this.entityManager = sessionFactory.createEntityManager();
      this.store = createStore(dialect);
      Class<?> storeType = classLoader.loadClass(dialect.storeInterface());
      this.codeSource = normalizedCodeSource(storeType);
    } finally {
      Thread.currentThread().setContextClassLoader(previous);
    }
  }

  String codeSource() {
    return codeSource;
  }

  Class<?> jobEntityClass() {
    return jobEntityClass;
  }

  void createPendingJob(UUID jobId, String businessKey, String payloadArgument) throws Exception {
    withRuntimeClassLoader(
        () -> {
          Object job = jobEntityClass.getConstructor().newInstance();
          invokeSetter(job, "setId", UUID.class, jobId);
          invokeSetter(job, "setScheduledTime", Instant.class, Instant.now().minusSeconds(1));

          Class<?> jobTypeClass =
              classLoader.loadClass("run.ratchet.store.entity.JobExecutionType");
          invokeSetter(job, "setJobType", jobTypeClass, enumValue(jobTypeClass, "SINGLE"));

          Class<?> priorityClass = classLoader.loadClass("run.ratchet.api.JobPriority");
          invokeSetter(job, "setPriority", priorityClass, enumValue(priorityClass, "NORMAL"));

          Class<?> backoffClass = classLoader.loadClass("run.ratchet.api.BackoffPolicy");
          invokeSetter(job, "setBackoffPolicy", backoffClass, enumValue(backoffClass, "NONE"));
          invokeSetter(job, "setIdempotencyKey", String.class, jobId.toString());
          invokeSetter(job, "setBusinessKey", String.class, businessKey);

          Constructor<?> payloadConstructor =
              jobPayloadClass.getConstructor(
                  String.class, String.class, String.class, boolean.class, List.class);
          Object payload =
              payloadConstructor.newInstance(
                  "com.example.CompatibilityJob",
                  "execute",
                  "(Ljava/lang/String;)V",
                  true,
                  List.of(payloadArgument));
          invokeSetter(job, "setPayload", jobPayloadClass, payload);

          transaction(() -> invokeStore("create", new Class<?>[] {jobEntityClass}, job));
          return null;
        });
  }

  void consumeJob(
      UUID jobId,
      String expectedBusinessKey,
      String expectedPayloadArgument,
      String nodeId,
      String result)
      throws Exception {
    withRuntimeClassLoader(
        () -> {
          Object pending = findRequired(jobId);
          requireEquals("PENDING", enumName(invokeNoArgs(pending, "getStatus")), "job status");
          requireEquals(
              expectedBusinessKey, invokeNoArgs(pending, "getBusinessKey"), "business key");
          assertPayload(pending, expectedPayloadArgument);

          Object pickedUp =
              transaction(
                  () ->
                      invokeStore(
                          "tryPickUpJob",
                          new Class<?>[] {UUID.class, String.class},
                          jobId,
                          nodeId));
          requireEquals(Boolean.TRUE, pickedUp, "pickup result");

          Object running = findRequired(jobId);
          requireEquals("RUNNING", enumName(invokeNoArgs(running, "getStatus")), "job status");
          requireEquals(nodeId, invokeNoArgs(running, "getPickedBy"), "claim owner");

          Instant start = Instant.now().minusMillis(10);
          Instant end = Instant.now();
          Object succeeded =
              transaction(
                  () ->
                      invokeStore(
                          "markJobSucceeded",
                          new Class<?>[] {
                            UUID.class,
                            String.class,
                            String.class,
                            Instant.class,
                            Instant.class,
                            Long.class,
                            Long.class
                          },
                          jobId,
                          jsonString(result),
                          String.class.getName(),
                          start,
                          end,
                          10L,
                          5L));
          requireEquals(Boolean.TRUE, succeeded, "success transition");
          return null;
        });
  }

  void assertSucceeded(UUID jobId, String expectedPayloadArgument, String expectedResult)
      throws Exception {
    withRuntimeClassLoader(
        () -> {
          Object job = findRequired(jobId);
          requireEquals("SUCCEEDED", enumName(invokeNoArgs(job, "getStatus")), "job status");
          assertPayload(job, expectedPayloadArgument);
          requireEquals(
              jsonString(expectedResult), invokeNoArgs(job, "getJobResult"), "job result");
          requireEquals(
              String.class.getName(), invokeNoArgs(job, "getResultType"), "job result type");
          return null;
        });
  }

  private Object createStore(StoreDialect dialect) throws Exception {
    Class<?> entityManagerProvider =
        classLoader.loadClass("run.ratchet.store.spi.RatchetEntityManagerProvider");
    Object provider =
        Proxy.newProxyInstance(
            classLoader,
            new Class<?>[] {entityManagerProvider},
            (proxy, method, args) -> entityManager);

    Class<?> metricsCollector = classLoader.loadClass("run.ratchet.spi.MetricsCollector");
    Object metrics =
        Proxy.newProxyInstance(
            classLoader,
            new Class<?>[] {metricsCollector},
            (proxy, method, args) -> defaultValue(method.getReturnType()));

    Class<?> optionsClass = classLoader.loadClass("run.ratchet.api.RatchetOptions");
    Object options = optionsClass.getMethod("defaults").invoke(null);
    Class<?> implementation = classLoader.loadClass(dialect.storeImplementation());
    Constructor<?> constructor =
        implementation.getDeclaredConstructor(
            entityManagerProvider, metricsCollector, optionsClass);
    constructor.setAccessible(true);
    Object result = constructor.newInstance(provider, metrics, options);
    Method initialize = implementation.getDeclaredMethod("checkIsolationLevel");
    initialize.setAccessible(true);
    invoke(result, initialize);
    return result;
  }

  private void assertPayload(Object job, String expectedArgument) throws Exception {
    Object payload = invokeNoArgs(job, "getPayload");
    requireEquals(
        "com.example.CompatibilityJob", invokeNoArgs(payload, "target"), "payload target");
    requireEquals("execute", invokeNoArgs(payload, "method"), "payload method");
    requireEquals(
        "(Ljava/lang/String;)V", invokeNoArgs(payload, "methodDescriptor"), "payload descriptor");
    requireEquals(Boolean.TRUE, invokeNoArgs(payload, "isStatic"), "payload static flag");
    requireEquals(List.of(expectedArgument), invokeNoArgs(payload, "args"), "payload arguments");
  }

  private Object findRequired(UUID jobId) throws Exception {
    Object result = transaction(() -> invokeStore("findById", new Class<?>[] {UUID.class}, jobId));
    if (result instanceof Optional<?> optional) {
      return optional.orElseThrow(() -> new AssertionError("Job was not found: " + jobId));
    }
    throw new AssertionError("findById did not return Optional: " + result);
  }

  private Object invokeStore(String methodName, Class<?>[] parameterTypes, Object... args)
      throws Exception {
    Method method = jobStoreClass.getMethod(methodName, parameterTypes);
    return invoke(store, method, args);
  }

  private Object transaction(CheckedSupplier work) throws Exception {
    EntityTransaction transaction = entityManager.getTransaction();
    transaction.begin();
    try {
      Object result = work.get();
      transaction.commit();
      entityManager.clear();
      return result;
    } catch (Exception | Error failure) {
      if (transaction.isActive()) {
        transaction.rollback();
      }
      entityManager.clear();
      throw failure;
    }
  }

  private <T> T withRuntimeClassLoader(CheckedOperation<T> operation) throws Exception {
    ClassLoader previous = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(classLoader);
    try {
      return operation.run();
    } finally {
      Thread.currentThread().setContextClassLoader(previous);
    }
  }

  private static void invokeSetter(Object target, String name, Class<?> type, Object value)
      throws Exception {
    invoke(target, target.getClass().getMethod(name, type), value);
  }

  private static Object invokeNoArgs(Object target, String name) throws Exception {
    return invoke(target, target.getClass().getMethod(name));
  }

  private static Object invoke(Object target, Method method, Object... args) throws Exception {
    try {
      return method.invoke(target, args);
    } catch (InvocationTargetException e) {
      Throwable cause = e.getCause();
      if (cause instanceof Exception exception) {
        throw exception;
      }
      if (cause instanceof Error error) {
        throw error;
      }
      throw e;
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static Object enumValue(Class<?> enumClass, String name) {
    return Enum.valueOf((Class<? extends Enum>) enumClass.asSubclass(Enum.class), name);
  }

  private static String enumName(Object value) {
    return value instanceof Enum<?> enumeration ? enumeration.name() : String.valueOf(value);
  }

  private static Object defaultValue(Class<?> type) {
    if (!type.isPrimitive() || type == void.class) {
      return null;
    }
    if (type == boolean.class) {
      return false;
    }
    if (type == char.class) {
      return '\0';
    }
    if (type == byte.class) {
      return (byte) 0;
    }
    if (type == short.class) {
      return (short) 0;
    }
    if (type == int.class) {
      return 0;
    }
    if (type == long.class) {
      return 0L;
    }
    if (type == float.class) {
      return 0F;
    }
    return 0D;
  }

  private static Path required(Path path) {
    if (!Files.exists(path)) {
      throw new IllegalStateException("Required runtime entry is missing: " + path);
    }
    return path;
  }

  private static String normalizedCodeSource(Class<?> type) throws Exception {
    URI location = type.getProtectionDomain().getCodeSource().getLocation().toURI();
    String normalized =
        Path.of(location).toAbsolutePath().normalize().toString().replace('\\', '/');
    return Files.isDirectory(Path.of(location)) ? normalized + "/" : normalized;
  }

  private static String jsonString(String value) {
    return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
  }

  enum StoreDialect {
    MYSQL(
        "ratchet-store-mysql-0.1.1.jar",
        "stores/ratchet-store-mysql/target/classes",
        "run.ratchet.store.mysql.MysqlJobStore",
        "run.ratchet.store.mysql.MysqlJobStoreImpl"),
    POSTGRESQL(
        "ratchet-store-postgresql-0.1.1.jar",
        "stores/ratchet-store-postgresql/target/classes",
        "run.ratchet.store.postgresql.PostgresqlJobStore",
        "run.ratchet.store.postgresql.PostgresqlJobStoreImpl");

    private final String releasedJarName;
    private final String currentClasses;
    private final String storeInterface;
    private final String storeImplementation;

    StoreDialect(
        String releasedJarName,
        String currentClasses,
        String storeInterface,
        String storeImplementation) {
      this.releasedJarName = releasedJarName;
      this.currentClasses = currentClasses;
      this.storeInterface = storeInterface;
      this.storeImplementation = storeImplementation;
    }

    String releasedJarName() {
      return releasedJarName;
    }

    String currentClasses() {
      return currentClasses;
    }

    String storeInterface() {
      return storeInterface;
    }

    String storeImplementation() {
      return storeImplementation;
    }
  }

  private static void requireEquals(Object expected, Object actual, String label) {
    if (!Objects.equals(expected, actual)) {
      throw new AssertionError(label + " expected <" + expected + "> but was <" + actual + ">");
    }
  }

  @Override
  public void close() throws Exception {
    try {
      if (entityManager.isOpen()) {
        entityManager.close();
      }
    } finally {
      try {
        sessionFactory.close();
      } finally {
        StandardServiceRegistryBuilder.destroy(serviceRegistry);
        classLoader.close();
      }
    }
  }

  @FunctionalInterface
  private interface CheckedOperation<T> {
    T run() throws Exception;
  }

  @FunctionalInterface
  private interface CheckedSupplier {
    Object get() throws Exception;
  }

  private static final class RatchetClassLoader extends URLClassLoader {

    private RatchetClassLoader(URL[] urls, ClassLoader parent) {
      super(Arrays.copyOf(urls, urls.length), parent);
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
      if (!name.startsWith("run.ratchet.")) {
        return super.loadClass(name, resolve);
      }
      synchronized (getClassLoadingLock(name)) {
        Class<?> loaded = findLoadedClass(name);
        if (loaded == null) {
          loaded = findClass(name);
        }
        if (resolve) {
          resolveClass(loaded);
        }
        return loaded;
      }
    }
  }
}
