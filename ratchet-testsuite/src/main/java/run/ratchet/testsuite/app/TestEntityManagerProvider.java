package run.ratchet.testsuite.app;

import jakarta.annotation.PreDestroy;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.interceptor.Interceptor;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.Persistence;
import jakarta.persistence.TransactionRequiredException;
import jakarta.transaction.TransactionSynchronizationRegistry;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import run.ratchet.store.spi.RatchetEntityManagerProvider;

/**
 * Test deployment override for store modules' unnamed persistence-context providers.
 *
 * <p>Some containers do not resolve {@code @PersistenceContext} fields inside WEB-INF/lib CDI bean
 * archives against the WAR persistence unit. The tests own the persistence-unit name, so an enabled
 * alternative keeps that binding explicit without changing production defaults.
 */
@Alternative
@Priority(Interceptor.Priority.APPLICATION)
@ApplicationScoped
public class TestEntityManagerProvider implements RatchetEntityManagerProvider {

  private final Set<EntityManager> createdEntityManagers = ConcurrentHashMap.newKeySet();
  private final ThreadLocal<Object> transactionKeys = new ThreadLocal<>();
  private volatile EntityManagerFactory entityManagerFactory;
  private final ThreadLocal<EntityManager> entityManagers =
      ThreadLocal.withInitial(this::createEntityManager);
  private volatile TransactionSynchronizationRegistry transactionSynchronizationRegistry;
  private volatile boolean transactionSynchronizationRegistryLookupAttempted;

  private final EntityManager proxy =
      (EntityManager)
          Proxy.newProxyInstance(
              EntityManager.class.getClassLoader(),
              new Class<?>[] {EntityManager.class},
              (proxy, method, args) -> {
                if (method.getDeclaringClass() == Object.class) {
                  return method.invoke(this, args);
                }

                EntityManager delegate = entityManagers.get();
                joinTransaction(delegate);
                clearIfTransactionChanged(delegate);
                try {
                  return method.invoke(delegate, args);
                } catch (InvocationTargetException e) {
                  throw e.getCause();
                }
              });

  private static void joinTransaction(EntityManager entityManager) {
    try {
      entityManager.joinTransaction();
    } catch (IllegalStateException | TransactionRequiredException ignored) {
      // Native startup checks can run before a JTA transaction exists.
    }
  }

  @Override
  public EntityManager getEntityManager() {
    return proxy;
  }

  @PreDestroy
  void close() {
    createdEntityManagers.stream().filter(EntityManager::isOpen).forEach(EntityManager::close);
    EntityManagerFactory factory = entityManagerFactory;
    if (factory != null && factory.isOpen()) {
      factory.close();
    }
  }

  private EntityManager createEntityManager() {
    EntityManager entityManager = entityManagerFactory().createEntityManager();
    createdEntityManagers.add(entityManager);
    return entityManager;
  }

  private EntityManagerFactory entityManagerFactory() {
    EntityManagerFactory factory = entityManagerFactory;
    if (factory == null) {
      synchronized (this) {
        factory = entityManagerFactory;
        if (factory == null) {
          factory = Persistence.createEntityManagerFactory("ratchet-test");
          entityManagerFactory = factory;
        }
      }
    }
    return factory;
  }

  private void clearIfTransactionChanged(EntityManager entityManager) {
    Object transactionKey = currentTransactionKey();
    Object previousTransactionKey = transactionKeys.get();
    if (transactionKey == null) {
      transactionKeys.remove();
      entityManager.clear();
      return;
    }

    if (previousTransactionKey != transactionKey) {
      transactionKeys.set(transactionKey);
      entityManager.clear();
    }
  }

  private Object currentTransactionKey() {
    TransactionSynchronizationRegistry registry = transactionSynchronizationRegistry();
    return registry == null ? null : registry.getTransactionKey();
  }

  private TransactionSynchronizationRegistry transactionSynchronizationRegistry() {
    TransactionSynchronizationRegistry registry = transactionSynchronizationRegistry;
    if (registry == null && !transactionSynchronizationRegistryLookupAttempted) {
      synchronized (this) {
        registry = transactionSynchronizationRegistry;
        if (registry == null && !transactionSynchronizationRegistryLookupAttempted) {
          transactionSynchronizationRegistryLookupAttempted = true;
          try {
            registry =
                (TransactionSynchronizationRegistry)
                    new InitialContext().lookup("java:comp/TransactionSynchronizationRegistry");
            transactionSynchronizationRegistry = registry;
          } catch (NamingException ignored) {
            // Some startup paths run before component JNDI is fully available.
          }
        }
      }
    }
    return registry;
  }
}
