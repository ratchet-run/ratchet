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
package run.ratchet.micronaut.jpa;

import io.micronaut.context.BeanContext;
import io.micronaut.context.annotation.Requires;
import io.micronaut.jdbc.DataSourceResolver;
import io.micronaut.transaction.hibernate.HibernateTransactionManager;
import jakarta.inject.Singleton;
import jakarta.persistence.EntityManager;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import javax.sql.DataSource;
import org.hibernate.SessionFactory;
import run.ratchet.micronaut.internal.RatchetTransactionManagerProvider;
import run.ratchet.micronaut.internal.RatchetTransactionOperations;

/** Selects a coherent named datasource, Hibernate session factory, and transaction manager. */
@Singleton
@Requires(property = "ratchet.enabled", notEquals = "false")
final class RatchetJpaInfrastructure implements RatchetTransactionManagerProvider {
  private final BeanContext context;
  private final String datasourceName;
  private final RatchetTransactionOperations selection;

  RatchetJpaInfrastructure(BeanContext context, RatchetTransactionOperations selection) {
    this.context = context;
    this.selection = selection;
    this.datasourceName = selection.datasourceName();
  }

  String datasourceName() {
    return datasourceName;
  }

  DataSource dataSource() {
    // Micronaut Data wraps the bean in a contextual proxy whose connections come from the
    // transaction manager, which needs the SessionFactory that the schema step must precede.
    // Schema setup and vendor detection run outside any transaction, so use the pool itself.
    DataSource dataSource = selected(DataSource.class);
    return context
        .findBean(DataSourceResolver.class)
        .map(resolver -> resolver.resolve(dataSource))
        .orElse(dataSource);
  }

  SessionFactory sessionFactory() {
    return selected(SessionFactory.class);
  }

  @Override
  public HibernateTransactionManager transactions() {
    // A JDBC TransactionOperations bean may have the same name; select Hibernate explicitly.
    return selected(HibernateTransactionManager.class);
  }

  private <T> T selected(Class<T> type) {
    return selection.selected(type);
  }

  EntityManager entityManager() {
    HibernateTransactionManager transactions = transactions();
    // Resolve the session on EVERY invocation, including after REQUIRES_NEW suspension/resumption.
    // Never retain an EntityManager created outside Micronaut's transaction/connection context.
    // The selected HibernateTransactionManager.getConnection() returns the active session on both
    // Micronaut 4.10 and 5.2.
    return (EntityManager)
        Proxy.newProxyInstance(
            EntityManager.class.getClassLoader(),
            new Class<?>[] {EntityManager.class},
            (proxy, method, args) -> {
              if (method.getDeclaringClass() == Object.class) {
                return switch (method.getName()) {
                  case "equals" -> proxy == args[0];
                  case "hashCode" -> System.identityHashCode(proxy);
                  default -> "Ratchet transaction-aware EntityManager[" + datasourceName + "]";
                };
              }
              if (method.getName().equals("close")) return null;
              try {
                return method.invoke(transactions.getConnection(), args);
              } catch (InvocationTargetException failure) {
                throw failure.getCause();
              }
            });
  }
}
