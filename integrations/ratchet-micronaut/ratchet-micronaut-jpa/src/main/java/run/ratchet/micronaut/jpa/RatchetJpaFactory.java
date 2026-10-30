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

import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import io.micronaut.transaction.TransactionDefinition;
import io.micronaut.transaction.hibernate.HibernateTransactionManager;
import jakarta.inject.Singleton;
import javax.sql.DataSource;
import run.ratchet.api.RatchetOptions;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.converter.RuntimeContextInstallation;
import run.ratchet.store.spi.JobStore;

/** SQL-store factory; application JobStore beans retain the override point. */
@Factory
@Requires(property = "ratchet.enabled", notEquals = "false")
@Requires(beans = DataSource.class)
public class RatchetJpaFactory {
  @Singleton
  @Requires(missingBeans = RatchetJpaSchemaInitializer.class)
  RatchetJpaSchemaInitializer schemaInitializer(
      RatchetJpaInfrastructure infrastructure, RatchetOptions options) {
    DataSource dataSource = infrastructure.dataSource();
    return new RatchetJpaSchemaInitializer(dataSource, SqlStoreVendor.detect(dataSource), options);
  }

  @Singleton
  @Requires(missingBeans = JobStore.class)
  JobStore jobStore(
      RatchetJpaInfrastructure infrastructure,
      RatchetOptions options,
      MetricsCollector metrics,
      RatchetJpaSchemaInitializer initializer,
      RuntimeContextInstallation installation) {
    initializer.initialize();
    SqlStoreVendor vendor = SqlStoreVendor.detect(infrastructure.dataSource());
    RatchetJpaPrerequisites.validateEntityManagerFactory(infrastructure.sessionFactory(), vendor);
    HibernateTransactionManager transactions = infrastructure.transactions();
    // Vendor factories run JDBC isolation checks immediately. Provide a managed session during
    // initialization as well as during every subsequent proxied store invocation.
    // Vendor isolation probes run inside this default transaction.
    JobStore target =
        transactions.execute(
            TransactionDefinition.DEFAULT,
            status -> vendor.createStore(infrastructure.entityManager(), options, metrics));
    return RatchetTransactionalStoreProxy.create(target, vendor.storeType(), transactions);
  }
}
