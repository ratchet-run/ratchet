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
package run.ratchet.spring.boot.autoconfigure.jpa;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import javax.sql.DataSource;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.env.Environment;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.SharedEntityManagerCreator;
import run.ratchet.api.RatchetOptions;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.spi.JobStore;

/** Spring Boot JPA store wiring for Ratchet's SQL stores. */
@AutoConfiguration(
    afterName = {
      "run.ratchet.spring.boot.autoconfigure.RatchetAutoConfiguration",
      "org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration",
      "org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration",
      "org.springframework.boot.hibernate.autoconfigure.HibernateJpaAutoConfiguration"
    },
    beforeName = "run.ratchet.spring.boot.autoconfigure.RatchetEngineAutoConfiguration")
@ConditionalOnClass({EntityManager.class, EntityManagerFactory.class, JpaTransactionManager.class})
@ConditionalOnBean({DataSource.class, EntityManagerFactory.class})
@ConditionalOnMissingBean(JobStore.class)
@ConditionalOnProperty(
    prefix = "ratchet",
    name = "enabled",
    havingValue = "true",
    matchIfMissing = true)
public class RatchetJpaAutoConfiguration {

  static final String SCHEMA_INITIALIZER_BEAN_NAME = "ratchetJpaSchemaInitializer";
  static final String RUNTIME_INSTALLATION_BEAN_NAME = "ratchetRuntimeInstallation";

  @Bean(name = SCHEMA_INITIALIZER_BEAN_NAME)
  RatchetJpaSchemaInitializer ratchetJpaSchemaInitializer(
      ConfigurableListableBeanFactory beanFactory, RatchetOptions options) {
    DataSource dataSource = RatchetJpaInfrastructure.selectDataSource(beanFactory);
    return new RatchetJpaSchemaInitializer(dataSource, SqlStoreVendor.detect(dataSource), options);
  }

  /**
   * Makes schema preparation finish after the selected data source starts and before each JPA
   * entity-manager factory initializes.
   */
  @Bean
  static BeanFactoryPostProcessor ratchetJpaSchemaInitializerDependency(Environment environment) {
    return new RatchetJpaSchemaInitializerDependency(SCHEMA_INITIALIZER_BEAN_NAME, environment);
  }

  /** Adds Ratchet's explicit entity set to the selected application's persistence unit. */
  @Bean
  static BeanPostProcessor ratchetJpaPersistenceUnitPostProcessor(
      ConfigurableListableBeanFactory beanFactory) {
    return new RatchetJpaPersistenceUnitPostProcessor(beanFactory);
  }

  @Bean
  @DependsOn({SCHEMA_INITIALIZER_BEAN_NAME, RUNTIME_INSTALLATION_BEAN_NAME})
  JobStore ratchetJpaJobStore(
      ConfigurableListableBeanFactory beanFactory,
      RatchetOptions options,
      MetricsCollector metricsCollector) {
    DataSource dataSource = RatchetJpaInfrastructure.selectDataSource(beanFactory);
    EntityManagerFactory entityManagerFactory =
        RatchetJpaInfrastructure.selectEntityManagerFactory(beanFactory, dataSource);
    JpaTransactionManager transactionManager =
        RatchetJpaInfrastructure.selectTransactionManager(beanFactory, entityManagerFactory);
    EntityManager entityManager =
        SharedEntityManagerCreator.createSharedEntityManager(entityManagerFactory);
    SqlStoreVendor vendor = SqlStoreVendor.detect(dataSource);
    RatchetJpaPrerequisites.validateEntityManagerFactory(entityManagerFactory, vendor);
    JobStore store = vendor.createStore(entityManager, options, metricsCollector);
    return RatchetTransactionalStoreProxy.create(store, vendor.storeType(), transactionManager);
  }
}
