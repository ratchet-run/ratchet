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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import jakarta.persistence.Entity;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.Id;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import run.ratchet.spring.boot.autoconfigure.RatchetJpaIsolationAutoConfiguration;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.JobStore;

class RatchetJpaCustomStoreMappingsTest {
  @Test
  void customSqlJobStoreKeepsTheJpaModuleImplicitMappings() {
    new ApplicationContextRunner()
        .withConfiguration(
            AutoConfigurations.of(
                RatchetJpaAutoConfiguration.class, RatchetJpaIsolationAutoConfiguration.class))
        .withUserConfiguration(CustomSqlJobStoreJpa.class)
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertThat(context).doesNotHaveBean("ratchetJpaPersistenceUnitPostProcessor");
              assertThat(context).doesNotHaveBean("ratchetJpaIsolationMappings");
              assertThat(context.getBean(EntityManagerFactory.class).getMetamodel().getEntities())
                  .extracting(entity -> entity.getJavaType().getName())
                  .contains(JobEntity.class.getName());
            });
  }

  @Configuration(proxyBeanMethods = false)
  static class CustomSqlJobStoreJpa {
    @Bean
    JobStore userJobStore() {
      return mock(JobStore.class);
    }

    @Bean
    LocalContainerEntityManagerFactoryBean hostEntityManagerFactory() {
      var factory = new LocalContainerEntityManagerFactoryBean();
      factory.setDataSource(new DriverManagerDataSource("jdbc:h2:mem:custom_store", "sa", ""));
      factory.setJpaVendorAdapter(new HibernateJpaVendorAdapter());
      factory.setEntityManagerFactoryInterface(EntityManagerFactory.class);
      factory.setPackagesToScan(CustomStoreHostRecord.class.getPackageName());
      return factory;
    }
  }

  @Entity
  static class CustomStoreHostRecord {
    @Id Long id;
  }
}
