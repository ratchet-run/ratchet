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
package run.ratchet.spring.boot.it.postgresql;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.persistenceunit.DefaultPersistenceUnitManager;
import org.springframework.orm.jpa.persistenceunit.PersistenceUnitManager;
import run.ratchet.spring.boot.it.postgresql.fixture.ratchetonly.RatchetOnlyApplication;

class PostgresqlNegativeStartupTest extends PostgresqlIntegrationTestSupport {

  @Test
  void secondEntityManagerFactoryFailsWithActionableTopologyMessage() {
    contextRunner(
            RatchetOnlyApplication.class,
            migrationOptions(""),
            SecondEntityManagerFactoryConfiguration.class)
        .run(
            context -> {
              assertThat(context).hasFailed();
              assertThat(failureMessages(context.getStartupFailure()))
                  .contains("requires exactly one EntityManagerFactory bean, but found");
            });
  }

  @Test
  void secondJpaTransactionManagerFailsWithActionableTopologyMessage() {
    contextRunner(
            RatchetOnlyApplication.class,
            migrationOptions(""),
            SecondTransactionManagerConfiguration.class)
        .run(
            context -> {
              assertThat(context).hasFailed();
              assertThat(failureMessages(context.getStartupFailure()))
                  .contains("requires exactly one JpaTransactionManager bean, but found");
            });
  }

  @Test
  void userPersistenceUnitManagerFailsBeforeJpaTopologyCanDrift() {
    contextRunner(
            RatchetOnlyApplication.class,
            migrationOptions(""),
            UserPersistenceUnitManagerConfiguration.class)
        .run(
            context -> {
              assertThat(context).hasFailed();
              assertThat(failureMessages(context.getStartupFailure()))
                  .contains(
                      "requires Spring Boot's default PersistenceUnitManager, but found"
                          + " user-provided PersistenceUnitManager bean(s)");
            });
  }

  @Test
  void mappingResourcesPropertyFailsBeforeDefaultOrmDiscoveryCanBeSuppressed() {
    contextRunner(RatchetOnlyApplication.class, migrationOptions(""))
        .withPropertyValues("spring.jpa.mapping-resources=META-INF/orm.xml")
        .run(
            context -> {
              assertThat(context).hasFailed();
              assertThat(failureMessages(context.getStartupFailure()))
                  .contains("cannot run when spring.jpa.mapping-resources is set");
            });
  }

  @Configuration(proxyBeanMethods = false)
  static class SecondEntityManagerFactoryConfiguration {

    @Bean
    static BeanFactoryPostProcessor secondEntityManagerFactoryRegistrar() {
      return beanFactory ->
          beanFactory.registerSingleton("secondEntityManagerFactory", entityManagerFactoryProxy());
    }
  }

  @Configuration(proxyBeanMethods = false)
  static class SecondTransactionManagerConfiguration {

    @Bean
    static BeanFactoryPostProcessor secondTransactionManagerRegistrar() {
      return beanFactory ->
          beanFactory.registerSingleton("secondJpaTransactionManager", new JpaTransactionManager());
    }
  }

  @Configuration(proxyBeanMethods = false)
  static class UserPersistenceUnitManagerConfiguration {

    @Bean
    PersistenceUnitManager userPersistenceUnitManager() {
      return new DefaultPersistenceUnitManager();
    }
  }
}
