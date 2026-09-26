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
package example.runtime.sql;

import static org.assertj.core.api.Assertions.*;

import example.runtime.model.RoutedRecord;
import jakarta.persistence.EntityManagerFactory;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.SharedEntityManagerCreator;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import org.springframework.transaction.support.TransactionTemplate;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.JobStatus;
import run.ratchet.consumer.sql.SqlDatabase;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.migration.SchemaMigrator;
import run.ratchet.store.postgresql.PostgresqlSchemaMigrationDialect;

class MultiplePersistenceUnitsRuntimeIT {
  @Test
  void namedPrimaryRoutesMetadataMigrationAndCommitRollbackToOneDatabase() {
    try (var orders = SqlDatabase.start();
        var audit = SqlDatabase.startIsolated()) {
      var properties = RuntimeSupport.properties(orders);
      properties.put("audit.url", audit.properties().get("spring.datasource.url"));
      properties.put("spring.sql.init.mode", "never");
      try (var context =
          new SpringApplicationBuilder(Application.class).properties(properties).run()) {
        var primary = context.getBean("ordersEntityManagerFactory", EntityManagerFactory.class);
        var secondary = context.getBean("entityManagerFactory", EntityManagerFactory.class);
        assertThat(primary.getMetamodel().getEntities())
            .extracting(e -> e.getJavaType().getName())
            .contains(JobEntity.class.getName(), RoutedRecord.class.getName());
        assertThat(secondary.getMetamodel().getEntities())
            .extracting(e -> e.getJavaType().getName())
            .containsExactly(RoutedRecord.class.getName());
        var orderJdbc = new JdbcTemplate(RuntimeSupport.dataSource(orders));
        var auditJdbc = new JdbcTemplate(RuntimeSupport.dataSource(audit));
        assertThat(
                auditJdbc.queryForObject(
                    "select count(*) from information_schema.tables where table_name like 'scheduler_%' or table_name = 'ratchet_schema_version'",
                    Integer.class))
            .isZero();
        var scheduler = context.getBean(JobSchedulerService.class);
        var transaction =
            new TransactionTemplate(
                context.getBean("ordersTransactions", JpaTransactionManager.class));
        var entityManager = SharedEntityManagerCreator.createSharedEntityManager(primary);
        JobHandle committed =
            transaction.execute(
                status -> {
                  entityManager.persist(new RoutedRecord("commit", "orders"));
                  return scheduler.enqueue(RuntimeSupport::noop).submit();
                });
        RuntimeSupport.status(context, committed, JobStatus.SUCCEEDED);
        AtomicReference<JobHandle> rolledBack = new AtomicReference<>();
        transaction.executeWithoutResult(
            status -> {
              entityManager.persist(new RoutedRecord("rollback", "absent"));
              rolledBack.set(scheduler.enqueue(RuntimeSupport::noop).submit());
              status.setRollbackOnly();
            });
        assertThat(orderJdbc.queryForList("select id from routed_record", String.class))
            .containsExactly("commit");
        assertThat(
                orderJdbc.queryForObject(
                    "select count(*) from scheduler_job where job_id = ?",
                    Integer.class,
                    rolledBack.get().id()))
            .isZero();
        var other =
            new TransactionTemplate(
                context.getBean("transactionManager", JpaTransactionManager.class));
        other.executeWithoutResult(
            status ->
                SharedEntityManagerCreator.createSharedEntityManager(secondary)
                    .persist(new RoutedRecord("audit", "independent")));
        assertThat(auditJdbc.queryForList("select id from routed_record", String.class))
            .containsExactly("audit");
        assertThat(
                orderJdbc.queryForObject(
                    "select count(*) from routed_record where id = 'audit'", Integer.class))
            .isZero();
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"factory", "transaction", "ambiguous"})
  void incoherentResourcesFailWithoutStartingWorkers(String mismatch) throws Exception {
    try (var orders = SqlDatabase.start();
        var audit = SqlDatabase.startIsolated()) {
      var properties = RuntimeSupport.properties(orders);
      properties.put("audit.url", audit.properties().get("spring.datasource.url"));
      properties.put("spring.sql.init.mode", "never");
      properties.put("mismatch", mismatch);
      if (mismatch.equals("factory")) {
        // Give the wrong persistence unit a valid schema so the resource-coherence guard,
        // rather than Hibernate's missing-table check, must reject this configuration.
        new SchemaMigrator(RuntimeSupport.dataSource(audit), new PostgresqlSchemaMigrationDialect())
            .migrate();
      }
      String expected =
          switch (mismatch) {
            case "factory" -> "does not use the selected DataSource";
            case "transaction" -> "does not manage the selected EntityManagerFactory";
            default -> "Ratchet requires one bean";
          };
      assertThatThrownBy(
              () -> new SpringApplicationBuilder(Application.class).properties(properties).run())
          .hasStackTraceContaining(expected);
      // A failed context must release the process-wide runtime installation.
      try (var recovered = RuntimeSupport.start(orders)) {
        RuntimeSupport.status(
            recovered,
            recovered.getBean(JobSchedulerService.class).enqueue(RuntimeSupport::noop).submit(),
            JobStatus.SUCCEEDED);
      }
    }
  }

  @SpringBootConfiguration
  @EnableAutoConfiguration
  static class Application {
    @Bean
    @Primary
    DataSource ordersDataSource(Environment env) {
      return source(env.getProperty("spring.datasource.url"), env);
    }

    @Bean
    DataSource dataSource(Environment env) {
      return source(env.getProperty("audit.url"), env);
    }

    @Bean
    @Primary
    LocalContainerEntityManagerFactoryBean ordersEntityManagerFactory(
        @Qualifier("ordersDataSource") DataSource source) {
      return factory(source, "orders");
    }

    @Bean
    LocalContainerEntityManagerFactoryBean entityManagerFactory(
        @Qualifier("dataSource") DataSource source) {
      return factory(source, "audit");
    }

    @Bean
    @Primary
    JpaTransactionManager ordersTransactions(
        @Qualifier("ordersEntityManagerFactory") EntityManagerFactory factory) {
      return new JpaTransactionManager(factory);
    }

    @Bean
    JpaTransactionManager transactionManager(
        @Qualifier("entityManagerFactory") EntityManagerFactory factory) {
      return new JpaTransactionManager(factory);
    }

    @Bean
    static BeanDefinitionRegistryPostProcessor selectMismatch(Environment env) {
      return registry -> {
        String mismatch = env.getProperty("mismatch", "none");
        if (mismatch.equals("factory")) {
          registry.getBeanDefinition("ordersEntityManagerFactory").setPrimary(false);
          registry.getBeanDefinition("entityManagerFactory").setPrimary(true);
        } else if (mismatch.equals("transaction")) {
          registry.getBeanDefinition("ordersTransactions").setPrimary(false);
          registry.getBeanDefinition("transactionManager").setPrimary(true);
        } else if (mismatch.equals("ambiguous"))
          registry.getBeanDefinition("ordersDataSource").setPrimary(false);
      };
    }

    private static DataSource source(String url, Environment env) {
      var source =
          new DriverManagerDataSource(
              url,
              env.getProperty("spring.datasource.username"),
              env.getProperty("spring.datasource.password"));
      new JdbcTemplate(source)
          .execute(
              "create table if not exists routed_record (id varchar(255) primary key, value_text varchar(255))");
      return source;
    }

    private static LocalContainerEntityManagerFactoryBean factory(DataSource source, String name) {
      var factory = new LocalContainerEntityManagerFactoryBean();
      factory.setDataSource(source);
      factory.setPersistenceUnitName(name);
      factory.setPackagesToScan("example.runtime.model");
      factory.setMappingResources(new String[0]);
      factory.setJpaVendorAdapter(new HibernateJpaVendorAdapter());
      factory.setJpaPropertyMap(
          Map.of(
              "hibernate.hbm2ddl.auto",
              "validate",
              "hibernate.physical_naming_strategy",
              "org.hibernate.boot.model.naming.CamelCaseToUnderscoresNamingStrategy"));
      return factory;
    }
  }
}
