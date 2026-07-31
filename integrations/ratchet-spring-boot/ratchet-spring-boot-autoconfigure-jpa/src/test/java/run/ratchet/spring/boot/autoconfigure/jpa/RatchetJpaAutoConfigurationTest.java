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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.persistence.EntityManagerFactory;
import java.io.IOException;
import java.net.URL;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.persistenceunit.PersistenceUnitManager;
import run.ratchet.api.RatchetOptions;
import run.ratchet.store.migration.SchemaInitializationException;
import run.ratchet.store.postgresql.PostgresqlJobStore;
import run.ratchet.store.postgresql.PostgresqlSchemaMigrationDialect;

class RatchetJpaAutoConfigurationTest {

  @Test
  void ordersAfterBothSpringBootHibernateAutoConfigurationNames() {
    AutoConfiguration autoConfiguration =
        RatchetJpaAutoConfiguration.class.getAnnotation(AutoConfiguration.class);

    assertArrayEquals(
        new String[] {
          "org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration",
          "org.springframework.boot.hibernate.autoconfigure.HibernateJpaAutoConfiguration"
        },
        autoConfiguration.afterName());
  }

  @Test
  void outerAutoConfigurationSignaturesDoNotLinkToOptionalPostgresqlStoreTypes() {
    boolean storeTypeInOuterMethodSignature =
        Stream.of(RatchetJpaAutoConfiguration.class.getDeclaredMethods())
            .flatMap(
                method ->
                    Stream.concat(
                        Stream.of(method.getReturnType()), Stream.of(method.getParameterTypes())))
            .map(Class::getName)
            .anyMatch(typeName -> typeName.startsWith("run.ratchet.store.postgresql."));

    assertFalse(storeTypeInOuterMethodSignature);
  }

  @Test
  void postgresqlBeansAreIsolatedInGatedNonProxyingNestedConfiguration() {
    Class<?> configurationClass = RatchetJpaAutoConfiguration.PostgresqlStoreConfiguration.class;
    Configuration configuration = configurationClass.getAnnotation(Configuration.class);
    ConditionalOnClass condition = configurationClass.getAnnotation(ConditionalOnClass.class);

    assertFalse(configuration.proxyBeanMethods());
    assertArrayEquals(new Class<?>[] {PostgresqlJobStore.class}, condition.value());
  }

  @Test
  void acceptsOrmXmlAndAnchorFromSameStoreCoreJar() throws Exception {
    URL ormXml = new URL("jar:file:/repo/ratchet-store-core-0.2.2.jar!/META-INF/orm.xml");
    URL anchor =
        new URL(
            "jar:file:/repo/ratchet-store-core-0.2.2.jar!/"
                + "run/ratchet/store/spi/RatchetEntityManagerProvider.class");

    assertTrue(RatchetJpaAutoConfiguration.isRatchetStoreCoreJarResource(ormXml, anchor));
  }

  @Test
  void rejectsExplodedStoreCoreClasspathRoot() throws Exception {
    URL ormXml = new URL("file:/repo/target/classes/META-INF/orm.xml");
    URL anchor =
        new URL(
            "file:/repo/target/classes/"
                + "run/ratchet/store/spi/RatchetEntityManagerProvider.class");

    assertFalse(RatchetJpaAutoConfiguration.isRatchetStoreCoreJarResource(ormXml, anchor));
  }

  @Test
  void rejectsMatchingRootFromWrongJar() throws Exception {
    URL ormXml = new URL("jar:file:/app/application.jar!/META-INF/orm.xml");
    URL anchor =
        new URL(
            "jar:file:/app/application.jar!/"
                + "run/ratchet/store/spi/RatchetEntityManagerProvider.class");

    assertFalse(RatchetJpaAutoConfiguration.isRatchetStoreCoreJarResource(ormXml, anchor));
  }

  @Test
  void rejectsUserProvidedPersistenceUnitManager() {
    DefaultListableBeanFactory beanFactory = beanFactory(getClass().getClassLoader());
    beanFactory.registerSingleton(
        "customPersistenceUnitManager", mock(PersistenceUnitManager.class));

    IllegalStateException failure =
        assertThrows(
            IllegalStateException.class,
            () ->
                RatchetJpaAutoConfiguration.validateJpaEnvironment(
                    beanFactory, new StandardEnvironment()));

    assertTrue(
        failure
            .getMessage()
            .contains(
                "requires Spring Boot's default PersistenceUnitManager, but found user-provided"
                    + " PersistenceUnitManager bean(s): [customPersistenceUnitManager]"));
  }

  @Test
  void rejectsConfiguredMappingResources() {
    StandardEnvironment environment = new StandardEnvironment();
    environment
        .getPropertySources()
        .addFirst(
            new MapPropertySource(
                "test",
                Map.of(
                    RatchetJpaAutoConfiguration.MAPPING_RESOURCES_PROPERTY,
                    "META-INF/application-orm.xml")));

    IllegalStateException failure =
        assertThrows(
            IllegalStateException.class,
            () ->
                RatchetJpaAutoConfiguration.validateJpaEnvironment(
                    beanFactory(getClass().getClassLoader()), environment));

    assertTrue(
        failure.getMessage().contains("cannot run when spring.jpa.mapping-resources is set"));
  }

  @Test
  void rejectsIndexedMappingResources() {
    StandardEnvironment environment = new StandardEnvironment();
    environment
        .getPropertySources()
        .addFirst(
            new MapPropertySource(
                "test", Map.of("spring.jpa.mapping-resources[0]", "META-INF/application-orm.xml")));

    IllegalStateException failure =
        assertThrows(
            IllegalStateException.class,
            () ->
                RatchetJpaAutoConfiguration.validateJpaEnvironment(
                    beanFactory(getClass().getClassLoader()), environment));

    assertTrue(
        failure.getMessage().contains("cannot run when spring.jpa.mapping-resources is set"));
  }

  @Test
  void rejectsMissingOrmXml() {
    ClassLoader classLoader =
        classLoaderWithOrmResources(Collections.enumeration(Collections.emptyList()));

    IllegalStateException failure =
        assertThrows(
            IllegalStateException.class,
            () ->
                RatchetJpaAutoConfiguration.validateJpaEnvironment(
                    beanFactory(classLoader), new StandardEnvironment()));

    assertTrue(failure.getMessage().contains("but found none"));
  }

  @Test
  void rejectsMultipleOrmXmlResources() throws Exception {
    ClassLoader classLoader =
        classLoaderWithOrmResources(
            Collections.enumeration(
                java.util.List.of(
                    new URL("file:/first/META-INF/orm.xml"),
                    new URL("file:/second/META-INF/orm.xml"))));

    IllegalStateException failure =
        assertThrows(
            IllegalStateException.class,
            () ->
                RatchetJpaAutoConfiguration.validateJpaEnvironment(
                    beanFactory(classLoader), new StandardEnvironment()));

    assertTrue(failure.getMessage().contains("but found 2"));
  }

  @Test
  void rejectsOrmXmlOutsideStoreCoreClasspathRoot() throws Exception {
    ClassLoader classLoader =
        classLoaderWithOrmResources(
            Collections.enumeration(
                java.util.List.of(new URL("file:/application/META-INF/orm.xml"))));

    IllegalStateException failure =
        assertThrows(
            IllegalStateException.class,
            () ->
                RatchetJpaAutoConfiguration.validateJpaEnvironment(
                    beanFactory(classLoader), new StandardEnvironment()));

    assertTrue(
        failure.getMessage().contains("resource to resolve from the ratchet-store-core jar"));
  }

  @Test
  void resolvesExactlyOneEntityManagerFactoryAndJpaTransactionManager() {
    DefaultListableBeanFactory beanFactory = beanFactory(getClass().getClassLoader());
    EntityManagerFactory entityManagerFactory = mock(EntityManagerFactory.class);
    JpaTransactionManager transactionManager = new JpaTransactionManager(entityManagerFactory);
    beanFactory.registerSingleton("entityManagerFactory", entityManagerFactory);
    beanFactory.registerSingleton("transactionManager", transactionManager);

    RatchetJpaAutoConfiguration.JpaTopology topology =
        RatchetJpaAutoConfiguration.resolveJpaTopology(beanFactory);

    assertSame(entityManagerFactory, topology.entityManagerFactory());
    assertSame(transactionManager, topology.transactionManager());
  }

  @Test
  void rejectsMultipleEntityManagerFactoriesEvenWithOneTransactionManager() {
    DefaultListableBeanFactory beanFactory = beanFactory(getClass().getClassLoader());
    EntityManagerFactory first = mock(EntityManagerFactory.class);
    EntityManagerFactory second = mock(EntityManagerFactory.class);
    beanFactory.registerSingleton("firstEntityManagerFactory", first);
    beanFactory.registerSingleton("secondEntityManagerFactory", second);
    beanFactory.registerSingleton("transactionManager", new JpaTransactionManager(first));

    IllegalStateException failure =
        assertThrows(
            IllegalStateException.class,
            () -> RatchetJpaAutoConfiguration.resolveJpaTopology(beanFactory));

    assertTrue(
        failure
            .getMessage()
            .contains("requires exactly one EntityManagerFactory bean, but found 2"));
  }

  @Test
  void rejectsMultipleJpaTransactionManagers() {
    DefaultListableBeanFactory beanFactory = beanFactory(getClass().getClassLoader());
    EntityManagerFactory entityManagerFactory = mock(EntityManagerFactory.class);
    beanFactory.registerSingleton("entityManagerFactory", entityManagerFactory);
    beanFactory.registerSingleton(
        "firstTransactionManager", new JpaTransactionManager(entityManagerFactory));
    beanFactory.registerSingleton(
        "secondTransactionManager", new JpaTransactionManager(entityManagerFactory));

    IllegalStateException failure =
        assertThrows(
            IllegalStateException.class,
            () -> RatchetJpaAutoConfiguration.resolveJpaTopology(beanFactory));

    assertTrue(
        failure
            .getMessage()
            .contains("requires exactly one JpaTransactionManager bean, but found 2"));
  }

  @Test
  void rejectsTransactionManagerOwnedByDifferentEntityManagerFactory() {
    DefaultListableBeanFactory beanFactory = beanFactory(getClass().getClassLoader());
    EntityManagerFactory selected = mock(EntityManagerFactory.class);
    EntityManagerFactory owned = mock(EntityManagerFactory.class);
    beanFactory.registerSingleton("entityManagerFactory", selected);
    beanFactory.registerSingleton("transactionManager", new JpaTransactionManager(owned));

    IllegalStateException failure =
        assertThrows(
            IllegalStateException.class,
            () -> RatchetJpaAutoConfiguration.resolveJpaTopology(beanFactory));

    assertTrue(
        failure
            .getMessage()
            .contains(
                "requires JpaTransactionManager bean 'transactionManager' to own the selected"
                    + " EntityManagerFactory bean 'entityManagerFactory'"));
  }

  @Test
  void migrationInitializerIsNoOpWhenAutoMigrationIsOff() {
    assertDoesNotThrow(
        () ->
            new RatchetJpaSchemaMigrationInitializer(
                beanFactory(getClass().getClassLoader()),
                RatchetOptions.defaults(),
                new PostgresqlSchemaMigrationDialect()));
  }

  @Test
  void migrationInitializerInfersPostgresqlWhenDialectIsBlank() {
    RatchetOptions options =
        RatchetOptions.builder().schema(schema -> schema.autoMigrate(true)).build();

    SchemaInitializationException failure =
        assertThrows(
            SchemaInitializationException.class,
            () ->
                new RatchetJpaSchemaMigrationInitializer(
                    beanFactory(getClass().getClassLoader()),
                    options,
                    new PostgresqlSchemaMigrationDialect()));

    assertTrue(
        failure
            .getMessage()
            .contains("requires exactly one DataSource bean for the Ratchet PostgreSQL store"));
  }

  @Test
  void migrationInitializerRejectsExplicitConflictingDialectBeforeDataSourceLookup() {
    RatchetOptions options =
        RatchetOptions.builder()
            .schema(schema -> schema.autoMigrate(true).migrationDialect("mysql"))
            .build();

    SchemaInitializationException failure =
        assertThrows(
            SchemaInitializationException.class,
            () ->
                new RatchetJpaSchemaMigrationInitializer(
                    beanFactory(getClass().getClassLoader()),
                    options,
                    new PostgresqlSchemaMigrationDialect()));

    assertTrue(
        failure
            .getMessage()
            .contains(
                "requires ratchet.schema.migration-dialect to be blank or 'postgresql', but was"
                    + " 'mysql'"));
  }

  @Test
  void migrationInitializerRequiresExactlyOneDataSource() {
    DefaultListableBeanFactory beanFactory = beanFactory(getClass().getClassLoader());
    beanFactory.registerSingleton("firstDataSource", mock(DataSource.class));
    beanFactory.registerSingleton("secondDataSource", mock(DataSource.class));
    RatchetOptions options =
        RatchetOptions.builder().schema(schema -> schema.autoMigrate(true)).build();

    SchemaInitializationException failure =
        assertThrows(
            SchemaInitializationException.class,
            () ->
                new RatchetJpaSchemaMigrationInitializer(
                    beanFactory, options, new PostgresqlSchemaMigrationDialect()));

    assertTrue(failure.getMessage().contains("but found 2: [firstDataSource, secondDataSource]"));
  }

  @Test
  void migrationInitializerWrapsMigrationFailureActionably() throws SQLException {
    DefaultListableBeanFactory beanFactory = beanFactory(getClass().getClassLoader());
    DataSource dataSource = mock(DataSource.class);
    when(dataSource.getConnection()).thenThrow(new SQLException("database unavailable"));
    beanFactory.registerSingleton("dataSource", dataSource);
    RatchetOptions options =
        RatchetOptions.builder().schema(schema -> schema.autoMigrate(true)).build();

    SchemaInitializationException failure =
        assertThrows(
            SchemaInitializationException.class,
            () ->
                new RatchetJpaSchemaMigrationInitializer(
                    beanFactory, options, new PostgresqlSchemaMigrationDialect()));

    assertTrue(
        failure
            .getMessage()
            .contains(
                "Ratchet PostgreSQL schema auto-migration failed: SQLException: database"
                    + " unavailable"));
  }

  private static DefaultListableBeanFactory beanFactory(ClassLoader classLoader) {
    DefaultListableBeanFactory beanFactory = new DefaultListableBeanFactory();
    beanFactory.setBeanClassLoader(classLoader);
    return beanFactory;
  }

  private ClassLoader classLoaderWithOrmResources(java.util.Enumeration<URL> resources) {
    return new ClassLoader(getClass().getClassLoader()) {
      @Override
      public java.util.Enumeration<URL> getResources(String name) throws IOException {
        if (RatchetJpaAutoConfiguration.ORM_XML_RESOURCE.equals(name)) {
          return resources;
        }
        return super.getResources(name);
      }
    };
  }
}
