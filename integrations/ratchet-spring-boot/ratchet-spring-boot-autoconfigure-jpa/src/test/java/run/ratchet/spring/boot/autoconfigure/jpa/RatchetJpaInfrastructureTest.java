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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.spi.PersistenceProvider;
import jakarta.persistence.spi.PersistenceUnitInfo;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.util.Map;
import java.util.function.Supplier;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.aop.scope.ScopedProxyUtils;
import org.springframework.beans.factory.config.BeanDefinitionHolder;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.support.SimpleThreadScope;
import org.springframework.mock.env.MockEnvironment;
import org.springframework.orm.jpa.EntityManagerFactoryInfo;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.persistenceunit.MutablePersistenceUnitInfo;
import org.springframework.transaction.PlatformTransactionManager;
import run.ratchet.api.RatchetOptions;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.postgresql.PostgresqlJobStore;
import run.ratchet.store.postgresql.PostgresqlJobStoreFactory;

class RatchetJpaInfrastructureTest {

  @Test
  void deferredDatabaseInitializationKeepsRatchetBeforeTheEntityManagerFactory() {
    new ApplicationContextRunner()
        .withInitializer(
            context -> {
              DefaultListableBeanFactory beanFactory =
                  (DefaultListableBeanFactory) context.getBeanFactory();
              RootBeanDefinition dataSource = new RootBeanDefinition(Object.class);
              dataSource.setTargetType(DataSource.class);
              beanFactory.registerBeanDefinition("dataSource", dataSource);
              RootBeanDefinition entityManagerFactory = new RootBeanDefinition(Object.class);
              entityManagerFactory.setTargetType(EntityManagerFactory.class);
              beanFactory.registerBeanDefinition("entityManagerFactory", entityManagerFactory);
              RootBeanDefinition initializer = new RootBeanDefinition(Object.class);
              beanFactory.registerBeanDefinition("ratchetJpaSchemaInitializer", initializer);
              RootBeanDefinition dataScripts = new RootBeanDefinition(Object.class);
              dataScripts.setDependsOn("entityManagerFactory");
              dataScripts.setAttribute(
                  "org.springframework.boot.sql.init.dependency.DatabaseInitializerDetector",
                  "org.springframework.boot.jdbc.init.DataSourceScriptDatabaseInitializerDetector");
              beanFactory.registerBeanDefinition(
                  "dataSourceScriptDatabaseInitializer", dataScripts);
              RootBeanDefinition flywayConfiguration = new RootBeanDefinition(Object.class);
              flywayConfiguration.setDependsOn("entityManagerFactory");
              flywayConfiguration.setAttribute(
                  "org.springframework.boot.sql.init.dependency.DatabaseInitializerDetector",
                  "org.springframework.boot.flyway.FlywayDatabaseInitializerDetector");
              beanFactory.registerBeanDefinition("flyway", flywayConfiguration);
              RootBeanDefinition flyway = new RootBeanDefinition(Object.class);
              flyway.setDependsOn("entityManagerFactory");
              flyway.setAttribute(
                  "org.springframework.boot.sql.init.dependency.DatabaseInitializerDetector",
                  "org.springframework.boot.autoconfigure.flyway.FlywayMigrationInitializerDatabaseInitializerDetector");
              beanFactory.registerBeanDefinition("flywayInitializer", flyway);
              RootBeanDefinition custom = new RootBeanDefinition(Object.class);
              custom.setDependsOn("entityManagerFactory");
              custom.setAttribute(
                  "org.springframework.boot.sql.init.dependency.DatabaseInitializerDetector",
                  "example.CustomDatabaseInitializerDetector");
              beanFactory.registerBeanDefinition("customInitializer", custom);
            })
        .withBean(
            "ratchetJpaSchemaInitializerDependency",
            org.springframework.beans.factory.config.BeanFactoryPostProcessor.class,
            () ->
                new RatchetJpaSchemaInitializerDependency(
                    "ratchetJpaSchemaInitializer",
                    new MockEnvironment()
                        .withProperty("spring.jpa.defer-datasource-initialization", "true")))
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertThat(
                      context
                          .getBeanFactory()
                          .getBeanDefinition("entityManagerFactory")
                          .getDependsOn())
                  .contains("ratchetJpaSchemaInitializer");
              assertThat(
                      context
                          .getBeanFactory()
                          .getBeanDefinition("ratchetJpaSchemaInitializer")
                          .getDependsOn())
                  .contains("flyway", "flywayInitializer")
                  .doesNotContain(
                      "dataSourceScriptDatabaseInitializer",
                      "entityManagerFactory",
                      "customInitializer");
              assertThat(context.getBeanFactory().getBeanDefinition("flyway").getDependsOn())
                  .doesNotContain("entityManagerFactory");
              assertThat(
                      context
                          .getBeanFactory()
                          .getBeanDefinition("flywayInitializer")
                          .getDependsOn())
                  .doesNotContain("entityManagerFactory");
              assertThat(
                      context
                          .getBeanFactory()
                          .getBeanDefinition("customInitializer")
                          .getDependsOn())
                  .contains("entityManagerFactory");
            });
  }

  @Test
  void onlyTheNamedPrimaryPersistenceUnitGetsRatchetDependenciesAndAugmentation() {
    DefaultListableBeanFactory beanFactory = persistenceUnitsWithNamedPrimary();
    beanFactory.registerBeanDefinition("ratchetJpaSchemaInitializer", new RootBeanDefinition());

    new RatchetJpaSchemaInitializerDependency("ratchetJpaSchemaInitializer", new MockEnvironment())
        .postProcessBeanFactory(beanFactory);

    assertThat(beanFactory.getBeanDefinition("ordersEntityManagerFactory").getDependsOn())
        .contains("ratchetJpaSchemaInitializer");
    assertThat(beanFactory.getBeanDefinition("entityManagerFactory").getDependsOn()).isNull();
    assertThat(beanFactory.getBeanDefinition("ratchetJpaSchemaInitializer").getDependsOn())
        .contains("dataSource");

    LocalContainerEntityManagerFactoryBean primary =
        mock(LocalContainerEntityManagerFactoryBean.class);
    LocalContainerEntityManagerFactoryBean secondary =
        mock(LocalContainerEntityManagerFactoryBean.class);
    when(primary.getPersistenceProvider()).thenReturn(mock(PersistenceProvider.class));

    RatchetJpaPersistenceUnitPostProcessor processor =
        new RatchetJpaPersistenceUnitPostProcessor(beanFactory);
    processor.postProcessBeforeInitialization(secondary, "entityManagerFactory");
    processor.postProcessBeforeInitialization(primary, "ordersEntityManagerFactory");

    verify(secondary, never()).setPersistenceProvider(any());
    verify(primary).setPersistenceProvider(any());
  }

  @Test
  void providerWrapperRetainsApplicationMappingsAndAddsPostgresqlMetadata() throws Exception {
    DefaultListableBeanFactory beanFactory = persistenceUnitsWithNamedPrimary();
    LocalContainerEntityManagerFactoryBean factory =
        mock(LocalContainerEntityManagerFactoryBean.class);
    PersistenceProvider applicationProvider = mock(PersistenceProvider.class);
    when(factory.getPersistenceProvider()).thenReturn(applicationProvider);

    new RatchetJpaPersistenceUnitPostProcessor(beanFactory)
        .postProcessBeforeInitialization(factory, "ordersEntityManagerFactory");

    ArgumentCaptor<PersistenceProvider> providerCaptor =
        ArgumentCaptor.forClass(PersistenceProvider.class);
    verify(factory).setPersistenceProvider(providerCaptor.capture());

    MutablePersistenceUnitInfo persistenceUnitInfo = new MutablePersistenceUnitInfo();
    persistenceUnitInfo.setPersistenceUnitName("application");
    persistenceUnitInfo.setNonJtaDataSource(postgresqlDataSource());
    persistenceUnitInfo.addManagedClassName("example.app.ApplicationEntity");
    persistenceUnitInfo.addMappingFileName("META-INF/application-orm.xml");

    providerCaptor.getValue().createContainerEntityManagerFactory(persistenceUnitInfo, Map.of());

    ArgumentCaptor<PersistenceUnitInfo> persistenceUnitCaptor =
        ArgumentCaptor.forClass(PersistenceUnitInfo.class);
    verify(applicationProvider)
        .createContainerEntityManagerFactory(persistenceUnitCaptor.capture(), any());
    PersistenceUnitInfo augmented = persistenceUnitCaptor.getValue();
    assertThat(augmented.getManagedClassNames())
        .contains(
            "example.app.ApplicationEntity",
            JobEntity.class.getName(),
            "run.ratchet.store.converter.JobPayloadConverter",
            "run.ratchet.store.converter.JobPriorityConverter",
            "run.ratchet.store.converter.JsonMapConverter",
            "run.ratchet.store.converter.JsonObjectMapConverter",
            "run.ratchet.store.converter.JsonListConverter");
    assertThat(augmented.getMappingFileNames())
        .contains("META-INF/application-orm.xml", "META-INF/orm-postgresql.xml");
    assertThat(SqlStoreVendor.MYSQL.jpaMappingFile()).isEqualTo("META-INF/orm-mysql.xml");
    assertThat(HibernateJpaMappings.sqlserverHibernateMappingFile(6))
        .isEqualTo("META-INF/ratchet/hibernate/orm-sqlserver-6.xml");
    assertThat(HibernateJpaMappings.sqlserverHibernateMappingFile(7))
        .isEqualTo("META-INF/ratchet/hibernate/orm-sqlserver-7.xml");
  }

  @Test
  void ambiguityAndResourceMismatchesProduceActionableDiagnostics() {
    DefaultListableBeanFactory ambiguousDataSources = new DefaultListableBeanFactory();
    register(ambiguousDataSources, "one", DataSource.class, () -> mock(DataSource.class), false);
    register(ambiguousDataSources, "two", DataSource.class, () -> mock(DataSource.class), false);
    assertThatThrownBy(() -> RatchetJpaInfrastructure.selectDataSource(ambiguousDataSources))
        .hasMessageContaining("Ratchet requires one bean for " + DataSource.class.getName());

    DefaultListableBeanFactory ambiguousFactories = new DefaultListableBeanFactory();
    registerPersistenceUnit(ambiguousFactories, "first", false);
    registerPersistenceUnit(ambiguousFactories, "second", false);
    assertThatThrownBy(
            () -> RatchetJpaInfrastructure.selectEntityManagerFactoryBeanName(ambiguousFactories))
        .hasMessageContaining(
            "Ratchet requires one bean for " + EntityManagerFactory.class.getName());

    DefaultListableBeanFactory ambiguousTransactions = new DefaultListableBeanFactory();
    register(
        ambiguousTransactions,
        "first",
        PlatformTransactionManager.class,
        () -> mock(PlatformTransactionManager.class),
        false);
    register(
        ambiguousTransactions,
        "second",
        PlatformTransactionManager.class,
        () -> mock(PlatformTransactionManager.class),
        false);
    assertThatThrownBy(
            () ->
                RatchetJpaInfrastructure.selectTransactionManager(
                    ambiguousTransactions, mock(EntityManagerFactory.class)))
        .hasMessageContaining(
            "Ratchet requires one bean for " + PlatformTransactionManager.class.getName());

    DataSource selectedDataSource = mock(DataSource.class);
    DataSource otherDataSource = mock(DataSource.class);
    EntityManagerFactory wrongDataSourceFactory = entityManagerFactory(otherDataSource);
    DefaultListableBeanFactory mismatchedDataSource = new DefaultListableBeanFactory();
    register(
        mismatchedDataSource,
        "entityManagerFactory",
        EntityManagerFactory.class,
        () -> wrongDataSourceFactory,
        true);
    assertThatThrownBy(
            () ->
                RatchetJpaInfrastructure.selectEntityManagerFactory(
                    mismatchedDataSource, selectedDataSource))
        .hasMessageContaining("does not use the selected DataSource");

    EntityManagerFactory selectedFactory = mock(EntityManagerFactory.class);
    JpaTransactionManager wrongTransactionManager = mock(JpaTransactionManager.class);
    when(wrongTransactionManager.getEntityManagerFactory())
        .thenReturn(mock(EntityManagerFactory.class));
    DefaultListableBeanFactory mismatchedTransactionManager = new DefaultListableBeanFactory();
    register(
        mismatchedTransactionManager,
        "transactionManager",
        PlatformTransactionManager.class,
        () -> wrongTransactionManager,
        true);
    assertThatThrownBy(
            () ->
                RatchetJpaInfrastructure.selectTransactionManager(
                    mismatchedTransactionManager, selectedFactory))
        .hasMessageContaining("does not manage the selected EntityManagerFactory");
  }

  @Test
  void infrastructureSelectionIgnoresScopedAndNonAutowireCandidates() {
    DefaultListableBeanFactory dataSources = new DefaultListableBeanFactory();
    register(dataSources, "dataSource", DataSource.class, () -> mock(DataSource.class), false);
    register(
        dataSources,
        "scopedTarget.dataSource",
        DataSource.class,
        () -> mock(DataSource.class),
        false);
    register(
        dataSources, "ignoredDataSource", DataSource.class, () -> mock(DataSource.class), false);
    dataSources.getBeanDefinition("ignoredDataSource").setAutowireCandidate(false);
    assertThat(
            RatchetJpaInfrastructure.selectPrimaryOrUniqueBeanName(dataSources, DataSource.class))
        .isEqualTo("dataSource");

    DefaultListableBeanFactory factories = new DefaultListableBeanFactory();
    registerPersistenceUnit(factories, "entityManagerFactory", false);
    registerPersistenceUnit(factories, "ignoredEntityManagerFactory", false);
    factories.getBeanDefinition("ignoredEntityManagerFactory").setAutowireCandidate(false);
    assertThat(RatchetJpaInfrastructure.selectEntityManagerFactoryBeanName(factories))
        .isEqualTo("entityManagerFactory");

    DefaultListableBeanFactory transactions = new DefaultListableBeanFactory();
    register(
        transactions,
        "transactionManager",
        PlatformTransactionManager.class,
        () -> mock(PlatformTransactionManager.class),
        false);
    register(
        transactions,
        "ignoredTransactionManager",
        PlatformTransactionManager.class,
        () -> mock(PlatformTransactionManager.class),
        false);
    transactions.getBeanDefinition("ignoredTransactionManager").setAutowireCandidate(false);
    assertThat(
            RatchetJpaInfrastructure.selectPrimaryOrUniqueBeanName(
                transactions, PlatformTransactionManager.class))
        .isEqualTo("transactionManager");
  }

  @Test
  void infrastructureSelectionHonorsDefaultCandidateAfterPrimaryAndBeforeAmbiguity() {
    DefaultListableBeanFactory dataSources = new DefaultListableBeanFactory();
    register(dataSources, "fallback", DataSource.class, () -> mock(DataSource.class), false);
    ((RootBeanDefinition) dataSources.getBeanDefinition("fallback")).setDefaultCandidate(false);
    register(dataSources, "default", DataSource.class, () -> mock(DataSource.class), false);
    assertThat(
            RatchetJpaInfrastructure.selectPrimaryOrUniqueBeanName(dataSources, DataSource.class))
        .isEqualTo("default");

    DefaultListableBeanFactory primary = new DefaultListableBeanFactory();
    register(primary, "default", DataSource.class, () -> mock(DataSource.class), false);
    register(primary, "primary", DataSource.class, () -> mock(DataSource.class), true);
    ((RootBeanDefinition) primary.getBeanDefinition("primary")).setDefaultCandidate(false);
    assertThat(RatchetJpaInfrastructure.selectPrimaryOrUniqueBeanName(primary, DataSource.class))
        .isEqualTo("primary");

    DefaultListableBeanFactory only = new DefaultListableBeanFactory();
    register(only, "only", DataSource.class, () -> mock(DataSource.class), false);
    ((RootBeanDefinition) only.getBeanDefinition("only")).setDefaultCandidate(false);
    assertThat(RatchetJpaInfrastructure.selectPrimaryOrUniqueBeanName(only, DataSource.class))
        .isEqualTo("only");

    DefaultListableBeanFactory multiplePrimary = new DefaultListableBeanFactory();
    register(multiplePrimary, "first", DataSource.class, () -> mock(DataSource.class), true);
    register(multiplePrimary, "second", DataSource.class, () -> mock(DataSource.class), true);
    ((RootBeanDefinition) multiplePrimary.getBeanDefinition("second")).setDefaultCandidate(false);
    assertThatThrownBy(
            () ->
                RatchetJpaInfrastructure.selectPrimaryOrUniqueBeanName(
                    multiplePrimary, DataSource.class))
        .hasMessageContaining("Ratchet requires one bean for " + DataSource.class.getName());
  }

  @Test
  void infrastructureSelectionSelectsTheSoleNonFallbackDataSourceLikeSpringTypeLookup() {
    DefaultListableBeanFactory dataSources = new DefaultListableBeanFactory();
    DataSource preferred = mock(DataSource.class);
    register(dataSources, "preferred", DataSource.class, () -> preferred, false);
    register(dataSources, "fallback", DataSource.class, () -> mock(DataSource.class), false);
    dataSources.getBeanDefinition("fallback").setFallback(true);

    assertThat(dataSources.getBean(DataSource.class)).isSameAs(preferred);
    assertThat(RatchetJpaInfrastructure.selectDataSource(dataSources)).isSameAs(preferred);
  }

  @Test
  void selectsTheLiveScopedDataSourceProxyRatherThanItsTarget() {
    DefaultListableBeanFactory beanFactory = new DefaultListableBeanFactory();
    beanFactory.registerScope("request", new SimpleThreadScope());
    RootBeanDefinition target =
        new RootBeanDefinition(DataSource.class, () -> mock(DataSource.class));
    target.setScope("request");
    BeanDefinitionHolder proxy =
        ScopedProxyUtils.createScopedProxy(
            new BeanDefinitionHolder(target, "dataSource"), beanFactory, false);
    beanFactory.registerBeanDefinition(proxy.getBeanName(), proxy.getBeanDefinition());

    DataSource selected = RatchetJpaInfrastructure.selectDataSource(beanFactory);

    assertThat(proxy.getBeanName()).isEqualTo("dataSource");
    assertThat(org.springframework.aop.support.AopUtils.isAopProxy(selected)).isTrue();
  }

  @Test
  void missingSelectedStoreArtifactNamesTheRequiredDependency() throws Exception {
    ClassLoader classLoader =
        new IsolatedSqlStoreVendorClassLoader(
            new FilteredClassLoader(PostgresqlJobStore.class, PostgresqlJobStoreFactory.class));
    Class<?> vendorType =
        Class.forName(
            "run.ratchet.spring.boot.autoconfigure.jpa.SqlStoreVendor", true, classLoader);
    @SuppressWarnings({"unchecked", "rawtypes"})
    Object postgresql = Enum.valueOf((Class) vendorType.asSubclass(Enum.class), "POSTGRESQL");
    Method createStore =
        vendorType.getDeclaredMethod(
            "createStore", EntityManager.class, RatchetOptions.class, MetricsCollector.class);
    createStore.setAccessible(true);

    assertThatThrownBy(() -> createStore.invoke(postgresql, null, null, null))
        .isInstanceOf(InvocationTargetException.class)
        .cause()
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Add run.ratchet:ratchet-store-postgresql");
  }

  private static DefaultListableBeanFactory persistenceUnitsWithNamedPrimary() {
    DefaultListableBeanFactory beanFactory = new DefaultListableBeanFactory();
    register(beanFactory, "dataSource", DataSource.class, () -> mock(DataSource.class), true);
    registerPersistenceUnit(beanFactory, "entityManagerFactory", false);
    registerPersistenceUnit(beanFactory, "ordersEntityManagerFactory", true);
    return beanFactory;
  }

  private static void registerPersistenceUnit(
      DefaultListableBeanFactory beanFactory, String name, boolean primary) {
    RootBeanDefinition definition =
        new RootBeanDefinition(LocalContainerEntityManagerFactoryBean.class);
    definition.setPrimary(primary);
    beanFactory.registerBeanDefinition(name, definition);
  }

  private static <T> void register(
      DefaultListableBeanFactory beanFactory,
      String name,
      Class<T> type,
      Supplier<T> supplier,
      boolean primary) {
    RootBeanDefinition definition = new RootBeanDefinition(type, supplier);
    definition.setPrimary(primary);
    beanFactory.registerBeanDefinition(name, definition);
  }

  private static EntityManagerFactory entityManagerFactory(DataSource dataSource) {
    EntityManagerFactory entityManagerFactory =
        mock(
            EntityManagerFactory.class,
            withSettings().extraInterfaces(EntityManagerFactoryInfo.class));
    when(((EntityManagerFactoryInfo) entityManagerFactory).getDataSource()).thenReturn(dataSource);
    return entityManagerFactory;
  }

  private static DataSource postgresqlDataSource() throws Exception {
    DataSource dataSource = mock(DataSource.class);
    Connection connection = mock(Connection.class);
    DatabaseMetaData metadata = mock(DatabaseMetaData.class);
    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.getMetaData()).thenReturn(metadata);
    when(metadata.getDatabaseProductName()).thenReturn("PostgreSQL");
    return dataSource;
  }

  private static final class IsolatedSqlStoreVendorClassLoader extends ClassLoader {
    private static final String VENDOR_CLASS =
        "run.ratchet.spring.boot.autoconfigure.jpa.SqlStoreVendor";

    private IsolatedSqlStoreVendorClassLoader(ClassLoader parent) {
      super(parent);
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
      synchronized (getClassLoadingLock(name)) {
        Class<?> loaded = findLoadedClass(name);
        if (loaded == null) {
          loaded =
              name.equals(VENDOR_CLASS) || name.startsWith(VENDOR_CLASS + "$")
                  ? findClass(name)
                  : super.loadClass(name, false);
        }
        if (resolve) {
          resolveClass(loaded);
        }
        return loaded;
      }
    }

    @Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
      if (!name.equals(VENDOR_CLASS) && !name.startsWith(VENDOR_CLASS + "$")) {
        throw new ClassNotFoundException(name);
      }
      String resource = name.replace('.', '/') + ".class";
      try (InputStream input =
          RatchetJpaInfrastructureTest.class.getClassLoader().getResourceAsStream(resource)) {
        if (input == null) {
          throw new ClassNotFoundException(name);
        }
        byte[] bytecode = input.readAllBytes();
        return defineClass(name, bytecode, 0, bytecode.length);
      } catch (IOException e) {
        throw new ClassNotFoundException(name, e);
      }
    }
  }
}
