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
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.AnyNestedCondition;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ConfigurationCondition.ConfigurationPhase;
import org.springframework.core.NativeDetector;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.core.env.Environment;
import org.springframework.core.env.PropertySource;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.SharedEntityManagerCreator;
import org.springframework.orm.jpa.persistenceunit.PersistenceUnitManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionManager;
import org.springframework.transaction.annotation.AnnotationTransactionAttributeSource;
import org.springframework.transaction.interceptor.TransactionInterceptor;
import org.springframework.util.ClassUtils;
import run.ratchet.api.RatchetOptions;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.spi.NoOpMetricsCollector;
import run.ratchet.spring.boot.autoconfigure.RatchetProperties;
import run.ratchet.store.mysql.MysqlJobStore;
import run.ratchet.store.mysql.MysqlJobStoreFactory;
import run.ratchet.store.mysql.MysqlSchemaMigrationDialect;
import run.ratchet.store.postgresql.PostgresqlJobStore;
import run.ratchet.store.postgresql.PostgresqlJobStoreFactory;
import run.ratchet.store.postgresql.PostgresqlSchemaMigrationDialect;
import run.ratchet.store.spi.RatchetEntityManagerProvider;

/**
 * Auto-configures a SQL job store against Spring Boot's single, application-owned JPA persistence
 * unit.
 */
@AutoConfiguration(
    afterName = {
      "org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration",
      "org.springframework.boot.hibernate.autoconfigure.HibernateJpaAutoConfiguration"
    })
@Conditional(RatchetJpaAutoConfiguration.AnyJpaStoreCondition.class)
@ConditionalOnProperty(
    name = RatchetProperties.ENABLED_PROPERTY,
    havingValue = "true",
    matchIfMissing = true)
public class RatchetJpaAutoConfiguration {

  static final String MAPPING_RESOURCES_PROPERTY = "spring.jpa.mapping-resources";
  static final String ORM_XML_RESOURCE = "META-INF/orm.xml";
  static final String POSTGRESQL_JOB_STORE_CLASS_NAME =
      "run.ratchet.store.postgresql.PostgresqlJobStore";
  static final String MYSQL_JOB_STORE_CLASS_NAME = "run.ratchet.store.mysql.MysqlJobStore";

  private static final String STORE_CORE_ANCHOR_RESOURCE =
      "run/ratchet/store/spi/RatchetEntityManagerProvider.class";

  @Bean
  static BeanFactoryPostProcessor ratchetJpaEnvironmentValidator(Environment environment) {
    return beanFactory -> {
      validateSingleStoreDependency(beanFactory.getBeanClassLoader());
      validateJpaEnvironment(beanFactory, environment);
    };
  }

  static void validateSingleStoreDependency(ClassLoader classLoader) {
    boolean postgresqlPresent = ClassUtils.isPresent(POSTGRESQL_JOB_STORE_CLASS_NAME, classLoader);
    boolean mysqlPresent = ClassUtils.isPresent(MYSQL_JOB_STORE_CLASS_NAME, classLoader);
    if (postgresqlPresent && mysqlPresent) {
      throw new IllegalStateException(
          "Ratchet JPA auto-configuration found both ratchet-store-postgresql and"
              + " ratchet-store-mysql on the classpath. Keep exactly one ratchet-store-*"
              + " dependency.");
    }
  }

  static void validateJpaEnvironment(
      ConfigurableListableBeanFactory beanFactory, Environment environment) {
    String[] persistenceUnitManagers =
        beanFactory.getBeanNamesForType(PersistenceUnitManager.class, true, false);
    if (persistenceUnitManagers.length > 0) {
      throw new IllegalStateException(
          "Ratchet JPA auto-configuration requires Spring Boot's default"
              + " PersistenceUnitManager, but found user-provided PersistenceUnitManager bean(s): "
              + beanNames(persistenceUnitManagers)
              + ". Remove the custom PersistenceUnitManager so Spring ORM can discover"
              + " ratchet-store-core's META-INF/orm.xml.");
    }

    if (hasMappingResourcesProperty(environment)) {
      throw new IllegalStateException(
          "Ratchet JPA auto-configuration cannot run when"
              + " spring.jpa.mapping-resources is set because it suppresses Spring ORM's default"
              + " META-INF/orm.xml discovery. Remove spring.jpa.mapping-resources so the Ratchet"
              + " mapping from ratchet-store-core is included in the application persistence"
              + " unit.");
    }

    List<URL> ormResources = ormXmlResources(beanFactory.getBeanClassLoader());
    if (ormResources.isEmpty()) {
      throw new IllegalStateException(
          "Ratchet JPA auto-configuration requires exactly one META-INF/orm.xml"
              + " resource from ratchet-store-core, but found none. Ensure exactly one"
              + " ratchet-store-core jar is present on the runtime classpath.");
    }
    if (ormResources.size() > 1) {
      throw new IllegalStateException(
          "Ratchet JPA auto-configuration requires exactly one META-INF/orm.xml"
              + " resource from ratchet-store-core, but found "
              + ormResources.size()
              + ": "
              + ormResources
              + ". Remove duplicate or application-provided META-INF/orm.xml resources.");
    }

    URL ormResource = ormResources.get(0);
    URL storeCoreAnchor =
        RatchetEntityManagerProvider.class.getResource("/" + STORE_CORE_ANCHOR_RESOURCE);
    if (!isRatchetStoreCoreJarResource(ormResource, storeCoreAnchor)) {
      throw new IllegalStateException(
          "Ratchet JPA auto-configuration requires the single META-INF/orm.xml"
              + " resource to resolve from the ratchet-store-core jar, but it resolved to "
              + ormResource
              + ". Remove the competing mapping resource and retain ratchet-store-core.");
    }
  }

  static JpaTopology resolveJpaTopology(
      ConfigurableListableBeanFactory beanFactory, RatchetProperties properties) {
    String[] entityManagerFactoryNames =
        beanFactory.getBeanNamesForType(EntityManagerFactory.class, true, true);
    if (entityManagerFactoryNames.length != 1) {
      throw new IllegalStateException(
          "Ratchet JPA requires exactly one EntityManagerFactory bean, but found "
              + entityManagerFactoryNames.length
              + ": "
              + beanNames(entityManagerFactoryNames)
              + ". Remove additional persistence units or configure a single application-owned"
              + " EntityManagerFactory.");
    }

    String entityManagerFactoryName = entityManagerFactoryNames[0];
    EntityManagerFactory entityManagerFactory =
        beanFactory.getBean(entityManagerFactoryName, EntityManagerFactory.class);

    NamedJpaTransactionManager namedTransactionManager =
        selectJpaTransactionManager(beanFactory, properties);
    EntityManagerFactory transactionManagerEntityManagerFactory =
        namedTransactionManager.transactionManager().getEntityManagerFactory();
    if (transactionManagerEntityManagerFactory != entityManagerFactory) {
      throw new IllegalStateException(
          "Ratchet JPA requires JpaTransactionManager bean '"
              + namedTransactionManager.name()
              + "' to own the selected EntityManagerFactory bean '"
              + entityManagerFactoryName
              + "', but it owns "
              + describeEntityManagerFactory(transactionManagerEntityManagerFactory)
              + ".");
    }

    return new JpaTopology(
        entityManagerFactoryName,
        entityManagerFactory,
        namedTransactionManager.name(),
        namedTransactionManager.transactionManager());
  }

  private static NamedJpaTransactionManager selectJpaTransactionManager(
      ConfigurableListableBeanFactory beanFactory, RatchetProperties properties) {
    String configuredName = properties.getTransactionManagerBeanName();
    if (configuredName != null && !configuredName.isBlank()) {
      return explicitlyNamedTransactionManager(beanFactory, configuredName.trim());
    }

    List<NamedJpaTransactionManager> transactionManagers = jpaTransactionManagers(beanFactory);
    if (transactionManagers.size() == 1) {
      return transactionManagers.get(0);
    }

    List<NamedJpaTransactionManager> primaryTransactionManagers =
        transactionManagers.stream()
            .filter(transactionManager -> isPrimary(beanFactory, transactionManager.name()))
            .toList();
    if (primaryTransactionManagers.size() == 1) {
      return primaryTransactionManagers.get(0);
    }

    throw new IllegalStateException(
        "Ratchet JPA could not select a JpaTransactionManager bean from candidates "
            + transactionManagerNames(transactionManagers)
            + ". Mark exactly one JpaTransactionManager bean @Primary or set '"
            + RatchetProperties.TRANSACTION_MANAGER_BEAN_NAME_PROPERTY
            + "' to the desired bean name.");
  }

  private static NamedJpaTransactionManager explicitlyNamedTransactionManager(
      ConfigurableListableBeanFactory beanFactory, String configuredName) {
    if (!beanFactory.containsBean(configuredName)) {
      throw new IllegalStateException(
          "Ratchet JPA transaction manager bean '"
              + configuredName
              + "' configured by '"
              + RatchetProperties.TRANSACTION_MANAGER_BEAN_NAME_PROPERTY
              + "' does not exist. Available JpaTransactionManager beans: "
              + transactionManagerNames(jpaTransactionManagers(beanFactory))
              + ".");
    }

    Object configuredBean = beanFactory.getBean(configuredName);
    if (!(configuredBean instanceof JpaTransactionManager transactionManager)) {
      throw new IllegalStateException(
          "Ratchet JPA transaction manager bean '"
              + configuredName
              + "' configured by '"
              + RatchetProperties.TRANSACTION_MANAGER_BEAN_NAME_PROPERTY
              + "' must be a JpaTransactionManager, but its actual type is "
              + configuredBean.getClass().getName()
              + ".");
    }
    return new NamedJpaTransactionManager(configuredName, transactionManager);
  }

  private static boolean isPrimary(
      ConfigurableListableBeanFactory beanFactory, String transactionManagerName) {
    return beanFactory.containsBeanDefinition(transactionManagerName)
        && beanFactory.getBeanDefinition(transactionManagerName).isPrimary();
  }

  private static List<String> transactionManagerNames(
      List<NamedJpaTransactionManager> transactionManagers) {
    return transactionManagers.stream().map(NamedJpaTransactionManager::name).sorted().toList();
  }

  private static List<NamedJpaTransactionManager> jpaTransactionManagers(
      ConfigurableListableBeanFactory beanFactory) {
    String[] transactionManagerNames =
        beanFactory.getBeanNamesForType(PlatformTransactionManager.class, true, true);
    List<NamedJpaTransactionManager> transactionManagers = new ArrayList<>();
    for (String transactionManagerName : transactionManagerNames) {
      PlatformTransactionManager transactionManager =
          beanFactory.getBean(transactionManagerName, PlatformTransactionManager.class);
      if (transactionManager instanceof JpaTransactionManager jpaTransactionManager) {
        transactionManagers.add(
            new NamedJpaTransactionManager(transactionManagerName, jpaTransactionManager));
      }
    }
    return transactionManagers;
  }

  private static String describeEntityManagerFactory(EntityManagerFactory entityManagerFactory) {
    if (entityManagerFactory == null) {
      return "no EntityManagerFactory";
    }
    return "a different EntityManagerFactory (" + entityManagerFactory + ")";
  }

  private static boolean hasMappingResourcesProperty(Environment environment) {
    if (environment.containsProperty(MAPPING_RESOURCES_PROPERTY)) {
      return true;
    }
    if (!(environment instanceof ConfigurableEnvironment configurableEnvironment)) {
      return false;
    }
    for (PropertySource<?> propertySource : configurableEnvironment.getPropertySources()) {
      if (propertySource instanceof EnumerablePropertySource<?> enumerablePropertySource) {
        boolean found =
            Stream.of(enumerablePropertySource.getPropertyNames())
                .anyMatch(
                    propertyName ->
                        propertyName.equals(MAPPING_RESOURCES_PROPERTY)
                            || propertyName.startsWith(MAPPING_RESOURCES_PROPERTY + "["));
        if (found) {
          return true;
        }
      }
    }
    return false;
  }

  private static List<URL> ormXmlResources(ClassLoader beanClassLoader) {
    ClassLoader classLoader = beanClassLoader;
    if (classLoader == null) {
      classLoader = Thread.currentThread().getContextClassLoader();
    }
    if (classLoader == null) {
      classLoader = RatchetJpaAutoConfiguration.class.getClassLoader();
    }
    try {
      return Collections.list(classLoader.getResources(ORM_XML_RESOURCE));
    } catch (IOException e) {
      throw new IllegalStateException(
          "Ratchet JPA auto-configuration could not enumerate META-INF/orm.xml resources.", e);
    }
  }

  static boolean isRatchetStoreCoreJarResource(URL ormResource, URL storeCoreAnchor) {
    return isRatchetStoreCoreJarResource(
        ormResource, storeCoreAnchor, NativeDetector.inNativeImage());
  }

  static boolean isRatchetStoreCoreJarResource(
      URL ormResource, URL storeCoreAnchor, boolean nativeImage) {
    if (ormResource == null || storeCoreAnchor == null) {
      return false;
    }
    String ormRoot = classpathRoot(ormResource, ORM_XML_RESOURCE);
    String anchorRoot = classpathRoot(storeCoreAnchor, STORE_CORE_ANCHOR_RESOURCE);
    if (ormRoot == null || !ormRoot.equals(anchorRoot)) {
      return false;
    }
    // process-aot already ran this post-processor's strict jar check on the JVM. Native resource
    // URLs carry no jar identity, and the closed-world image only exposes resources registered
    // from that validated build-time classpath.
    return nativeImage || identifiesRatchetStoreCoreJar(ormRoot);
  }

  private static String classpathRoot(URL resource, String resourceName) {
    String externalForm = resource.toExternalForm();
    return externalForm.endsWith(resourceName)
        ? externalForm.substring(0, externalForm.length() - resourceName.length())
        : null;
  }

  private static boolean identifiesRatchetStoreCoreJar(String classpathRoot) {
    if (!classpathRoot.endsWith(".jar!/")) {
      return false;
    }
    int jarNameStart = classpathRoot.lastIndexOf('/', classpathRoot.length() - ".jar!/".length());
    if (jarNameStart < 0) {
      jarNameStart = classpathRoot.lastIndexOf(':', classpathRoot.length() - ".jar!/".length());
    }
    String jarName = classpathRoot.substring(jarNameStart + 1, classpathRoot.length() - 2);
    return jarName.equals("ratchet-store-core.jar") || jarName.startsWith("ratchet-store-core-");
  }

  private static List<String> beanNames(String[] names) {
    return Arrays.stream(names).sorted().toList();
  }

  record JpaTopology(
      String entityManagerFactoryName,
      EntityManagerFactory entityManagerFactory,
      String transactionManagerName,
      JpaTransactionManager transactionManager) {}

  private record NamedJpaTransactionManager(
      String name, JpaTransactionManager transactionManager) {}

  static class AnyJpaStoreCondition extends AnyNestedCondition {

    AnyJpaStoreCondition() {
      super(ConfigurationPhase.PARSE_CONFIGURATION);
    }

    @ConditionalOnClass(name = POSTGRESQL_JOB_STORE_CLASS_NAME)
    static class PostgresqlStoreAvailable {}

    @ConditionalOnClass(name = MYSQL_JOB_STORE_CLASS_NAME)
    static class MysqlStoreAvailable {}
  }

  @Configuration(proxyBeanMethods = false)
  static class JpaTopologyConfiguration {

    @Bean
    JpaTopology ratchetJpaTopology(
        ConfigurableListableBeanFactory beanFactory, RatchetProperties properties) {
      return resolveJpaTopology(beanFactory, properties);
    }
  }

  @Configuration(proxyBeanMethods = false)
  @ConditionalOnClass(PostgresqlJobStore.class)
  static class PostgresqlStoreConfiguration {

    @Bean
    @ConditionalOnMissingBean(PostgresqlSchemaMigrationDialect.class)
    PostgresqlSchemaMigrationDialect postgresqlSchemaMigrationDialect() {
      return new PostgresqlSchemaMigrationDialect();
    }

    @Bean
    RatchetJpaSchemaMigrationInitializer ratchetJpaSchemaMigrationInitializer(
        ConfigurableListableBeanFactory beanFactory,
        ObjectProvider<RatchetOptions> optionsProvider,
        PostgresqlSchemaMigrationDialect dialect) {
      RatchetOptions options = optionsProvider.getIfAvailable(RatchetOptions::defaults);
      return new RatchetJpaSchemaMigrationInitializer(beanFactory, options, dialect, "postgresql");
    }

    @Bean
    @ConditionalOnMissingBean(PostgresqlJobStore.class)
    PostgresqlJobStore postgresqlJobStore(
        JpaTopology topology,
        RatchetJpaSchemaMigrationInitializer migrationInitializer,
        ObjectProvider<MetricsCollector> metricsCollectorProvider,
        ObjectProvider<RatchetOptions> optionsProvider) {
      Objects.requireNonNull(migrationInitializer, "migrationInitializer");
      EntityManager sharedEntityManager =
          SharedEntityManagerCreator.createSharedEntityManager(topology.entityManagerFactory());
      RatchetEntityManagerProvider entityManagerProvider = () -> sharedEntityManager;
      MetricsCollector metricsCollector =
          metricsCollectorProvider.getIfAvailable(NoOpMetricsCollector::new);
      RatchetOptions options = optionsProvider.getIfAvailable(RatchetOptions::defaults);
      PostgresqlJobStore store =
          PostgresqlJobStoreFactory.create(entityManagerProvider, metricsCollector, options);

      // Native images cannot create the runtime CGLIB proxy, and Spring AOT cannot see the
      // annotated implementation behind this factory method. Build a deterministic JDK proxy.
      ProxyFactory proxyFactory = new ProxyFactory();
      proxyFactory.setTarget(store);
      proxyFactory.setInterfaces(PostgresqlJobStore.class);
      proxyFactory.setProxyTargetClass(false);
      TransactionManager transactionManager = topology.transactionManager();
      proxyFactory.addAdvice(
          new TransactionInterceptor(
              transactionManager, new AnnotationTransactionAttributeSource()));
      return (PostgresqlJobStore) proxyFactory.getProxy();
    }
  }

  @Configuration(proxyBeanMethods = false)
  @ConditionalOnClass(MysqlJobStore.class)
  static class MysqlStoreConfiguration {

    @Bean
    @ConditionalOnMissingBean(MysqlSchemaMigrationDialect.class)
    MysqlSchemaMigrationDialect mysqlSchemaMigrationDialect() {
      return new MysqlSchemaMigrationDialect();
    }

    @Bean
    RatchetJpaSchemaMigrationInitializer mysqlSchemaMigrationInitializer(
        ConfigurableListableBeanFactory beanFactory,
        ObjectProvider<RatchetOptions> optionsProvider,
        MysqlSchemaMigrationDialect dialect) {
      RatchetOptions options = optionsProvider.getIfAvailable(RatchetOptions::defaults);
      return new RatchetJpaSchemaMigrationInitializer(beanFactory, options, dialect, "mysql");
    }

    @Bean
    @ConditionalOnMissingBean(MysqlJobStore.class)
    MysqlJobStore mysqlJobStore(
        JpaTopology topology,
        RatchetJpaSchemaMigrationInitializer migrationInitializer,
        ObjectProvider<MetricsCollector> metricsCollectorProvider,
        ObjectProvider<RatchetOptions> optionsProvider) {
      Objects.requireNonNull(migrationInitializer, "migrationInitializer");
      EntityManager sharedEntityManager =
          SharedEntityManagerCreator.createSharedEntityManager(topology.entityManagerFactory());
      RatchetEntityManagerProvider entityManagerProvider = () -> sharedEntityManager;
      MetricsCollector metricsCollector =
          metricsCollectorProvider.getIfAvailable(NoOpMetricsCollector::new);
      RatchetOptions options = optionsProvider.getIfAvailable(RatchetOptions::defaults);
      MysqlJobStore store =
          MysqlJobStoreFactory.create(entityManagerProvider, metricsCollector, options);

      // Native images cannot create the runtime CGLIB proxy, and Spring AOT cannot see the
      // annotated implementation behind this factory method. Build a deterministic JDK proxy.
      ProxyFactory proxyFactory = new ProxyFactory();
      proxyFactory.setTarget(store);
      proxyFactory.setInterfaces(MysqlJobStore.class);
      proxyFactory.setProxyTargetClass(false);
      TransactionManager transactionManager = topology.transactionManager();
      proxyFactory.addAdvice(
          new TransactionInterceptor(
              transactionManager, new AnnotationTransactionAttributeSource()));
      return (MysqlJobStore) proxyFactory.getProxy();
    }
  }
}
