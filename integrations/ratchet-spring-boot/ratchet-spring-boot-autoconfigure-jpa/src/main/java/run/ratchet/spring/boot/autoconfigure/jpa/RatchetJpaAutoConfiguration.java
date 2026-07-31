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
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.core.env.Environment;
import org.springframework.core.env.PropertySource;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.SharedEntityManagerCreator;
import org.springframework.orm.jpa.persistenceunit.PersistenceUnitManager;
import org.springframework.transaction.PlatformTransactionManager;
import run.ratchet.api.RatchetOptions;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.spi.NoOpMetricsCollector;
import run.ratchet.store.postgresql.PostgresqlJobStore;
import run.ratchet.store.postgresql.PostgresqlJobStoreFactory;
import run.ratchet.store.postgresql.PostgresqlSchemaMigrationDialect;
import run.ratchet.store.spi.RatchetEntityManagerProvider;

/**
 * Auto-configures the PostgreSQL job store against Spring Boot's single, application-owned JPA
 * persistence unit.
 */
@AutoConfiguration(
    afterName = {
      "org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration",
      "org.springframework.boot.hibernate.autoconfigure.HibernateJpaAutoConfiguration"
    })
@ConditionalOnClass(name = "run.ratchet.store.postgresql.PostgresqlJobStore")
public class RatchetJpaAutoConfiguration {

  static final String MAPPING_RESOURCES_PROPERTY = "spring.jpa.mapping-resources";
  static final String ORM_XML_RESOURCE = "META-INF/orm.xml";

  private static final String STORE_CORE_ANCHOR_RESOURCE =
      "run/ratchet/store/spi/RatchetEntityManagerProvider.class";

  @Bean
  static BeanFactoryPostProcessor ratchetJpaEnvironmentValidator(Environment environment) {
    return beanFactory -> validateJpaEnvironment(beanFactory, environment);
  }

  static void validateJpaEnvironment(
      ConfigurableListableBeanFactory beanFactory, Environment environment) {
    String[] persistenceUnitManagers =
        beanFactory.getBeanNamesForType(PersistenceUnitManager.class, true, false);
    if (persistenceUnitManagers.length > 0) {
      throw new IllegalStateException(
          "Ratchet PostgreSQL JPA auto-configuration requires Spring Boot's default"
              + " PersistenceUnitManager, but found user-provided PersistenceUnitManager bean(s): "
              + beanNames(persistenceUnitManagers)
              + ". Remove the custom PersistenceUnitManager so Spring ORM can discover"
              + " ratchet-store-core's META-INF/orm.xml.");
    }

    if (hasMappingResourcesProperty(environment)) {
      throw new IllegalStateException(
          "Ratchet PostgreSQL JPA auto-configuration cannot run when"
              + " spring.jpa.mapping-resources is set because it suppresses Spring ORM's default"
              + " META-INF/orm.xml discovery. Remove spring.jpa.mapping-resources so the Ratchet"
              + " mapping from ratchet-store-core is included in the application persistence"
              + " unit.");
    }

    List<URL> ormResources = ormXmlResources(beanFactory.getBeanClassLoader());
    if (ormResources.isEmpty()) {
      throw new IllegalStateException(
          "Ratchet PostgreSQL JPA auto-configuration requires exactly one META-INF/orm.xml"
              + " resource from ratchet-store-core, but found none. Ensure exactly one"
              + " ratchet-store-core jar is present on the runtime classpath.");
    }
    if (ormResources.size() > 1) {
      throw new IllegalStateException(
          "Ratchet PostgreSQL JPA auto-configuration requires exactly one META-INF/orm.xml"
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
          "Ratchet PostgreSQL JPA auto-configuration requires the single META-INF/orm.xml"
              + " resource to resolve from the ratchet-store-core jar, but it resolved to "
              + ormResource
              + ". Remove the competing mapping resource and retain ratchet-store-core.");
    }
  }

  static JpaTopology resolveJpaTopology(ConfigurableListableBeanFactory beanFactory) {
    String[] entityManagerFactoryNames =
        beanFactory.getBeanNamesForType(EntityManagerFactory.class, true, true);
    if (entityManagerFactoryNames.length != 1) {
      throw new IllegalStateException(
          "Ratchet PostgreSQL requires exactly one EntityManagerFactory bean, but found "
              + entityManagerFactoryNames.length
              + ": "
              + beanNames(entityManagerFactoryNames)
              + ". Remove additional persistence units or configure a single application-owned"
              + " EntityManagerFactory.");
    }

    String entityManagerFactoryName = entityManagerFactoryNames[0];
    EntityManagerFactory entityManagerFactory =
        beanFactory.getBean(entityManagerFactoryName, EntityManagerFactory.class);

    List<NamedJpaTransactionManager> transactionManagers = jpaTransactionManagers(beanFactory);
    if (transactionManagers.size() != 1) {
      throw new IllegalStateException(
          "Ratchet PostgreSQL requires exactly one JpaTransactionManager bean, but found "
              + transactionManagers.size()
              + ": "
              + transactionManagers.stream().map(NamedJpaTransactionManager::name).sorted().toList()
              + ". Configure one JpaTransactionManager for the application persistence unit.");
    }

    NamedJpaTransactionManager namedTransactionManager = transactionManagers.get(0);
    EntityManagerFactory transactionManagerEntityManagerFactory =
        namedTransactionManager.transactionManager().getEntityManagerFactory();
    if (transactionManagerEntityManagerFactory != entityManagerFactory) {
      throw new IllegalStateException(
          "Ratchet PostgreSQL requires JpaTransactionManager bean '"
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
          "Ratchet PostgreSQL JPA auto-configuration could not enumerate META-INF/orm.xml"
              + " resources.",
          e);
    }
  }

  static boolean isRatchetStoreCoreJarResource(URL ormResource, URL storeCoreAnchor) {
    if (ormResource == null || storeCoreAnchor == null) {
      return false;
    }
    String ormRoot = classpathRoot(ormResource, ORM_XML_RESOURCE);
    String anchorRoot = classpathRoot(storeCoreAnchor, STORE_CORE_ANCHOR_RESOURCE);
    return ormRoot != null && ormRoot.equals(anchorRoot) && identifiesRatchetStoreCoreJar(ormRoot);
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

  @Configuration(proxyBeanMethods = false)
  @ConditionalOnClass(PostgresqlJobStore.class)
  static class PostgresqlStoreConfiguration {

    @Bean
    JpaTopology ratchetJpaTopology(ConfigurableListableBeanFactory beanFactory) {
      return resolveJpaTopology(beanFactory);
    }

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
      return new RatchetJpaSchemaMigrationInitializer(beanFactory, options, dialect);
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
      return PostgresqlJobStoreFactory.create(entityManagerProvider, metricsCollector, options);
    }
  }
}
