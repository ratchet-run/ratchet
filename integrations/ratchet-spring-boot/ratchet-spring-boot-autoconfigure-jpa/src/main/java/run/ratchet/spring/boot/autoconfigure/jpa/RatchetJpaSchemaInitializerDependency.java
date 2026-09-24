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

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;
import javax.sql.DataSource;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.core.Ordered;
import org.springframework.core.env.Environment;

/** Establishes Ratchet's schema-initializer dependency graph before bean instantiation begins. */
final class RatchetJpaSchemaInitializerDependency implements BeanFactoryPostProcessor, Ordered {
  // Boot 4 moves this type to spring-boot-sql; Boot uses its stable FQCN as the attribute key.
  private static final String INITIALIZER_DETECTOR_ATTRIBUTE =
      "org.springframework.boot.sql.init.dependency.DatabaseInitializerDetector";

  private final String initializerBeanName;
  private final boolean deferDataSourceInitialization;

  RatchetJpaSchemaInitializerDependency(String initializerBeanName, Environment environment) {
    this.initializerBeanName = initializerBeanName;
    this.deferDataSourceInitialization =
        environment.getProperty("spring.jpa.defer-datasource-initialization", boolean.class, false);
  }

  @Override
  public int getOrder() {
    // Run after Boot records its detected initializer edges and before any bean is instantiated.
    return Ordered.LOWEST_PRECEDENCE;
  }

  @Override
  public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) {
    if (!beanFactory.containsBeanDefinition(initializerBeanName)) {
      return;
    }
    String dataSourceName =
        RatchetJpaInfrastructure.selectPrimaryOrUniqueBeanName(beanFactory, DataSource.class);
    addDependency(beanFactory.getBeanDefinition(initializerBeanName), dataSourceName);
    String entityManagerFactory =
        RatchetJpaInfrastructure.selectEntityManagerFactoryBeanName(beanFactory);
    for (String candidate : beanFactory.getBeanDefinitionNames()) {
      if (candidate.equals(initializerBeanName) || !isEarlyInitializer(beanFactory, candidate)) {
        continue;
      }
      BeanDefinition candidateDefinition = beanFactory.getBeanDefinition(candidate);
      if (deferDataSourceInitialization && isKnownMigrationInitializer(candidateDefinition)) {
        // Boot defers all database initializers behind JPA. Restore only its known migration
        // initializers before Ratchet and JPA; a custom initializer may legitimately require JPA.
        removeDependency(candidateDefinition, entityManagerFactory);
      }
      addDependency(beanFactory.getBeanDefinition(initializerBeanName), candidate);
    }
    if (beanFactory.containsBeanDefinition(entityManagerFactory)) {
      addDependency(beanFactory.getBeanDefinition(entityManagerFactory), initializerBeanName);
    }
  }

  private boolean isEarlyInitializer(ConfigurableListableBeanFactory beanFactory, String name) {
    Object detector =
        beanFactory.getBeanDefinition(name).getAttribute(INITIALIZER_DETECTOR_ATTRIBUTE);
    if (!(detector instanceof String detectorName)
        || detectorName.endsWith("JpaDatabaseInitializerDetector")) {
      return false;
    }
    return !deferDataSourceInitialization || isKnownMigrationInitializer(detectorName);
  }

  private static boolean isKnownMigrationInitializer(BeanDefinition beanDefinition) {
    Object detector = beanDefinition.getAttribute(INITIALIZER_DETECTOR_ATTRIBUTE);
    return detector instanceof String detectorName && isKnownMigrationInitializer(detectorName);
  }

  private static boolean isKnownMigrationInitializer(String detectorName) {
    String simpleName = detectorName.substring(detectorName.lastIndexOf('.') + 1);
    return simpleName.equals("FlywayDatabaseInitializerDetector")
        || simpleName.equals("FlywayMigrationInitializerDatabaseInitializerDetector")
        || simpleName.equals("LiquibaseDatabaseInitializerDetector");
  }

  private static void removeDependency(BeanDefinition beanDefinition, String dependency) {
    if (beanDefinition.getDependsOn() == null) {
      return;
    }
    beanDefinition.setDependsOn(
        Arrays.stream(beanDefinition.getDependsOn())
            .filter(candidate -> !candidate.equals(dependency))
            .toArray(String[]::new));
  }

  private static void addDependency(BeanDefinition beanDefinition, String dependency) {
    Set<String> dependencies = new LinkedHashSet<>();
    if (beanDefinition.getDependsOn() != null) {
      dependencies.addAll(Arrays.asList(beanDefinition.getDependsOn()));
    }
    dependencies.add(dependency);
    beanDefinition.setDependsOn(dependencies.toArray(String[]::new));
  }
}
