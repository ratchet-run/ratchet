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

import jakarta.persistence.EntityManagerFactory;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import javax.sql.DataSource;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.orm.jpa.AbstractEntityManagerFactoryBean;
import org.springframework.orm.jpa.EntityManagerFactoryInfo;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

/** Resolves the one coherent DataSource, entity-manager factory, and JPA transaction manager. */
final class RatchetJpaInfrastructure {

  private RatchetJpaInfrastructure() {}

  static DataSource selectDataSource(ConfigurableListableBeanFactory beanFactory) {
    return beanFactory.getBean(
        selectPrimaryOrUniqueBeanName(beanFactory, DataSource.class), DataSource.class);
  }

  static EntityManagerFactory selectEntityManagerFactory(
      ConfigurableListableBeanFactory beanFactory, DataSource dataSource) {
    String name = selectEntityManagerFactoryBeanName(beanFactory);
    EntityManagerFactory entityManagerFactory =
        beanFactory.getBean(name, EntityManagerFactory.class);
    if (!(entityManagerFactory instanceof EntityManagerFactoryInfo info)) {
      throw new IllegalStateException(
          "Ratchet selected EntityManagerFactory bean '"
              + name
              + "' but it does not expose EntityManagerFactoryInfo; use Spring's "
              + "LocalContainerEntityManagerFactoryBean for the selected persistence unit");
    }
    if (info.getDataSource() != dataSource) {
      throw new IllegalStateException(
          "Ratchet selected EntityManagerFactory bean '"
              + name
              + "' does not use the selected DataSource. Mark a coherent DataSource and "
              + "EntityManagerFactory pair @Primary.");
    }
    return entityManagerFactory;
  }

  static JpaTransactionManager selectTransactionManager(
      ConfigurableListableBeanFactory beanFactory, EntityManagerFactory entityManagerFactory) {
    String name = selectPrimaryOrUniqueBeanName(beanFactory, PlatformTransactionManager.class);
    PlatformTransactionManager transactionManager =
        beanFactory.getBean(name, PlatformTransactionManager.class);
    if (!(transactionManager instanceof JpaTransactionManager jpaTransactionManager)) {
      throw new IllegalStateException(
          "Ratchet selected transaction manager bean '"
              + name
              + "' but it is "
              + transactionManager.getClass().getName()
              + "; the selected EntityManagerFactory requires a JpaTransactionManager");
    }
    if (jpaTransactionManager.getEntityManagerFactory() != entityManagerFactory) {
      throw new IllegalStateException(
          "Ratchet selected transaction manager bean '"
              + name
              + "' does not manage the selected EntityManagerFactory. Mark a coherent "
              + "JpaTransactionManager and EntityManagerFactory pair @Primary.");
    }
    return jpaTransactionManager;
  }

  static String selectPrimaryOrUniqueBeanName(
      ConfigurableListableBeanFactory beanFactory, Class<?> type) {
    return selectPrimaryOrUniqueBeanName(
        beanFactory, type, Arrays.asList(beanFactory.getBeanNamesForType(type, true, false)));
  }

  /**
   * Resolves an entity-manager-factory definition before its FactoryBean product exists. Spring
   * exposes {@link AbstractEntityManagerFactoryBean} metadata at this point while the JPA product
   * type may not yet be visible, including in Boot 4.
   */
  static String selectEntityManagerFactoryBeanName(ConfigurableListableBeanFactory beanFactory) {
    Set<String> candidates = new LinkedHashSet<>();
    candidates.addAll(
        factoryBeanNames(
            beanFactory.getBeanNamesForType(AbstractEntityManagerFactoryBean.class, true, false)));
    candidates.addAll(
        factoryBeanNames(beanFactory.getBeanNamesForType(EntityManagerFactory.class, true, false)));
    return selectPrimaryOrUniqueBeanName(
        beanFactory, EntityManagerFactory.class, List.copyOf(candidates));
  }

  private static List<String> factoryBeanNames(String[] names) {
    return Arrays.stream(names)
        .map(name -> name.startsWith("&") ? name.substring(1) : name)
        .toList();
  }

  static String selectPrimaryOrUniqueBeanName(
      ConfigurableListableBeanFactory beanFactory, Class<?> type, List<String> candidates) {
    if (candidates.isEmpty()) {
      throw new IllegalStateException("Ratchet requires a " + type.getSimpleName());
    }
    if (candidates.size() == 1) {
      return candidates.get(0);
    }
    List<String> primaries =
        candidates.stream()
            .filter(
                name ->
                    beanFactory.containsBeanDefinition(name)
                        && beanFactory.getBeanDefinition(name).isPrimary())
            .toList();
    if (primaries.size() == 1) {
      return primaries.get(0);
    }
    throw new IllegalStateException(
        "Ratchet found multiple "
            + type.getSimpleName()
            + " beans "
            + candidates
            + "; declare exactly one or mark one @Primary");
  }
}
