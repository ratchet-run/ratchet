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

/** Establishes the schema-initializer dependency graph before bean instantiation begins. */
final class RatchetJpaSchemaInitializerDependency implements BeanFactoryPostProcessor {

  private final String initializerBeanName;

  RatchetJpaSchemaInitializerDependency(String initializerBeanName) {
    this.initializerBeanName = initializerBeanName;
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
    if (beanFactory.containsBeanDefinition(entityManagerFactory)) {
      addDependency(beanFactory.getBeanDefinition(entityManagerFactory), initializerBeanName);
    }
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
