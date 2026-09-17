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

import jakarta.persistence.spi.PersistenceProvider;
import jakarta.persistence.spi.PersistenceUnitInfo;
import java.util.List;
import javax.sql.DataSource;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;

/** Adds Ratchet's entities and converters while retaining application persistence metadata. */
final class RatchetJpaPersistenceUnitPostProcessor implements BeanPostProcessor {

  private final ConfigurableListableBeanFactory beanFactory;

  RatchetJpaPersistenceUnitPostProcessor(ConfigurableListableBeanFactory beanFactory) {
    this.beanFactory = beanFactory;
  }

  // Names must not load entities before a provider installs its class transformer.
  private static List<String> ratchetManagedClassNames() {
    return List.of(
        "run.ratchet.store.entity.ArchivedJobEntity",
        "run.ratchet.store.entity.BatchEntity",
        "run.ratchet.store.entity.BatchMetricsEntity",
        "run.ratchet.store.entity.JobEntity",
        "run.ratchet.store.entity.JobExecutionEntity",
        "run.ratchet.store.entity.JobLogEntity",
        "run.ratchet.store.entity.NodeEntity",
        "run.ratchet.store.entity.ResourceLimitEntity",
        "run.ratchet.store.entity.ResourcePermitEntity",
        "run.ratchet.store.entity.WorkflowConditionEntity",
        "run.ratchet.store.converter.JobPayloadConverter",
        "run.ratchet.store.converter.JobPriorityConverter",
        "run.ratchet.store.converter.JsonMapConverter",
        "run.ratchet.store.converter.JsonObjectMapConverter",
        "run.ratchet.store.converter.JsonListConverter");
  }

  @Override
  public Object postProcessBeforeInitialization(Object bean, String beanName) {
    if (bean instanceof LocalContainerEntityManagerFactoryBean entityManagerFactory
        && beanName.equals(
            RatchetJpaInfrastructure.selectEntityManagerFactoryBeanName(beanFactory))) {
      installEntityAugmentingProvider(entityManagerFactory);
    }
    return bean;
  }

  private static void installEntityAugmentingProvider(
      LocalContainerEntityManagerFactoryBean entityManagerFactory) {
    RatchetPersistenceProvider.install(
        entityManagerFactory, RatchetJpaPersistenceUnitPostProcessor::withRatchetEntities, true);
  }

  static PersistenceUnitInfo withRatchetEntities(
      PersistenceProvider provider, PersistenceUnitInfo unit) {
    return RatchetJpaMappings.transform(
        unit, ratchetManagedClassNames(), ratchetVendorMappingFile(provider, unit));
  }

  private static String ratchetVendorMappingFile(
      PersistenceProvider provider, PersistenceUnitInfo persistenceUnitInfo) {
    DataSource dataSource = persistenceUnitInfo.getNonJtaDataSource();
    if (dataSource == null) {
      dataSource = persistenceUnitInfo.getJtaDataSource();
    }
    return dataSource == null
        ? null
        : HibernateJpaMappings.mappingFile(provider, SqlStoreVendor.detect(dataSource));
  }
}
