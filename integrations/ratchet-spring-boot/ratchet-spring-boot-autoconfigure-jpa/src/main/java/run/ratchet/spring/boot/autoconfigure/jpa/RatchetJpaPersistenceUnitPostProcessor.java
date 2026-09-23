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
import java.util.stream.Stream;
import javax.sql.DataSource;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import run.ratchet.spring.boot.autoconfigure.internal.jpa.RatchetJpaAotSettings;
import run.ratchet.spring.boot.autoconfigure.internal.jpa.RatchetJpaMappings;
import run.ratchet.spring.boot.autoconfigure.internal.jpa.RatchetPersistenceProvider;
import run.ratchet.store.schema.RatchetJpaModel;

/** Adds Ratchet's entities and converters while retaining application persistence metadata. */
final class RatchetJpaPersistenceUnitPostProcessor implements BeanPostProcessor {

  private final ConfigurableListableBeanFactory beanFactory;

  RatchetJpaPersistenceUnitPostProcessor(ConfigurableListableBeanFactory beanFactory) {
    this.beanFactory = beanFactory;
  }

  private static final List<String> RATCHET_MANAGED_CLASS_NAMES =
      Stream.concat(
              RatchetJpaModel.ENTITY_CLASS_NAMES.stream(),
              RatchetJpaModel.CONVERTER_CLASS_NAMES.stream())
          .toList();

  @Override
  public Object postProcessBeforeInitialization(Object bean, String beanName) {
    if (bean instanceof LocalContainerEntityManagerFactoryBean entityManagerFactory
        && beanName.equals(
            RatchetJpaInfrastructure.selectEntityManagerFactoryBeanName(beanFactory))) {
      RatchetPersistenceProvider.install(
          entityManagerFactory, RatchetJpaPersistenceUnitPostProcessor::withRatchetEntities, true);
    }
    return bean;
  }

  static PersistenceUnitInfo withRatchetEntities(
      PersistenceProvider provider, PersistenceUnitInfo unit) {
    return RatchetJpaMappings.transform(
        unit, RATCHET_MANAGED_CLASS_NAMES, ratchetVendorMappingFile(provider, unit));
  }

  private static String ratchetVendorMappingFile(
      PersistenceProvider provider, PersistenceUnitInfo persistenceUnitInfo) {
    String aotMapping = RatchetJpaAotSettings.get("mapping");
    if (aotMapping != null) return aotMapping;
    DataSource dataSource = persistenceUnitInfo.getNonJtaDataSource();
    if (dataSource == null) {
      dataSource = persistenceUnitInfo.getJtaDataSource();
    }
    return dataSource == null
        ? null
        : HibernateJpaMappings.mappingFile(provider, SqlStoreVendor.detect(dataSource));
  }
}
