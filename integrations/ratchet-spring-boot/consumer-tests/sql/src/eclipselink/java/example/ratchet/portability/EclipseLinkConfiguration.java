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
package example.ratchet.portability;

import jakarta.persistence.EntityManagerFactory;
import java.util.Map;
import javax.sql.DataSource;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigurationPackages;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.persistenceunit.PersistenceManagedTypes;
import org.springframework.orm.jpa.vendor.EclipseLinkJpaVendorAdapter;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.util.ClassUtils;

/** Application-owned provider configuration, included only in the EclipseLink consumer profile. */
@AutoConfiguration(
    afterName = {
      "org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration",
      "org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration"
    },
    beforeName = {
      "run.ratchet.spring.boot.autoconfigure.jpa.RatchetJpaAutoConfiguration",
      "run.ratchet.spring.boot.autoconfigure.jpa.RatchetDisabledJpaAutoConfiguration"
    })
@EnableTransactionManagement
@PropertySource("classpath:eclipselink.properties")
public class EclipseLinkConfiguration {
  @Bean
  LocalContainerEntityManagerFactoryBean entityManagerFactory(
      DataSource dataSource,
      ConfigurableListableBeanFactory beans,
      ObjectProvider<PersistenceManagedTypes> managedTypes,
      Environment environment) {
    if (ClassUtils.isPresent("org.hibernate.Session", getClass().getClassLoader())) {
      throw new IllegalStateException("The EclipseLink consumer must run without Hibernate");
    }
    var factory = new LocalContainerEntityManagerFactoryBean();
    factory.setDataSource(dataSource);
    factory.setJpaVendorAdapter(new EclipseLinkJpaVendorAdapter());
    PersistenceManagedTypes types = managedTypes.getIfAvailable();
    if (types == null) {
      factory.setPackagesToScan(AutoConfigurationPackages.get(beans).toArray(String[]::new));
    } else {
      factory.setManagedTypes(types);
    }
    factory.setMappingResources(
        environment.getProperty("spring.jpa.mapping-resources", String[].class, new String[0]));
    factory.setJpaPropertyMap(
        Map.of(
            "eclipselink.weaving", "false",
            "eclipselink.cache.shared.default", "false",
            "eclipselink.logging.level", "WARNING",
            "jakarta.persistence.schema-generation.database.action", "none"));
    return factory;
  }

  @Bean
  JpaTransactionManager transactionManager(EntityManagerFactory factory) {
    return new JpaTransactionManager(factory);
  }
}
