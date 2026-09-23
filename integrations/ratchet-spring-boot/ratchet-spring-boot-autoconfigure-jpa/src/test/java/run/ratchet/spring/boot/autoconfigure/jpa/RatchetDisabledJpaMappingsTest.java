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

import java.net.URL;
import org.junit.jupiter.api.Test;
import org.springframework.orm.jpa.persistenceunit.MutablePersistenceUnitInfo;
import org.springframework.orm.jpa.persistenceunit.SmartPersistenceUnitInfo;
import run.ratchet.spring.boot.autoconfigure.internal.jpa.RatchetJpaMappings;
import run.ratchet.store.entity.JobEntity;

class RatchetDisabledJpaMappingsTest {
  @Test
  void removesOnlyRatchetImplicitResourceAndItsScannerRoot() throws Exception {
    URL entity = JobEntity.class.getResource("JobEntity.class");
    String root = entity.toExternalForm().replace("run/ratchet/store/entity/JobEntity.class", "");
    URL orm = new URL(root + "META-INF/orm.xml");
    var unit = unitWithDefaultOrm(orm);
    unit.setPersistenceUnitRootUrl(new URL(root));
    unit.addJarFileUrl(new URL(root));
    unit.addJarFileUrl(new URL("file:/application.jar"));
    unit.addMappingFileName("META-INF/orm.xml");
    unit.addMappingFileName("META-INF/application-mappings.xml");
    unit.addManagedClassName("example.ApplicationEntity");
    unit.addProperty("hibernate.hbm2ddl.auto", "validate");

    var filtered = RatchetJpaMappings.withoutImplicitRatchetMapping(unit);

    assertThat(filtered.getMappingFileNames()).containsExactly("META-INF/application-mappings.xml");
    assertThat(filtered.getManagedClassNames()).containsExactly("example.ApplicationEntity");
    assertThat(filtered.getJarFileUrls()).containsExactly(new URL("file:/application.jar"));
    assertThat(filtered.getPersistenceUnitRootUrl()).isNull();
    assertThat(filtered.getProperties()).isSameAs(unit.getProperties());
    assertThat(filtered.getClassLoader()).isSameAs(unit.getClassLoader());
  }

  @Test
  void enabledUnitReplacesOnlyRatchetDefaultMappingWithExplicitEntitiesAndConverters()
      throws Exception {
    URL entity = JobEntity.class.getResource("JobEntity.class");
    String root = entity.toExternalForm().replace("run/ratchet/store/entity/JobEntity.class", "");
    var unit = unitWithDefaultOrm(new URL(root + "META-INF/orm.xml"));
    unit.setPersistenceUnitRootUrl(new URL(root));
    unit.addMappingFileName("META-INF/orm.xml");
    unit.addMappingFileName("META-INF/application-mappings.xml");
    unit.addManagedClassName("example.ApplicationEntity");
    unit.addManagedPackage("example");

    var augmented = RatchetJpaPersistenceUnitPostProcessor.withRatchetEntities(null, unit);

    assertThat(augmented.getMappingFileNames())
        .containsExactly("META-INF/application-mappings.xml");
    assertThat(augmented.getManagedClassNames())
        .contains("example.ApplicationEntity", JobEntity.class.getName())
        .hasSize(16);
    assertThat(augmented.getPersistenceUnitRootUrl()).isNull();
    assertThat(((SmartPersistenceUnitInfo) augmented).getManagedPackages())
        .containsExactly("example");
  }

  @Test
  void applicationOwnedDefaultOrmAndExplicitMappingsRemainUntouched() throws Exception {
    var unit = unitWithDefaultOrm(new URL("file:/application/META-INF/orm.xml"));
    unit.addMappingFileName("META-INF/orm.xml");
    unit.addMappingFileName("META-INF/other.xml");
    unit.setPersistenceUnitRootUrl(new URL("file:/application/"));
    assertThat(RatchetJpaMappings.withoutImplicitRatchetMapping(unit)).isSameAs(unit);
  }

  private static MutablePersistenceUnitInfo unitWithDefaultOrm(URL resource) {
    ClassLoader loader =
        new ClassLoader(JobEntity.class.getClassLoader()) {
          @Override
          public URL getResource(String name) {
            return name.equals("META-INF/orm.xml") ? resource : super.getResource(name);
          }
        };
    return new MutablePersistenceUnitInfo() {
      @Override
      public ClassLoader getClassLoader() {
        return loader;
      }
    };
  }
}
