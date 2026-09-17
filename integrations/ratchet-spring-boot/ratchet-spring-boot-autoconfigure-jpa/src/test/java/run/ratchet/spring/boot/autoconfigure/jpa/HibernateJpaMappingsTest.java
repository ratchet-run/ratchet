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

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.mock;

import jakarta.persistence.spi.PersistenceProvider;
import java.net.URL;
import java.net.URLClassLoader;
import org.hibernate.jpa.HibernatePersistenceProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class HibernateJpaMappingsTest {
  @ParameterizedTest
  @EnumSource(SqlStoreVendor.class)
  void selectsAdaptationsOnlyForTheActualHibernateProvider(SqlStoreVendor vendor) {
    assertThat(HibernateJpaMappings.mappingFile(mock(PersistenceProvider.class), vendor))
        .isEqualTo(vendor.jpaMappingFile());
    String mapping = HibernateJpaMappings.mappingFile(new HibernatePersistenceProvider(), vendor);
    assertThat(getClass().getClassLoader().getResource(mapping)).isNotNull();
    if (vendor == SqlStoreVendor.POSTGRESQL) {
      assertThat(mapping).isEqualTo(vendor.jpaMappingFile());
    } else {
      assertThat(mapping).startsWith("META-INF/ratchet/hibernate/");
    }
  }

  @Test
  void unsupportedHibernateVersionHasAnActionableFailure() {
    assertThatThrownBy(() -> HibernateJpaMappings.sqlserverHibernateMappingFile(8))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("SQL Server BINARY(16)")
        .hasMessageContaining("Hibernate 6 or 7");
  }

  @Test
  void standardMappingSelectionLoadsWithoutHibernate() throws Exception {
    URL source = HibernateJpaMappings.class.getProtectionDomain().getCodeSource().getLocation();
    try (var loader =
        new URLClassLoader(new URL[] {source}, getClass().getClassLoader()) {
          @Override
          protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            synchronized (getClassLoadingLock(name)) {
              if (name.startsWith("org.hibernate.")) throw new ClassNotFoundException(name);
              if (name.startsWith(HibernateJpaMappings.class.getName())
                  || name.startsWith(SqlStoreVendor.class.getName())) {
                Class<?> loaded = findLoadedClass(name);
                if (loaded == null) loaded = findClass(name);
                if (resolve) resolveClass(loaded);
                return loaded;
              }
              return super.loadClass(name, resolve);
            }
          }
        }) {
      Class<?> vendorClass = loader.loadClass(SqlStoreVendor.class.getName());
      Class<?> mappings = loader.loadClass(HibernateJpaMappings.class.getName());
      var select =
          mappings.getDeclaredMethod("mappingFile", PersistenceProvider.class, vendorClass);
      select.setAccessible(true);
      PersistenceProvider provider = mock(PersistenceProvider.class);
      for (Object vendor : vendorClass.getEnumConstants()) {
        assertThat(select.invoke(null, provider, vendor))
            .isEqualTo(SqlStoreVendor.valueOf(vendor.toString()).jpaMappingFile());
      }
    }
  }
}
