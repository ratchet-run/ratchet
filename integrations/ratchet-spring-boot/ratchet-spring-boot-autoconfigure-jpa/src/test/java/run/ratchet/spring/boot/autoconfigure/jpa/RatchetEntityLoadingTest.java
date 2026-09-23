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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.persistence.spi.PersistenceProvider;
import jakarta.persistence.spi.PersistenceUnitInfo;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import org.junit.jupiter.api.Test;
import run.ratchet.spring.boot.autoconfigure.internal.jpa.RatchetJpaMappings;

class RatchetEntityLoadingTest {
  @Test
  void metadataLeavesEntityLoadingToThePersistenceProvider() throws Exception {
    URL mappingsSource =
        RatchetJpaMappings.class.getProtectionDomain().getCodeSource().getLocation();
    URL postProcessorSource =
        RatchetJpaPersistenceUnitPostProcessor.class
            .getProtectionDomain()
            .getCodeSource()
            .getLocation();
    try (var loader =
        new URLClassLoader(
            new URL[] {mappingsSource, postProcessorSource}, getClass().getClassLoader()) {
          @Override
          protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            synchronized (getClassLoadingLock(name)) {
              if (name.startsWith("run.ratchet.store.entity.")
                  || name.startsWith("run.ratchet.store.converter.")) {
                throw new AssertionError("Metadata prematurely loaded " + name);
              }
              if (name.startsWith("run.ratchet.spring.boot.autoconfigure.jpa.")
                  || name.startsWith("run.ratchet.spring.boot.autoconfigure.internal.jpa.")) {
                Class<?> type = findLoadedClass(name);
                if (type == null) type = findClass(name);
                if (resolve) resolveClass(type);
                return type;
              }
              return super.loadClass(name, resolve);
            }
          }
        }) {
      PersistenceUnitInfo original = mock(PersistenceUnitInfo.class);
      when(original.getClassLoader()).thenReturn(loader);
      when(original.getManagedClassNames()).thenReturn(List.of("example.HostEntity"));
      when(original.getMappingFileNames()).thenReturn(List.of("application-orm.xml"));
      when(original.getJarFileUrls()).thenReturn(List.of());
      Class<?> processor = loader.loadClass(RatchetJpaPersistenceUnitPostProcessor.class.getName());
      Class<?> mappings = loader.loadClass(RatchetJpaMappings.class.getName());
      assertThat(processor.getClassLoader()).isSameAs(loader);
      assertThat(mappings.getClassLoader()).isSameAs(loader);
      Method transform =
          processor.getDeclaredMethod(
              "withRatchetEntities", PersistenceProvider.class, PersistenceUnitInfo.class);
      transform.setAccessible(true);
      PersistenceUnitInfo view = (PersistenceUnitInfo) transform.invoke(null, null, original);
      assertThat(view.getManagedClassNames())
          .hasSize(16)
          .contains(
              "example.HostEntity",
              "run.ratchet.store.entity.BatchMetricsEntity",
              "run.ratchet.store.converter.JobPayloadConverter");
      assertThat(view.getMappingFileNames()).containsExactly("application-orm.xml");
    }
  }
}
