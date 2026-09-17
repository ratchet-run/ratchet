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
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.mockito.Mockito.*;

import jakarta.persistence.spi.ClassTransformer;
import jakarta.persistence.spi.PersistenceUnitInfo;
import java.net.URL;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.orm.jpa.persistenceunit.SmartPersistenceUnitInfo;
import run.ratchet.store.entity.JobEntity;

class RatchetPersistenceMetadataTest {
  private static final Set<String> OVERRIDDEN =
      Set.of(
          "getManagedClassNames",
          "getMappingFileNames",
          "getPersistenceUnitRootUrl",
          "getJarFileUrls");

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void forwardsEveryRuntimeMetadataMethod(boolean enabled) throws Exception {
    PersistenceUnitInfo original =
        mock(
            PersistenceUnitInfo.class,
            withSettings().extraInterfaces(SmartPersistenceUnitInfo.class));
    URL entity = JobEntity.class.getResource("JobEntity.class");
    String root = entity.toExternalForm().replace("run/ratchet/store/entity/JobEntity.class", "");
    ClassLoader loader = mock(ClassLoader.class);
    when(loader.getResource("META-INF/orm.xml")).thenReturn(new URL(root + "META-INF/orm.xml"));
    when(original.getClassLoader()).thenReturn(loader);
    when(original.getManagedClassNames()).thenReturn(List.of("example.Entity"));
    when(original.getMappingFileNames()).thenReturn(List.of("META-INF/orm.xml"));
    PersistenceUnitInfo view =
        enabled
            ? RatchetJpaPersistenceUnitPostProcessor.withRatchetEntities(null, original)
            : RatchetJpaMappings.withoutImplicitRatchetMapping(original);
    assertAll(
        Stream.concat(
                Stream.of(PersistenceUnitInfo.class.getMethods()),
                Stream.of(SmartPersistenceUnitInfo.class.getMethods()))
            .distinct()
            .filter(method -> !OVERRIDDEN.contains(method.getName()))
            .map(
                method ->
                    () -> {
                      Object[] arguments = new Object[method.getParameterCount()];
                      for (int i = 0; i < arguments.length; i++) {
                        arguments[i] =
                            method.getParameterTypes()[i] == String.class
                                ? "example.provider"
                                : mock(ClassTransformer.class);
                      }
                      Object expected = method.invoke(original, arguments);
                      clearInvocations(original);
                      assertThat(method.invoke(view, arguments))
                          .as(method.toString())
                          .isEqualTo(expected);
                      assertThat(mockingDetails(original).getInvocations())
                          .as("Delegates %s", method)
                          .anyMatch(invocation -> invocation.getMethod().equals(method));
                    }));
  }
}
