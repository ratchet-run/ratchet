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
package run.ratchet.micronaut.jpa;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.micronaut.context.ApplicationContext;
import java.util.Map;
import org.junit.jupiter.api.Test;
import run.ratchet.store.spi.JobStore;

class RatchetJpaConfigurationTest {
  @Test
  void selectsSqlserverXmlForTheRuntimeHibernateMajor() {
    assertEquals(
        "META-INF/ratchet/hibernate/orm-sqlserver-6.xml",
        HibernateJpaMappings.sqlserverHibernateMappingFile(6));
    assertEquals(
        "META-INF/ratchet/hibernate/orm-sqlserver-7.xml",
        HibernateJpaMappings.sqlserverHibernateMappingFile(7));
    assertThrows(
        IllegalStateException.class, () -> HibernateJpaMappings.sqlserverHibernateMappingFile(8));
  }

  @Test
  void disabledRatchetCreatesNoJpaInfrastructure() {
    try (ApplicationContext context = ApplicationContext.run(Map.of("ratchet.enabled", false))) {
      assertFalse(context.containsBean(JobStore.class));
      assertFalse(context.containsBean(RatchetJpaSchemaInitializer.class));
      assertFalse(context.containsBean(RatchetJpaMappings.class));
      assertFalse(context.containsBean(RatchetJpaInfrastructure.class));
    }
  }
}
