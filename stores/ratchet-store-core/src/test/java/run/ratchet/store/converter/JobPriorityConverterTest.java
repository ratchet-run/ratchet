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
package run.ratchet.store.converter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.persistence.Convert;
import jakarta.persistence.Enumerated;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobPriority;
import run.ratchet.store.entity.ArchivedJobEntity;
import run.ratchet.store.entity.JobEntity;

class JobPriorityConverterTest {

  private final JobPriorityConverter converter = new JobPriorityConverter();

  @Test
  void writesStablePersistedCodeRatherThanEnumPosition() {
    for (JobPriority priority : JobPriority.values()) {
      assertEquals(priority.persistedCode(), converter.convertToDatabaseColumn(priority));
    }
    assertNull(converter.convertToDatabaseColumn(null));
  }

  @Test
  void readsExistingPersistedCodes() {
    for (JobPriority priority : JobPriority.values()) {
      assertEquals(priority, converter.convertToEntityAttribute(priority.persistedCode()));
    }
    assertNull(converter.convertToEntityAttribute(null));
  }

  @Test
  void rejectsUnknownPersistedCode() {
    assertThrows(IllegalArgumentException.class, () -> converter.convertToEntityAttribute(99));
  }

  @Test
  void jobEntitiesUseStableCodeConverterInsteadOfJpaOrdinalMapping() throws Exception {
    assertUsesStableCodeConverter(JobEntity.class.getDeclaredField("priority"));
    assertUsesStableCodeConverter(ArchivedJobEntity.class.getDeclaredField("priority"));
  }

  @Test
  void coreOrmRegistersConverterForExcludeUnlistedPersistenceUnits() throws IOException {
    try (InputStream mapping =
        JobPriorityConverterTest.class.getClassLoader().getResourceAsStream("META-INF/orm.xml")) {
      assertNotNull(mapping);
      String xml = new String(mapping.readAllBytes(), StandardCharsets.UTF_8);

      assertTrue(
          xml.contains("<converter class=\"run.ratchet.store.converter.JobPriorityConverter\"/>"));
    }
  }

  private static void assertUsesStableCodeConverter(Field field) {
    assertEquals(JobPriorityConverter.class, field.getAnnotation(Convert.class).converter());
    assertNull(field.getAnnotation(Enumerated.class));
  }
}
