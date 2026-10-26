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
package run.ratchet.store.util;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class ArchiveExtensionDataTest {

  @Test
  void absentSurfacesDoNotReadEitherTable() {
    var em = mock(EntityManager.class);
    var id = UUID.randomUUID();
    var data =
        ArchiveExtensionData.fetch(
            em, List.of(id), false, false, value -> value, RowValues::uuidOrNull);
    assertTrue(data.properties(id).isEmpty());
    assertTrue(data.states(id).isEmpty());
    verifyNoInteractions(em);
  }

  @Test
  void propertyOnlyStoreDoesNotReadStateTable() {
    var em = emptyResults();
    ArchiveExtensionData.fetch(
        em, List.of(UUID.randomUUID()), true, false, value -> value, RowValues::uuidOrNull);
    verify(em).createNativeQuery(contains("FROM scheduler_job_properties"));
    verify(em, never()).createNativeQuery(contains("FROM scheduler_job_extension_state"));
  }

  @Test
  void stateOnlyStoreDoesNotReadPropertyTable() {
    var em = emptyResults();
    ArchiveExtensionData.fetch(
        em, List.of(UUID.randomUUID()), false, true, value -> value, RowValues::uuidOrNull);
    verify(em).createNativeQuery(contains("FROM scheduler_job_extension_state"));
    verify(em, never()).createNativeQuery(contains("FROM scheduler_job_properties"));
  }

  private static EntityManager emptyResults() {
    var em = mock(EntityManager.class);
    var query = mock(Query.class);
    when(em.createNativeQuery(anyString())).thenReturn(query);
    when(query.getResultList()).thenReturn(List.of());
    return em;
  }
}
