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

import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;

public final class ArchiveExtensionData {

  private static final int FIND_BY_IDS_CHUNK_SIZE = 500;

  private final Map<UUID, Map<String, String>> propertiesByJobId;
  private final Map<UUID, List<ExtensionArchiveJson.StateRow>> statesByJobId;

  private ArchiveExtensionData(
      Map<UUID, Map<String, String>> propertiesByJobId,
      Map<UUID, List<ExtensionArchiveJson.StateRow>> statesByJobId) {
    this.propertiesByJobId = propertiesByJobId;
    this.statesByJobId = statesByJobId;
  }

  public static ArchiveExtensionData fetch(
      EntityManager em,
      Collection<UUID> jobIds,
      Function<UUID, Object> idEncoder,
      Function<Object, UUID> idDecoder) {
    Objects.requireNonNull(em, "em");
    Objects.requireNonNull(jobIds, "jobIds");
    Objects.requireNonNull(idEncoder, "idEncoder");
    Objects.requireNonNull(idDecoder, "idDecoder");
    if (jobIds.isEmpty()) {
      return new ArchiveExtensionData(Map.of(), Map.of());
    }

    Map<UUID, Map<String, String>> propertiesByJobId = new LinkedHashMap<>();
    Map<UUID, List<ExtensionArchiveJson.StateRow>> statesByJobId = new LinkedHashMap<>();
    List<UUID> ids = jobIds.stream().toList();
    for (int start = 0; start < ids.size(); start += FIND_BY_IDS_CHUNK_SIZE) {
      List<UUID> chunk = ids.subList(start, Math.min(start + FIND_BY_IDS_CHUNK_SIZE, ids.size()));
      fetchProperties(em, chunk, idEncoder, idDecoder, propertiesByJobId);
      fetchStates(em, chunk, idEncoder, idDecoder, statesByJobId);
    }
    return new ArchiveExtensionData(propertiesByJobId, statesByJobId);
  }

  public Map<String, String> properties(UUID jobId) {
    return propertiesByJobId.getOrDefault(jobId, Map.of());
  }

  public List<ExtensionArchiveJson.StateRow> states(UUID jobId) {
    return statesByJobId.getOrDefault(jobId, List.of());
  }

  private static void fetchProperties(
      EntityManager em,
      List<UUID> jobIds,
      Function<UUID, Object> idEncoder,
      Function<Object, UUID> idDecoder,
      Map<UUID, Map<String, String>> propertiesByJobId) {
    String placeholders = String.join(",", Collections.nCopies(jobIds.size(), "?"));
    String sql =
        """
        SELECT job_id, property_key, value
        FROM scheduler_job_properties
        WHERE job_id IN (%s)
        """
            .formatted(placeholders);
    @SuppressWarnings("unchecked")
    List<Object[]> rows = bindIds(em.createNativeQuery(sql), jobIds, idEncoder).getResultList();
    for (Object[] row : rows) {
      UUID jobId = idDecoder.apply(row[0]);
      String value = RowValues.stringOrNull(row[2]);
      if (value != null) {
        propertiesByJobId
            .computeIfAbsent(jobId, ignored -> new LinkedHashMap<>())
            .put((String) row[1], value);
      }
    }
  }

  private static void fetchStates(
      EntityManager em,
      List<UUID> jobIds,
      Function<UUID, Object> idEncoder,
      Function<Object, UUID> idDecoder,
      Map<UUID, List<ExtensionArchiveJson.StateRow>> statesByJobId) {
    String placeholders = String.join(",", Collections.nCopies(jobIds.size(), "?"));
    String sql =
        """
        SELECT job_id, namespace, state, encrypted_state, encryption_key_id, version, updated_at
        FROM scheduler_job_extension_state
        WHERE job_id IN (%s)
        """
            .formatted(placeholders);
    @SuppressWarnings("unchecked")
    List<Object[]> rows = bindIds(em.createNativeQuery(sql), jobIds, idEncoder).getResultList();
    for (Object[] row : rows) {
      UUID jobId = idDecoder.apply(row[0]);
      statesByJobId
          .computeIfAbsent(jobId, ignored -> new ArrayList<>())
          .add(
              new ExtensionArchiveJson.StateRow(
                  (String) row[1],
                  RowValues.stringOrNull(row[2]),
                  RowValues.booleanOrFalse(row[3]),
                  RowValues.stringOrNull(row[4]),
                  ((Number) row[5]).intValue(),
                  RowValues.instantOrNull(row[6])));
    }
  }

  private static Query bindIds(Query query, List<UUID> jobIds, Function<UUID, Object> idEncoder) {
    int parameter = 1;
    for (UUID id : jobIds) {
      query.setParameter(parameter++, idEncoder.apply(id));
    }
    return query;
  }
}
