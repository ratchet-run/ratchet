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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import run.ratchet.api.Nullable;
import run.ratchet.store.entity.ResourcePermitEntity;
import run.ratchet.store.spi.ResourcePermitStore.PermitSweepPage;

/** Scans persisted permit identities without taking job or resource-limit locks. */
public final class SqlResourcePermitSweep {
  private SqlResourcePermitSweep() {}

  public static PermitSweepPage sweep(
      EntityManager em, @Nullable UUID after, int limit, Function<UUID, ?> bindId) {
    if (limit < 1) throw new IllegalArgumentException("limit must be positive");
    String sql =
        """
        SELECT p.id, p.resource_name, p.job_id, q.claim_seq
        FROM scheduler_resource_permit p
        LEFT JOIN scheduler_job_queue q ON q.job_id = p.job_id AND q.status = 'RUNNING'
        """;
    Query page =
        em.createNativeQuery(sql + (after == null ? "" : "WHERE p.id > ?\n") + "ORDER BY p.id");
    if (after != null) page.setParameter(1, bindId.apply(after));
    List<?> permits = page.setMaxResults(limit).getResultList();
    Set<String> freed = new LinkedHashSet<>();
    UUID last = null;
    for (Object permit : permits) {
      Object[] row = (Object[]) permit;
      last = RowValues.uuidOrNull(row[0]);
      String resource = (String) row[1];
      UUID job = RowValues.uuidOrNull(row[2]);
      if (row[3] instanceof Number claim
          && last.equals(ResourcePermitEntity.idFor(resource, job, claim.longValue()))) {
        continue;
      }
      // A later claim has a different permit id. Once a claim leaves RUNNING it cannot run
      // again under the same sequence, so deleting this id cannot remove a later claim's permit.
      int deleted =
          em.createNativeQuery("DELETE FROM scheduler_resource_permit WHERE id = ?")
              .setParameter(1, bindId.apply(last))
              .executeUpdate();
      if (deleted > 0) freed.add(resource);
    }
    return new PermitSweepPage(permits.size() < limit ? null : last, List.copyOf(freed));
  }
}
