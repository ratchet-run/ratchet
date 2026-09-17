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

import java.sql.Timestamp;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import run.ratchet.store.context.AbstractSqlStoreContext;
import run.ratchet.store.entity.JobEntity;

/** Permanent key reservations; callers must join the job creation transaction. */
public final class SqlIdempotencyKeys {
  private SqlIdempotencyKeys() {}

  // The EntityManager is borrowed from the store context; its owner controls its lifecycle.
  @SuppressWarnings("AutoCloseableResource")
  public static Optional<UUID> find(AbstractSqlStoreContext ctx, String key) {
    try {
      List<?> rows =
          ctx.em()
              .createNativeQuery(
                  "SELECT original_job_id FROM scheduler_idempotency_key WHERE idempotency_key = ?")
              .setParameter(1, key)
              .getResultList();
      return rows.isEmpty() ? Optional.empty() : Optional.of(RowValues.uuidOrNull(rows.get(0)));
    } catch (RuntimeException failure) {
      throw ctx.translateTransientStoreException("find permanent idempotency key", failure);
    }
  }

  // The EntityManager is borrowed from the store context; its owner controls its lifecycle.
  @SuppressWarnings("AutoCloseableResource")
  public static void reserve(
      AbstractSqlStoreContext ctx,
      List<JobEntity> jobs,
      Timestamp now,
      Function<UUID, Object> encodeId,
      boolean oracle) {
    for (int start = 0; start < jobs.size(); start += 500) {
      List<JobEntity> chunk = jobs.subList(start, Math.min(start + 500, jobs.size()));
      StringBuilder sql =
          new StringBuilder(
              oracle
                  ? "INSERT ALL "
                  : "INSERT INTO scheduler_idempotency_key (idempotency_key, original_job_id, reserved_at) VALUES ");
      for (int n = 0; n < chunk.size(); n++) {
        if (oracle)
          sql.append(
              "INTO scheduler_idempotency_key (idempotency_key, original_job_id, reserved_at) VALUES (?, ?, ?) ");
        else {
          if (n > 0) sql.append(",");
          sql.append("(?, ?, ?)");
        }
      }
      if (oracle) sql.append("SELECT 1 FROM dual");
      var query = ctx.em().createNativeQuery(sql.toString());
      int parameter = 1;
      for (JobEntity job : chunk) {
        query.setParameter(parameter++, job.getIdempotencyKey());
        query.setParameter(parameter++, encodeId.apply(job.getId()));
        query.setParameter(parameter++, now);
      }
      query.executeUpdate();
    }
  }
}
