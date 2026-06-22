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
package run.ratchet.store.sqlserver;

import java.util.List;
import java.util.UUID;
import org.jboss.logging.Logger;
import run.ratchet.api.JobStatus;
import run.ratchet.store.sqlserver.converter.UuidByteArrayConverter;

final class SqlserverJobStatusTransitions {

  private static final Logger log = Logger.getLogger(SqlserverJobStatusTransitions.class);

  private final SqlserverStoreContext ctx;

  SqlserverJobStatusTransitions(SqlserverStoreContext ctx) {
    this.ctx = ctx;
  }

  boolean tryPickUpJob(UUID id, String nodeId) {
    // language=SQL Server
    String sql =
        """
        UPDATE scheduler_job_queue
        SET status = 'RUNNING', picked_by = ?, picked_at = SYSUTCDATETIME(),
            updated_at = SYSUTCDATETIME()
        WHERE job_id = ? AND status = 'PENDING'
        """;
    return ctx.timedStoreOperation(
            "pickup_job",
            () ->
                ctx.em()
                    .createNativeQuery(sql)
                    .setParameter(1, nodeId)
                    .setParameter(2, UuidByteArrayConverter.toBytes(id))
                    .executeUpdate(),
            updated -> updated > 0 ? "updated" : "miss")
        > 0;
  }

  boolean transitionToPaused(UUID id, JobStatus expected) {
    if (expected == JobStatus.PAUSED) {
      throw new IllegalArgumentException("transitionToPaused expects expected != PAUSED");
    }
    if (expected == JobStatus.WAITING) {
      log.debugf("transitionToPaused(%s, WAITING) is a no-op — waiting jobs cannot be paused", id);
      return false;
    }
    if (!SqlserverJobRowMapper.isLiveStatus(expected)) {
      log.debugf(
          "transitionToPaused(%s, %s) is a no-op post hot/cold-split — terminal jobs cannot be paused",
          id, expected);
      return false;
    }
    try {
      // language=SQL Server
      String sql =
          """
          UPDATE scheduler_job_queue
          SET status = 'PAUSED', paused_from_status = ?, updated_at = SYSUTCDATETIME()
          WHERE job_id = ? AND status = ?
          """;
      int updated =
          ctx.em()
              .createNativeQuery(sql)
              .setParameter(1, expected.name())
              .setParameter(2, UuidByteArrayConverter.toBytes(id))
              .setParameter(3, expected.name())
              .executeUpdate();
      return updated > 0;
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("transition job to paused", e);
    }
  }

  boolean transitionFromPaused(UUID id, JobStatus target) {
    if (!SqlserverJobRowMapper.isLiveStatus(target)
        || target == JobStatus.PAUSED
        || target == JobStatus.WAITING) {
      throw new IllegalArgumentException(
          "transitionFromPaused expects a non-PAUSED live status; got " + target);
    }
    try {
      // language=SQL Server
      String sql =
          """
          UPDATE scheduler_job_queue
          SET status = ?, paused_from_status = NULL, updated_at = SYSUTCDATETIME()
          WHERE job_id = ? AND status = 'PAUSED'
          """;
      int updated =
          ctx.em()
              .createNativeQuery(sql)
              .setParameter(1, target.name())
              .setParameter(2, UuidByteArrayConverter.toBytes(id))
              .executeUpdate();
      return updated > 0;
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("transition job from paused", e);
    }
  }

  @SuppressWarnings("unchecked")
  JobStatus transitionFromPausedAtomic(UUID id) {
    try {
      // language=SQL Server
      String selectSql =
          """
          SELECT paused_from_status FROM scheduler_job_queue WITH (UPDLOCK, ROWLOCK)
          WHERE job_id = ? AND status = 'PAUSED'
          """;
      List<?> results =
          ctx.em()
              .createNativeQuery(selectSql)
              .setParameter(1, UuidByteArrayConverter.toBytes(id))
              .getResultList();
      if (results.isEmpty()) {
        return null;
      }
      String pausedFrom = (String) results.get(0);
      JobStatus target = pausedFrom != null ? JobStatus.valueOf(pausedFrom) : JobStatus.PENDING;
      // language=SQL Server
      String updateSql =
          """
          UPDATE scheduler_job_queue
          SET status = ?, paused_from_status = NULL, updated_at = SYSUTCDATETIME()
          WHERE job_id = ? AND status = 'PAUSED'
          """;
      int updated =
          ctx.em()
              .createNativeQuery(updateSql)
              .setParameter(1, target.name())
              .setParameter(2, UuidByteArrayConverter.toBytes(id))
              .executeUpdate();
      return updated > 0 ? target : null;
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("transition job from paused atomically", e);
    }
  }
}
