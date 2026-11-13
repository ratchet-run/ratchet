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
package run.ratchet.store.postgresql;

import jakarta.persistence.Query;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import run.ratchet.api.JobStatus;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.util.BusinessKeyReservations;
import run.ratchet.store.util.StatusClassifier;

final class PostgresqlBusinessKeyReservations {

  static final String OWNER_TABLE_QUEUE = BusinessKeyReservations.OWNER_TABLE_QUEUE;
  static final String OWNER_TABLE_RECURRING = BusinessKeyReservations.OWNER_TABLE_RECURRING;

  private static final int DELETE_RESERVATIONS_CHUNK_SIZE = 500;

  private final PostgresqlStoreContext ctx;

  PostgresqlBusinessKeyReservations(PostgresqlStoreContext ctx) {
    this.ctx = ctx;
  }

  static String ownerTableFor(JobExecutionType jobType) {
    return BusinessKeyReservations.ownerTableFor(jobType);
  }

  static String ownerTableFor(String jobType) {
    return BusinessKeyReservations.ownerTableFor(jobType);
  }

  boolean tryReserve(String businessKey, UUID ownerJobId, String ownerTable) {
    try {
      String sql =
          """
          INSERT INTO scheduler_business_key_reservation
            (business_key, owner_job_id, owner_table, reserved_at)
          VALUES (?, ?, ?, statement_timestamp())
          ON CONFLICT (business_key) DO NOTHING
          """;
      return ctx.em()
              .createNativeQuery(sql)
              .setParameter(1, businessKey)
              .setParameter(2, ownerJobId)
              .setParameter(3, ownerTable)
              .executeUpdate()
          == 1;
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("try reserve business key", e);
    }
  }

  void insertReservation(String businessKey, UUID ownerJobId, String ownerTable) {
    try {
      // Keep DML local: PostgreSQL binds UUID values directly and owns its timestamp expression.
      // language=PostgreSQL
      String sql =
          """
          INSERT INTO scheduler_business_key_reservation
            (business_key, owner_job_id, owner_table, reserved_at)
          VALUES (?, ?, ?, statement_timestamp())
          """;
      ctx.em()
          .createNativeQuery(sql)
          .setParameter(1, businessKey)
          .setParameter(2, ownerJobId)
          .setParameter(3, ownerTable)
          .executeUpdate();
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("insert business key reservation", e);
    }
  }

  int bindInsert(Query q, JobEntity job, int i) {
    q.setParameter(i++, job.getBusinessKey());
    q.setParameter(i++, job.getId());
    q.setParameter(i++, ownerTableFor(job.getJobType()));
    return i;
  }

  @SuppressWarnings("UnusedReturnValue")
  int deleteReservationByOwner(UUID ownerJobId) {
    try {
      // language=PostgreSQL
      String sql = "DELETE FROM scheduler_business_key_reservation WHERE owner_job_id = ?";
      return ctx.em().createNativeQuery(sql).setParameter(1, ownerJobId).executeUpdate();
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("delete business key reservation", e);
    }
  }

  void deleteReservationsByOwners(List<UUID> ownerJobIds) {
    if (ownerJobIds.isEmpty()) {
      return;
    }
    for (int start = 0; start < ownerJobIds.size(); start += DELETE_RESERVATIONS_CHUNK_SIZE) {
      deleteReservationsByOwnersChunk(
          ownerJobIds.subList(
              start, Math.min(start + DELETE_RESERVATIONS_CHUNK_SIZE, ownerJobIds.size())));
    }
  }

  private void deleteReservationsByOwnersChunk(List<UUID> ownerJobIds) {
    try {
      String placeholders = String.join(",", Collections.nCopies(ownerJobIds.size(), "?"));
      // language=PostgreSQL
      String sql =
          "DELETE FROM scheduler_business_key_reservation WHERE owner_job_id IN ("
              + placeholders
              + ")";
      Query query = ctx.em().createNativeQuery(sql);
      int parameter = 1;
      for (UUID ownerJobId : ownerJobIds) {
        query.setParameter(parameter++, ownerJobId);
      }
      query.executeUpdate();
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("delete business key reservations", e);
    }
  }

  void syncForJob(JobEntity job) {
    try {
      deleteReservationByOwner(job.getId());
      JobStatus status = StatusClassifier.effectiveStatus(job.getStatus());
      if (StatusClassifier.isLiveStatus(status) && job.getBusinessKey() != null) {
        insertReservation(job.getBusinessKey(), job.getId(), ownerTableFor(job.getJobType()));
      }
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("sync business key reservation", e);
    }
  }

  /**
   * Rebuilds the active reservation for one job.
   *
   * @implNote TX: REQUIRED - callers must invoke this inside the same store transaction that
   *     changed the job state. The delete/read/insert sequence must commit or roll back as one
   *     unit.
   */
  void syncForJob(UUID ownerJobId, JobStatus status) {
    try {
      deleteReservationByOwner(ownerJobId);
      if (!StatusClassifier.isLiveStatus(status)) {
        return;
      }
      // language=PostgreSQL
      String selectSql = "SELECT business_key, job_type FROM scheduler_job WHERE job_id = ?";
      @SuppressWarnings("unchecked")
      List<Object[]> rows =
          ctx.em().createNativeQuery(selectSql).setParameter(1, ownerJobId).getResultList();
      if (rows.isEmpty()) {
        return;
      }
      Object[] row = rows.get(0);
      String businessKey = (String) row[0];
      if (businessKey != null) {
        insertReservation(businessKey, ownerJobId, ownerTableFor((String) row[1]));
      }
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("sync business key reservation", e);
    }
  }

  void holdReservation(UUID id, Instant until, String nodeId, long claimSeq) {
    // language=PostgreSQL
    String sql =
        """
        UPDATE scheduler_business_key_reservation
        SET held_until = ?, held_by_node = ?, held_claim_seq = ?
        WHERE owner_job_id = ?
        """;
    ctx.em()
        .createNativeQuery(sql)
        .setParameter(1, Timestamp.from(until))
        .setParameter(2, nodeId)
        .setParameter(3, claimSeq)
        .setParameter(4, id)
        .executeUpdate();
  }

  boolean releaseBusinessKeyHold(UUID jobId, long claimSeq) {
    return ctx.timedStoreOperation(
        "release_business_key_hold",
        () -> {
          try {
            // Read without locking the owner index; delete through the primary key.
            // language=PostgreSQL
            String selectSql =
                """
                SELECT business_key FROM scheduler_business_key_reservation
                WHERE owner_job_id = ? AND held_claim_seq = ? AND held_until IS NOT NULL
                """;
            List<?> keys =
                ctx.em()
                    .createNativeQuery(selectSql)
                    .setParameter(1, jobId)
                    .setParameter(2, claimSeq)
                    .getResultList();
            if (keys.isEmpty()) {
              return false;
            }
            // language=PostgreSQL
            String deleteSql =
                """
                DELETE FROM scheduler_business_key_reservation
                WHERE business_key = ? AND owner_job_id = ? AND held_claim_seq = ?
                  AND held_until IS NOT NULL
                """;
            return ctx.em()
                    .createNativeQuery(deleteSql)
                    .setParameter(1, keys.get(0))
                    .setParameter(2, jobId)
                    .setParameter(3, claimSeq)
                    .executeUpdate()
                > 0;
          } catch (RuntimeException e) {
            throw ctx.translateTransientStoreException("release business key hold", e);
          }
        },
        released -> released ? "deleted" : "miss");
  }

  int releaseBusinessKeyHolds(List<String> nodeIds) {
    if (nodeIds.isEmpty()) {
      return 0;
    }
    return ctx.timedStoreOperation(
        "release_business_key_holds",
        () -> {
          try {
            int deleted = 0;
            for (int start = 0; start < nodeIds.size(); start += 500) {
              List<String> chunk = nodeIds.subList(start, Math.min(start + 500, nodeIds.size()));
              String predicate =
                  "held_until IS NOT NULL AND held_by_node IN ("
                      + String.join(",", Collections.nCopies(chunk.size(), "?"))
                      + ")";
              Query select =
                  ctx.em()
                      .createNativeQuery(
                          "SELECT business_key FROM scheduler_business_key_reservation WHERE "
                              + predicate);
              for (int i = 0; i < chunk.size(); i++) {
                select.setParameter(i + 1, chunk.get(i));
              }
              List<?> keys = select.getResultList();
              for (String key : BusinessKeyReservations.holdDeleteOrder(keys)) {
                Query delete =
                    ctx.em()
                        .createNativeQuery(
                            "DELETE FROM scheduler_business_key_reservation WHERE business_key = ?"
                                + " AND "
                                + predicate)
                        .setParameter(1, key);
                for (int i = 0; i < chunk.size(); i++) {
                  delete.setParameter(i + 2, chunk.get(i));
                }
                deleted += delete.executeUpdate();
              }
            }
            return deleted;
          } catch (RuntimeException e) {
            throw ctx.translateTransientStoreException("release business key holds", e);
          }
        },
        count -> count > 0 ? "deleted" : "miss");
  }

  int deleteExpiredBusinessKeyHolds(Instant now, int limit) {
    if (limit <= 0) {
      return 0;
    }
    return ctx.timedStoreOperation(
        "delete_expired_business_key_holds",
        () -> {
          try {
            // language=PostgreSQL
            String selectSql =
                """
                SELECT business_key FROM scheduler_business_key_reservation
                WHERE held_until IS NOT NULL AND held_until < ?
                ORDER BY held_until LIMIT ?
                """;
            List<?> keys =
                ctx.em()
                    .createNativeQuery(selectSql)
                    .setParameter(1, Timestamp.from(now))
                    .setParameter(2, limit)
                    .getResultList();
            // language=PostgreSQL
            String deleteSql =
                """
                DELETE FROM scheduler_business_key_reservation
                WHERE business_key = ? AND held_until IS NOT NULL AND held_until < ?
                """;
            int deleted = 0;
            for (String key : BusinessKeyReservations.holdDeleteOrder(keys)) {
              deleted +=
                  ctx.em()
                      .createNativeQuery(deleteSql)
                      .setParameter(1, key)
                      .setParameter(2, Timestamp.from(now))
                      .executeUpdate();
            }
            return deleted;
          } catch (RuntimeException e) {
            throw ctx.translateTransientStoreException("delete expired business key holds", e);
          }
        },
        count -> count > 0 ? "deleted" : "miss");
  }
}
