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
package run.ratchet.store.oracle;

import jakarta.persistence.Query;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.oracle.converter.UuidRawConverter;
import run.ratchet.store.util.BusinessKeyReservations;

final class OracleBusinessKeyReservations {

  private static final int DELETE_RESERVATIONS_CHUNK_SIZE = 500;

  static final String OWNER_TABLE_QUEUE = BusinessKeyReservations.OWNER_TABLE_QUEUE;
  static final String OWNER_TABLE_RECURRING = BusinessKeyReservations.OWNER_TABLE_RECURRING;

  // language=Oracle
  static final String BKRES_INSERT_SQL =
      """
      INSERT INTO scheduler_business_key_reservation
        (business_key, owner_job_id, owner_table, reserved_at)
      VALUES (?, ?, ?, ?)
      """;

  private final OracleStoreContext ctx;

  OracleBusinessKeyReservations(OracleStoreContext ctx) {
    this.ctx = ctx;
  }

  boolean tryReserve(String businessKey, UUID ownerJobId, String ownerTable) {
    // This hint applies only to single-table INSERT, names the unique key, and disables
    // APPEND and parallel DML.
    String sql =
        """
        INSERT /*+ IGNORE_ROW_ON_DUPKEY_INDEX(scheduler_business_key_reservation
          (business_key)) */ INTO scheduler_business_key_reservation
          (business_key, owner_job_id, owner_table, reserved_at)
        VALUES (?, ?, ?, CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS TIMESTAMP))
        """;
    return ctx.em()
            .createNativeQuery(sql)
            .setParameter(1, businessKey)
            .setParameter(2, UuidRawConverter.toBytes(ownerJobId))
            .setParameter(3, ownerTable)
            .executeUpdate()
        == 1;
  }

  void insertReservation(String businessKey, UUID ownerJobId, String ownerTable) {
    Objects.requireNonNull(businessKey, "businessKey");
    // Keep DML local: Oracle stores UUIDs as binary values and uses dialect-specific timestamps.
    // language=Oracle
    String sql =
        """
        INSERT INTO scheduler_business_key_reservation
          (business_key, owner_job_id, owner_table, reserved_at)
        VALUES (?, ?, ?, CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS TIMESTAMP))
        """;
    ctx.em()
        .createNativeQuery(sql)
        .setParameter(1, businessKey)
        .setParameter(2, UuidRawConverter.toBytes(ownerJobId))
        .setParameter(3, ownerTable)
        .executeUpdate();
  }

  @SuppressWarnings("UnusedReturnValue")
  int deleteReservationByOwner(UUID ownerJobId) {
    // language=Oracle
    String sql = "DELETE FROM scheduler_business_key_reservation WHERE owner_job_id = ?";
    return ctx.em()
        .createNativeQuery(sql)
        .setParameter(1, UuidRawConverter.toBytes(ownerJobId))
        .executeUpdate();
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
    String placeholders = String.join(",", Collections.nCopies(ownerJobIds.size(), "?"));
    // language=Oracle
    String sql =
        "DELETE FROM scheduler_business_key_reservation WHERE owner_job_id IN ("
            + placeholders
            + ")";
    Query query = ctx.em().createNativeQuery(sql);
    int parameter = 1;
    for (UUID ownerJobId : ownerJobIds) {
      query.setParameter(parameter++, UuidRawConverter.toBytes(ownerJobId));
    }
    query.executeUpdate();
  }

  void bindInsert(Query q, JobEntity job, Timestamp nowTs) {
    bindInsert(q, job, nowTs, 1);
  }

  int bindInsert(Query q, JobEntity job, Timestamp nowTs, int i) {
    q.setParameter(i++, Objects.requireNonNull(job.getBusinessKey(), "businessKey"));
    q.setParameter(i++, UuidRawConverter.toBytes(job.getId()));
    q.setParameter(i++, BusinessKeyReservations.ownerTableFor(job.getJobType()));
    q.setParameter(i, nowTs);
    return i + 1;
  }

  void holdReservation(UUID id, Instant until, String nodeId, long claimSeq) {
    // language=Oracle
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
        .setParameter(4, UuidRawConverter.toBytes(id))
        .executeUpdate();
  }

  boolean releaseBusinessKeyHold(UUID jobId, long claimSeq) {
    return ctx.timedStoreOperation(
        "release_business_key_hold",
        () -> {
          try {
            // Read without locking the owner index; delete through the primary key.
            // language=Oracle
            String selectSql =
                """
                SELECT business_key FROM scheduler_business_key_reservation
                WHERE owner_job_id = ? AND held_claim_seq = ? AND held_until IS NOT NULL
                """;
            List<?> keys =
                ctx.em()
                    .createNativeQuery(selectSql)
                    .setParameter(1, UuidRawConverter.toBytes(jobId))
                    .setParameter(2, claimSeq)
                    .getResultList();
            if (keys.isEmpty()) {
              return false;
            }
            // language=Oracle
            String deleteSql =
                """
                DELETE FROM scheduler_business_key_reservation
                WHERE business_key = ? AND owner_job_id = ? AND held_claim_seq = ?
                  AND held_until IS NOT NULL
                """;
            return ctx.em()
                    .createNativeQuery(deleteSql)
                    .setParameter(1, keys.get(0))
                    .setParameter(2, UuidRawConverter.toBytes(jobId))
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
              for (Object key : keys) {
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
            // language=Oracle
            String selectSql =
                """
                SELECT business_key FROM scheduler_business_key_reservation
                WHERE held_until IS NOT NULL AND held_until < ?
                ORDER BY held_until FETCH FIRST ? ROWS ONLY
                """;
            List<?> keys =
                ctx.em()
                    .createNativeQuery(selectSql)
                    .setParameter(1, Timestamp.from(now))
                    .setParameter(2, limit)
                    .getResultList();
            // language=Oracle
            String deleteSql =
                """
                DELETE FROM scheduler_business_key_reservation
                WHERE business_key = ? AND held_until IS NOT NULL AND held_until < ?
                """;
            int deleted = 0;
            for (Object key : keys) {
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
