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
}
