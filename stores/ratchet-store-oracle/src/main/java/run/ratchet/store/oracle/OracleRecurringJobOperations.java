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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import run.ratchet.api.NodeTagFilter;
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.spi.ProtectedSurface;
import run.ratchet.store.oracle.converter.UuidRawConverter;
import run.ratchet.store.spi.RecurringJobDefinition;
import run.ratchet.store.spi.RecurringJobStore;
import run.ratchet.store.spi.RecurringJobStore.ArchiveReason;
import run.ratchet.store.util.JobClaimSqlSupport;
import run.ratchet.store.util.JobEncryption;
import run.ratchet.store.util.RecurringJobRows;
import run.ratchet.store.util.RowValues;

/**
 * Oracle implementation of {@link RecurringJobStore} against the dedicated {@code
 * scheduler_recurring_job} table.
 */
final class OracleRecurringJobOperations implements RecurringJobStore {

  private static final int CANCEL_CHUNK = 500;

  // language=Oracle
  private static final String SELECT_COLUMNS =
      "id, priority, max_retries, backoff_policy, backoff_param_ms, timeout_sec, cron_expr,"
          + " zone_id, next_fire, is_paused, paused_at, payload, on_success_payload,"
          + " on_failure_payload, business_key, resource_name, execution_target, created_at,"
          + " caller_principal, encrypted_payload";

  private final OracleStoreContext ctx;
  private final OracleBusinessKeyReservations reservations;

  OracleRecurringJobOperations(OracleStoreContext ctx, OracleBusinessKeyReservations reservations) {
    this.ctx = ctx;
    this.reservations = reservations;
  }

  @Override
  @SuppressWarnings({"unchecked", "SqlSourceToSinkFlow"})
  public List<RecurringJobDefinition> claimDueRecurring(
      int limit, String nodeId, NodeTagFilter tagFilter) {
    if (limit <= 0) {
      return List.of();
    }
    try {
      String tagSql = JobClaimSqlSupport.buildTagFilterSql(tagFilter, "r", "id");
      // Phase A: due masters ordered by effective priority, NO lock. Oracle rejects FETCH FIRST
      // combined with FOR UPDATE SKIP LOCKED (ORA-02014), so selection and locking are split.
      // language=Oracle
      String selectSql =
          ("SELECT "
                  + SELECT_COLUMNS
                  + " FROM scheduler_recurring_job r"
                  + " WHERE r.is_paused = FALSE"
                  + " AND r.next_fire <= CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS TIMESTAMP)"
                  + tagSql
                  + " ORDER BY r.priority DESC, r.next_fire ASC, r.id ASC"
                  + " FETCH FIRST ? ROWS ONLY")
              .replaceAll("\\s+", " ");
      Query q = ctx.em().createNativeQuery(selectSql);
      int p = 1;
      p = JobClaimSqlSupport.bindTagFilter(q, tagFilter, p);
      q.setParameter(p, limit);
      @SuppressWarnings("unchecked")
      List<Object[]> candidateRows = q.getResultList();
      if (candidateRows.isEmpty()) {
        return List.of();
      }

      // Phase B: lock the candidates still due, skipping any another node already holds. The
      // re-checked next_fire/is_paused predicate drops masters a racing node fired between phases,
      // and FOR UPDATE SKIP LOCKED is the only concurrency guard (advanceNextFire has no CAS).
      List<UUID> ids = new ArrayList<>(candidateRows.size());
      for (Object[] row : candidateRows) {
        ids.add(OracleJobRowMapper.uuidOrNull(row[0]));
      }
      String placeholders = String.join(",", Collections.nCopies(ids.size(), "?"));
      // language=Oracle
      String lockSql =
          "SELECT id FROM scheduler_recurring_job WHERE id IN ("
              + placeholders
              + ") AND is_paused = FALSE"
              + " AND next_fire <= CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS TIMESTAMP)"
              + " FOR UPDATE SKIP LOCKED";
      Query lockQuery = ctx.em().createNativeQuery(lockSql);
      int lp = 1;
      for (UUID id : ids) {
        lockQuery.setParameter(lp++, UuidRawConverter.toBytes(id));
      }
      @SuppressWarnings("unchecked")
      List<Object> lockedRows = lockQuery.getResultList();
      Set<UUID> locked = new HashSet<>(lockedRows.size());
      for (Object lockedRow : lockedRows) {
        locked.add(OracleJobRowMapper.uuidOrNull(lockedRow));
      }

      List<RecurringJobDefinition> defs = new ArrayList<>(candidateRows.size());
      for (Object[] row : candidateRows) {
        if (locked.contains(OracleJobRowMapper.uuidOrNull(row[0]))) {
          defs.add(hydrate(row));
        }
      }
      return defs;
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("claim recurring (new)", e);
    }
  }

  @Override
  public void advanceNextFire(UUID id, Instant nextFire) {
    // language=Oracle
    String sql = "UPDATE scheduler_recurring_job SET next_fire = ? WHERE id = ?";
    ctx.em()
        .createNativeQuery(sql)
        .setParameter(1, Timestamp.from(nextFire))
        .setParameter(2, UuidRawConverter.toBytes(id))
        .executeUpdate();
  }

  @Override
  @SuppressWarnings("unchecked")
  public Optional<Instant> findEarliestRecurringNextFire() {
    // language=Oracle
    String sql = "SELECT MIN(next_fire) FROM scheduler_recurring_job WHERE is_paused = FALSE";
    List<?> rows = ctx.em().createNativeQuery(sql).getResultList();
    if (rows.isEmpty() || rows.get(0) == null) {
      return Optional.empty();
    }
    Instant nf = RowValues.instantOrNull(rows.get(0));
    return Optional.ofNullable(nf);
  }

  @Override
  public boolean pauseRecurring(UUID id) {
    // language=Oracle
    String sql =
        "UPDATE scheduler_recurring_job SET is_paused = TRUE, paused_at = CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS TIMESTAMP)"
            + " WHERE id = ? AND is_paused = FALSE";
    return ctx.em()
            .createNativeQuery(sql)
            .setParameter(1, UuidRawConverter.toBytes(id))
            .executeUpdate()
        > 0;
  }

  @Override
  public boolean resumeRecurring(UUID id) {
    // language=Oracle
    String sql =
        "UPDATE scheduler_recurring_job SET is_paused = FALSE, paused_at = NULL"
            + " WHERE id = ? AND is_paused = TRUE";
    return ctx.em()
            .createNativeQuery(sql)
            .setParameter(1, UuidRawConverter.toBytes(id))
            .executeUpdate()
        > 0;
  }

  @Override
  public boolean cancelRecurringAndArchive(UUID id, ArchiveReason reason) {
    return archiveAndDelete(List.of(id), reason) > 0;
  }

  @Override
  public int cancelOrphanedRecurringAnnotationJobs(
      Set<String> knownBusinessKeys, Instant nodeStartTime) {
    // language=Oracle
    String baseSql =
        "SELECT id FROM scheduler_recurring_job"
            + " WHERE business_key IS NOT NULL"
            + " AND created_at < ?";
    String suffix =
        knownBusinessKeys.isEmpty()
            ? ""
            : " AND business_key NOT IN ("
                + String.join(",", Collections.nCopies(knownBusinessKeys.size(), "?"))
                + ")";
    Query q = ctx.em().createNativeQuery(baseSql + suffix);
    int p = 1;
    q.setParameter(p++, OracleTimestamps.microTimestamp(nodeStartTime));
    for (String key : knownBusinessKeys) {
      q.setParameter(p++, key);
    }
    @SuppressWarnings("unchecked")
    List<Object> raw = q.getResultList();
    List<UUID> ids = new ArrayList<>(raw.size());
    for (Object id : raw) {
      ids.add(OracleJobRowMapper.uuidOrNull(id));
    }
    return archiveAndDelete(ids, ArchiveReason.CANCELED);
  }

  @Override
  public int cancelRecurringJobsByTag(String tag) {
    // language=Oracle
    String sql =
        "SELECT r.id FROM scheduler_recurring_job r"
            + " JOIN scheduler_job_tag t ON t.job_id = r.id"
            + " WHERE t.tag = ?";
    @SuppressWarnings("unchecked")
    List<Object> raw = ctx.em().createNativeQuery(sql).setParameter(1, tag).getResultList();
    List<UUID> ids = new ArrayList<>(raw.size());
    for (Object id : raw) {
      ids.add(OracleJobRowMapper.uuidOrNull(id));
    }
    return archiveAndDelete(ids, ArchiveReason.CANCELED);
  }

  @Override
  public boolean cancelRecurringJobByBusinessKey(String businessKey) {
    return cancelRecurringJobsByBusinessKeys(Set.of(businessKey)) > 0;
  }

  @Override
  public int cancelRecurringJobsByBusinessKeys(Set<String> businessKeys) {
    if (businessKeys.isEmpty()) {
      return 0;
    }
    String placeholders = String.join(",", Collections.nCopies(businessKeys.size(), "?"));
    // language=Oracle
    String sql =
        "SELECT id FROM scheduler_recurring_job WHERE business_key IN (" + placeholders + ")";
    Query q = ctx.em().createNativeQuery(sql);
    int p = 1;
    for (String key : businessKeys) {
      q.setParameter(p++, key);
    }
    @SuppressWarnings("unchecked")
    List<Object> raw = q.getResultList();
    List<UUID> ids = new ArrayList<>(raw.size());
    for (Object id : raw) {
      ids.add(OracleJobRowMapper.uuidOrNull(id));
    }
    return archiveAndDelete(ids, ArchiveReason.CANCELED);
  }

  @Override
  public UUID createRecurring(RecurringJobDefinition d) {
    // language=Oracle
    String sql =
        "INSERT INTO scheduler_recurring_job ("
            + "id, priority, max_retries, backoff_policy, backoff_param_ms, timeout_sec,"
            + " cron_expr, zone_id, next_fire, is_paused, paused_at, payload,"
            + " on_success_payload, on_failure_payload, business_key, resource_name,"
            + " execution_target, created_at, caller_principal, encrypted_payload)"
            + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,"
            + " ?, ?, ?, ?, ?, ?, ?, ?)";
    Instant created = d.createdAt() != null ? d.createdAt() : Instant.now();
    boolean active = JobEncryption.activeFor(d.encryptedPayload());
    Query q = ctx.em().createNativeQuery(sql);
    int i = 1;
    q.setParameter(i++, UuidRawConverter.toBytes(d.id()));
    q.setParameter(i++, d.priority());
    q.setParameter(i++, d.maxRetries());
    q.setParameter(i++, d.backoffPolicy() != null ? d.backoffPolicy().name() : "NONE");
    q.setParameter(i++, d.backoffParamMs());
    q.setParameter(i++, d.timeoutSec());
    q.setParameter(i++, d.cronExpr());
    q.setParameter(i++, d.zoneId() != null ? d.zoneId() : "UTC");
    q.setParameter(i++, Timestamp.from(d.nextFire()));
    q.setParameter(i++, d.paused());
    q.setParameter(i++, d.pausedAt() != null ? Timestamp.from(d.pausedAt()) : null);
    q.setParameter(
        i++,
        RecurringJobRows.encryptPayloadColumn(
            d.payload(), active, ProtectedSurface.PAYLOAD_ARGS, d.id()));
    q.setParameter(
        i++,
        RecurringJobRows.encryptPayloadColumn(
            d.onSuccessPayload(), active, ProtectedSurface.ON_SUCCESS_PAYLOAD, d.id()));
    q.setParameter(
        i++,
        RecurringJobRows.encryptPayloadColumn(
            d.onFailurePayload(), active, ProtectedSurface.ON_FAILURE_PAYLOAD, d.id()));
    q.setParameter(i++, d.businessKey());
    q.setParameter(i++, d.resourceName());
    q.setParameter(i++, d.executionTarget());
    q.setParameter(i++, Timestamp.from(created));
    q.setParameter(i++, d.callerPrincipal());
    q.setParameter(i, active);
    try {
      q.executeUpdate();
      if (d.businessKey() != null) {
        reservations.insertReservation(
            d.businessKey(), d.id(), OracleBusinessKeyReservations.OWNER_TABLE_RECURRING);
      }
    } catch (RuntimeException e) {
      if (ctx.constraintDetector().isDuplicateBusinessKey(e)) {
        throw new RatchetTransientStoreException(
            "Active business key in use for recurring master " + d.id(), e);
      }
      throw e;
    }
    return d.id();
  }

  @Override
  public boolean updateRecurring(UUID id, RecurringJobDefinition d) {
    // language=Oracle
    String sql =
        "UPDATE scheduler_recurring_job SET"
            + " priority = ?, max_retries = ?, backoff_policy = ?, backoff_param_ms = ?,"
            + " timeout_sec = ?, cron_expr = ?, zone_id = ?, next_fire = ?,"
            + " payload = ?,"
            + " on_success_payload = ?, on_failure_payload = ?,"
            + " resource_name = ?, execution_target = ?, encrypted_payload = ?"
            + " WHERE id = ?";
    boolean active = JobEncryption.activeFor(d.encryptedPayload());
    Query q = ctx.em().createNativeQuery(sql);
    int i = 1;
    q.setParameter(i++, d.priority());
    q.setParameter(i++, d.maxRetries());
    q.setParameter(i++, d.backoffPolicy() != null ? d.backoffPolicy().name() : "NONE");
    q.setParameter(i++, d.backoffParamMs());
    q.setParameter(i++, d.timeoutSec());
    q.setParameter(i++, d.cronExpr());
    q.setParameter(i++, d.zoneId() != null ? d.zoneId() : "UTC");
    q.setParameter(i++, Timestamp.from(d.nextFire()));
    q.setParameter(
        i++,
        RecurringJobRows.encryptPayloadColumn(
            d.payload(), active, ProtectedSurface.PAYLOAD_ARGS, id));
    q.setParameter(
        i++,
        RecurringJobRows.encryptPayloadColumn(
            d.onSuccessPayload(), active, ProtectedSurface.ON_SUCCESS_PAYLOAD, id));
    q.setParameter(
        i++,
        RecurringJobRows.encryptPayloadColumn(
            d.onFailurePayload(), active, ProtectedSurface.ON_FAILURE_PAYLOAD, id));
    q.setParameter(i++, d.resourceName());
    q.setParameter(i++, d.executionTarget());
    q.setParameter(i++, active);
    q.setParameter(i, UuidRawConverter.toBytes(id));
    return q.executeUpdate() > 0;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Optional<RecurringJobDefinition> getRecurring(UUID id) {
    // language=Oracle
    String sql = "SELECT " + SELECT_COLUMNS + " FROM scheduler_recurring_job WHERE id = ?";
    List<Object[]> rows =
        ctx.em()
            .createNativeQuery(sql)
            .setParameter(1, UuidRawConverter.toBytes(id))
            .getResultList();
    if (rows.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(hydrate(rows.get(0)));
  }

  @Override
  @SuppressWarnings("unchecked")
  public Optional<RecurringJobDefinition> findRecurringByBusinessKey(String businessKey) {
    // language=Oracle
    String sql =
        "SELECT "
            + SELECT_COLUMNS
            + " FROM scheduler_recurring_job WHERE business_key = ? FETCH FIRST 1 ROWS ONLY";
    List<Object[]> rows =
        ctx.em().createNativeQuery(sql).setParameter(1, businessKey).getResultList();
    if (rows.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(hydrate(rows.get(0)));
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<RecurringJobDefinition> listAll() {
    // language=Oracle
    String sql = "SELECT " + SELECT_COLUMNS + " FROM scheduler_recurring_job";
    List<Object[]> rows = ctx.em().createNativeQuery(sql).getResultList();
    List<RecurringJobDefinition> defs = new ArrayList<>(rows.size());
    for (Object[] row : rows) {
      defs.add(hydrate(row));
    }
    return defs;
  }

  private int archiveAndDelete(List<UUID> ids, ArchiveReason reason) {
    if (ids.isEmpty()) {
      return 0;
    }
    int total = 0;
    for (int start = 0; start < ids.size(); start += CANCEL_CHUNK) {
      total +=
          archiveAndDeleteChunk(
              ids.subList(start, Math.min(start + CANCEL_CHUNK, ids.size())), reason);
    }
    return total;
  }

  private int archiveAndDeleteChunk(List<UUID> ids, ArchiveReason reason) {
    String placeholders = String.join(",", Collections.nCopies(ids.size(), "?"));
    // 1. Insert archive rows from live rows. IGNORE_ROW_ON_DUPKEY_INDEX gives INSERT IGNORE
    // semantics so concurrent cancels of the same id silently skip the duplicate archive row
    // (the loser's row violates the archive PK); the DELETE below is naturally idempotent. The
    // statement's row count is unused, so this hint's count behavior does not matter here.
    // language=Oracle
    String archiveSql =
        "INSERT /*+ IGNORE_ROW_ON_DUPKEY_INDEX(scheduler_recurring_job_archive(id)) */"
            + " INTO scheduler_recurring_job_archive ("
            + "id, cron_expr, zone_id, payload, on_success_payload, on_failure_payload,"
            + " business_key, execution_target, created_at, caller_principal, archived_at,"
            + " archive_reason)"
            + " SELECT id, cron_expr, zone_id, payload, on_success_payload,"
            + " on_failure_payload, business_key, execution_target, created_at,"
            + " caller_principal, CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS TIMESTAMP), ?"
            + " FROM scheduler_recurring_job WHERE id IN ("
            + placeholders
            + ")";
    Query archiveQ = ctx.em().createNativeQuery(archiveSql);
    int p = 1;
    archiveQ.setParameter(p++, reason.name());
    for (UUID id : ids) {
      archiveQ.setParameter(p++, UuidRawConverter.toBytes(id));
    }
    archiveQ.executeUpdate();

    // 2. Delete bkres rows owned by these recurring masters.
    reservations.deleteReservationsByOwners(ids);

    // 3. Delete recurring-master tag rows. scheduler_job_tag.job_id is polymorphic since
    // fk_job_tag_job was dropped; the SQL FK no longer cascades, so cancel must explicitly drop
    // the rows or they'd outlive the master row.
    // language=Oracle
    String tagDeleteSql = "DELETE FROM scheduler_job_tag WHERE job_id IN (" + placeholders + ")";
    Query tagDeleteQ = ctx.em().createNativeQuery(tagDeleteSql);
    int tp = 1;
    for (UUID id : ids) {
      tagDeleteQ.setParameter(tp++, UuidRawConverter.toBytes(id));
    }
    tagDeleteQ.executeUpdate();

    // 4. Delete live rows.
    // language=Oracle
    String deleteSql = "DELETE FROM scheduler_recurring_job WHERE id IN (" + placeholders + ")";
    Query deleteQ = ctx.em().createNativeQuery(deleteSql);
    p = 1;
    for (UUID id : ids) {
      deleteQ.setParameter(p++, UuidRawConverter.toBytes(id));
    }
    return deleteQ.executeUpdate();
  }

  private static RecurringJobDefinition hydrate(Object[] row) {
    return RecurringJobRows.hydrate(row);
  }
}
