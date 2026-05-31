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
package run.ratchet.store.mysql;

import jakarta.persistence.Query;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import run.ratchet.api.NodeTagFilter;
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.store.converter.JobPayloadConverter;
import run.ratchet.store.mysql.converter.UuidByteArrayConverter;
import run.ratchet.store.spi.RecurringJobDefinition;
import run.ratchet.store.spi.RecurringJobStore;
import run.ratchet.store.spi.RecurringJobStore.ArchiveReason;
import run.ratchet.store.util.JobClaimSqlSupport;
import run.ratchet.store.util.RecurringJobRows;

/**
 * MySQL implementation of {@link RecurringJobStore} against the dedicated {@code
 * scheduler_recurring_job} table.
 */
final class MysqlRecurringJobOperations implements RecurringJobStore {

  private static final int CANCEL_CHUNK = 500;
  private static final JobPayloadConverter PAYLOAD_CONVERTER = new JobPayloadConverter();

  // language=MySQL
  private static final String SELECT_COLUMNS =
      "id, priority, max_retries, backoff_policy, backoff_param_ms, timeout_sec, cron_expr,"
          + " zone_id, next_fire, is_paused, paused_at, payload, on_success_payload,"
          + " on_failure_payload, business_key, resource_name, execution_target, created_at,"
          + " caller_principal";

  private final MysqlStoreContext ctx;
  private final MysqlBusinessKeyReservations reservations;

  MysqlRecurringJobOperations(MysqlStoreContext ctx, MysqlBusinessKeyReservations reservations) {
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
      // language=MySQL
      String sql =
          ("SELECT "
                  + SELECT_COLUMNS
                  + " FROM scheduler_recurring_job r"
                  + " WHERE r.is_paused = FALSE"
                  + " AND r.next_fire <= NOW(3)"
                  + tagSql
                  + " ORDER BY r.priority DESC, r.next_fire ASC, r.id ASC"
                  + " LIMIT ? FOR UPDATE SKIP LOCKED")
              .replaceAll("\\s+", " ");
      Query q = ctx.em().createNativeQuery(sql);
      int p = 1;
      p = JobClaimSqlSupport.bindTagFilter(q, tagFilter, p);
      q.setParameter(p, limit);
      List<Object[]> rows = q.getResultList();
      List<RecurringJobDefinition> defs = new ArrayList<>(rows.size());
      for (Object[] row : rows) {
        defs.add(hydrate(row));
      }
      return defs;
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("claim recurring (new)", e);
    }
  }

  @Override
  public void advanceNextFire(UUID id, Instant nextFire) {
    // language=MySQL
    String sql = "UPDATE scheduler_recurring_job SET next_fire = ? WHERE id = ?";
    ctx.em()
        .createNativeQuery(sql)
        .setParameter(1, Timestamp.from(nextFire))
        .setParameter(2, UuidByteArrayConverter.toBytes(id))
        .executeUpdate();
  }

  @Override
  @SuppressWarnings("unchecked")
  public Optional<Instant> findEarliestRecurringNextFire() {
    // language=MySQL
    String sql = "SELECT MIN(next_fire) FROM scheduler_recurring_job WHERE is_paused = FALSE";
    List<?> rows = ctx.em().createNativeQuery(sql).getResultList();
    if (rows.isEmpty() || rows.get(0) == null) {
      return Optional.empty();
    }
    Instant nf = MysqlJobRowMapper.toInstant(rows.get(0));
    return Optional.ofNullable(nf);
  }

  @Override
  public boolean pauseRecurring(UUID id) {
    // language=MySQL
    String sql =
        "UPDATE scheduler_recurring_job SET is_paused = TRUE, paused_at = NOW(3)"
            + " WHERE id = ? AND is_paused = FALSE";
    return ctx.em()
            .createNativeQuery(sql)
            .setParameter(1, UuidByteArrayConverter.toBytes(id))
            .executeUpdate()
        > 0;
  }

  @Override
  public boolean resumeRecurring(UUID id) {
    // language=MySQL
    String sql =
        "UPDATE scheduler_recurring_job SET is_paused = FALSE, paused_at = NULL"
            + " WHERE id = ? AND is_paused = TRUE";
    return ctx.em()
            .createNativeQuery(sql)
            .setParameter(1, UuidByteArrayConverter.toBytes(id))
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
    // language=MySQL
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
    q.setParameter(p++, Timestamp.from(nodeStartTime));
    for (String key : knownBusinessKeys) {
      q.setParameter(p++, key);
    }
    @SuppressWarnings("unchecked")
    List<Object> raw = q.getResultList();
    List<UUID> ids = new ArrayList<>(raw.size());
    for (Object id : raw) {
      ids.add(MysqlJobRowMapper.uuidOrNull(id));
    }
    return archiveAndDelete(ids, ArchiveReason.CANCELED);
  }

  @Override
  public int cancelRecurringJobsByTag(String tag) {
    // language=MySQL
    String sql =
        "SELECT r.id FROM scheduler_recurring_job r"
            + " JOIN scheduler_job_tag t ON t.job_id = r.id"
            + " WHERE t.tag = ?";
    @SuppressWarnings("unchecked")
    List<Object> raw = ctx.em().createNativeQuery(sql).setParameter(1, tag).getResultList();
    List<UUID> ids = new ArrayList<>(raw.size());
    for (Object id : raw) {
      ids.add(MysqlJobRowMapper.uuidOrNull(id));
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
    // language=MySQL
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
      ids.add(MysqlJobRowMapper.uuidOrNull(id));
    }
    return archiveAndDelete(ids, ArchiveReason.CANCELED);
  }

  @Override
  public UUID createRecurring(RecurringJobDefinition d) {
    // language=MySQL
    String sql =
        "INSERT INTO scheduler_recurring_job ("
            + "id, priority, max_retries, backoff_policy, backoff_param_ms, timeout_sec,"
            + " cron_expr, zone_id, next_fire, is_paused, paused_at, payload,"
            + " on_success_payload, on_failure_payload, business_key, resource_name,"
            + " execution_target, created_at, caller_principal)"
            + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CAST(? AS JSON),"
            + " CAST(? AS JSON), CAST(? AS JSON), ?, ?, ?, ?, ?)";
    Instant created = d.createdAt() != null ? d.createdAt() : Instant.now();
    Query q = ctx.em().createNativeQuery(sql);
    int i = 1;
    q.setParameter(i++, UuidByteArrayConverter.toBytes(d.id()));
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
    q.setParameter(i++, PAYLOAD_CONVERTER.convertToDatabaseColumn(d.payload()));
    q.setParameter(i++, PAYLOAD_CONVERTER.convertToDatabaseColumn(d.onSuccessPayload()));
    q.setParameter(i++, PAYLOAD_CONVERTER.convertToDatabaseColumn(d.onFailurePayload()));
    q.setParameter(i++, d.businessKey());
    q.setParameter(i++, d.resourceName());
    q.setParameter(i++, d.executionTarget());
    q.setParameter(i++, Timestamp.from(created));
    q.setParameter(i, d.callerPrincipal());
    try {
      q.executeUpdate();
      if (d.businessKey() != null) {
        reservations.insertReservation(
            d.businessKey(), d.id(), MysqlBusinessKeyReservations.OWNER_TABLE_RECURRING);
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
    // language=MySQL
    String sql =
        "UPDATE scheduler_recurring_job SET"
            + " priority = ?, max_retries = ?, backoff_policy = ?, backoff_param_ms = ?,"
            + " timeout_sec = ?, cron_expr = ?, zone_id = ?, next_fire = ?,"
            + " payload = CAST(? AS JSON),"
            + " on_success_payload = CAST(? AS JSON), on_failure_payload = CAST(? AS JSON),"
            + " resource_name = ?, execution_target = ?"
            + " WHERE id = ?";
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
    q.setParameter(i++, PAYLOAD_CONVERTER.convertToDatabaseColumn(d.payload()));
    q.setParameter(i++, PAYLOAD_CONVERTER.convertToDatabaseColumn(d.onSuccessPayload()));
    q.setParameter(i++, PAYLOAD_CONVERTER.convertToDatabaseColumn(d.onFailurePayload()));
    q.setParameter(i++, d.resourceName());
    q.setParameter(i++, d.executionTarget());
    q.setParameter(i, UuidByteArrayConverter.toBytes(id));
    return q.executeUpdate() > 0;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Optional<RecurringJobDefinition> getRecurring(UUID id) {
    // language=MySQL
    String sql = "SELECT " + SELECT_COLUMNS + " FROM scheduler_recurring_job WHERE id = ?";
    List<Object[]> rows =
        ctx.em()
            .createNativeQuery(sql)
            .setParameter(1, UuidByteArrayConverter.toBytes(id))
            .getResultList();
    if (rows.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(hydrate(rows.get(0)));
  }

  @Override
  @SuppressWarnings("unchecked")
  public Optional<RecurringJobDefinition> findRecurringByBusinessKey(String businessKey) {
    // language=MySQL
    String sql =
        "SELECT " + SELECT_COLUMNS + " FROM scheduler_recurring_job WHERE business_key = ? LIMIT 1";
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
    // language=MySQL
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
    // 1. Insert archive rows from live rows. INSERT IGNORE keeps cancel idempotent under
    // concurrent cancels for the same id: the loser of the race silently skips the duplicate
    // archive row, and the DELETE below is naturally idempotent (0 rows affected on a row
    // someone else already removed).
    // language=MySQL
    String archiveSql =
        "INSERT IGNORE INTO scheduler_recurring_job_archive ("
            + "id, cron_expr, zone_id, payload, on_success_payload, on_failure_payload,"
            + " business_key, execution_target, created_at, caller_principal, archived_at,"
            + " archive_reason)"
            + " SELECT id, cron_expr, zone_id, payload, on_success_payload,"
            + " on_failure_payload, business_key, execution_target, created_at,"
            + " caller_principal, NOW(3), ?"
            + " FROM scheduler_recurring_job WHERE id IN ("
            + placeholders
            + ")";
    Query archiveQ = ctx.em().createNativeQuery(archiveSql);
    int p = 1;
    archiveQ.setParameter(p++, reason.name());
    for (UUID id : ids) {
      archiveQ.setParameter(p++, UuidByteArrayConverter.toBytes(id));
    }
    archiveQ.executeUpdate();

    // 2. Delete bkres rows owned by these recurring masters.
    reservations.deleteReservationsByOwners(ids);

    // 3. Delete recurring-master tag rows. scheduler_job_tag.job_id is polymorphic since
    // fk_job_tag_job was dropped; the SQL FK no longer cascades, so cancel must explicitly drop
    // the rows or they'd outlive the master row.
    // language=MySQL
    String tagDeleteSql = "DELETE FROM scheduler_job_tag WHERE job_id IN (" + placeholders + ")";
    Query tagDeleteQ = ctx.em().createNativeQuery(tagDeleteSql);
    int tp = 1;
    for (UUID id : ids) {
      tagDeleteQ.setParameter(tp++, UuidByteArrayConverter.toBytes(id));
    }
    tagDeleteQ.executeUpdate();

    // 4. Delete live rows.
    // language=MySQL
    String deleteSql = "DELETE FROM scheduler_recurring_job WHERE id IN (" + placeholders + ")";
    Query deleteQ = ctx.em().createNativeQuery(deleteSql);
    p = 1;
    for (UUID id : ids) {
      deleteQ.setParameter(p++, UuidByteArrayConverter.toBytes(id));
    }
    return deleteQ.executeUpdate();
  }

  private static RecurringJobDefinition hydrate(Object[] row) {
    return RecurringJobRows.hydrate(row);
  }
}
