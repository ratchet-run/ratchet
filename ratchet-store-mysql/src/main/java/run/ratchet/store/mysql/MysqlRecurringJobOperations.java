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
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.NodeTagFilter;
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.store.converter.JobPayloadConverter;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.store.mysql.converter.UuidByteArrayConverter;
import run.ratchet.store.spi.RecurringJobDefinition;
import run.ratchet.store.spi.RecurringJobStore;
import run.ratchet.store.spi.RecurringJobStore.ArchiveReason;
import run.ratchet.store.util.JobClaimSqlSupport;

/**
 * MySQL implementation of {@link RecurringJobStore} against the dedicated {@code
 * scheduler_recurring_job} table (CP2 split).
 */
final class MysqlRecurringJobOperations implements RecurringJobStore {

  private static final int CANCEL_CHUNK = 500;
  private static final JobPayloadConverter PAYLOAD_CONVERTER = new JobPayloadConverter();

  // language=MySQL
  private static final String SELECT_COLUMNS =
      "id, priority, max_retries, backoff_policy, backoff_param_ms, timeout_sec, cron_expr,"
          + " zone_id, next_fire, is_paused, paused_at, payload, params, on_success_payload,"
          + " on_failure_payload, business_key, resource_name, created_at, caller_principal";

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
            + " cron_expr, zone_id, next_fire, is_paused, paused_at, payload, params,"
            + " on_success_payload, on_failure_payload, business_key, resource_name, created_at,"
            + " caller_principal)"
            + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CAST(? AS JSON), CAST(? AS JSON),"
            + " CAST(? AS JSON), CAST(? AS JSON), ?, ?, ?, ?)";
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
    q.setParameter(i++, PAYLOAD_CONVERTER.convertToDatabaseColumn(d.params()));
    q.setParameter(i++, PAYLOAD_CONVERTER.convertToDatabaseColumn(d.onSuccessPayload()));
    q.setParameter(i++, PAYLOAD_CONVERTER.convertToDatabaseColumn(d.onFailurePayload()));
    q.setParameter(i++, d.businessKey());
    q.setParameter(i++, d.resourceName());
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
            + " payload = CAST(? AS JSON), params = CAST(? AS JSON),"
            + " on_success_payload = CAST(? AS JSON), on_failure_payload = CAST(? AS JSON),"
            + " resource_name = ?"
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
    q.setParameter(i++, PAYLOAD_CONVERTER.convertToDatabaseColumn(d.params()));
    q.setParameter(i++, PAYLOAD_CONVERTER.convertToDatabaseColumn(d.onSuccessPayload()));
    q.setParameter(i++, PAYLOAD_CONVERTER.convertToDatabaseColumn(d.onFailurePayload()));
    q.setParameter(i++, d.resourceName());
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
    // 1. Insert archive rows from live rows.
    // language=MySQL
    String archiveSql =
        "INSERT INTO scheduler_recurring_job_archive ("
            + "id, cron_expr, zone_id, payload, params, on_success_payload, on_failure_payload,"
            + " business_key, created_at, caller_principal, archived_at, archive_reason)"
            + " SELECT id, cron_expr, zone_id, payload, params, on_success_payload,"
            + " on_failure_payload, business_key, created_at, caller_principal, NOW(3), ?"
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

    // 3. Delete live rows.
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
    UUID id = MysqlJobRowMapper.uuidOrNull(row[0]);
    int priority = ((Number) row[1]).intValue();
    int maxRetries = ((Number) row[2]).intValue();
    BackoffPolicy backoffPolicy = BackoffPolicy.valueOf((String) row[3]);
    int backoffParamMs = ((Number) row[4]).intValue();
    int timeoutSec = ((Number) row[5]).intValue();
    String cronExpr = (String) row[6];
    String zoneId = (String) row[7];
    Instant nextFire = MysqlJobRowMapper.toInstant(row[8]);
    boolean isPaused = toBoolean(row[9]);
    Instant pausedAt = MysqlJobRowMapper.toInstant(row[10]);
    JobPayload payload =
        PAYLOAD_CONVERTER.convertToEntityAttribute(MysqlJobRowMapper.stringOrNull(row[11]));
    JobPayload params =
        PAYLOAD_CONVERTER.convertToEntityAttribute(MysqlJobRowMapper.stringOrNull(row[12]));
    JobPayload onSuccess =
        PAYLOAD_CONVERTER.convertToEntityAttribute(MysqlJobRowMapper.stringOrNull(row[13]));
    JobPayload onFailure =
        PAYLOAD_CONVERTER.convertToEntityAttribute(MysqlJobRowMapper.stringOrNull(row[14]));
    String businessKey = (String) row[15];
    String resourceName = (String) row[16];
    Instant createdAt = MysqlJobRowMapper.toInstant(row[17]);
    String callerPrincipal = (String) row[18];

    return new RecurringJobDefinition(
        id,
        cronExpr,
        zoneId,
        nextFire,
        isPaused,
        pausedAt,
        priority,
        maxRetries,
        backoffPolicy,
        backoffParamMs,
        timeoutSec,
        payload,
        params,
        onSuccess,
        onFailure,
        businessKey,
        resourceName,
        createdAt,
        callerPrincipal);
  }

  private static boolean toBoolean(Object val) {
    if (val == null) {
      return false;
    }
    if (val instanceof Boolean b) {
      return b;
    }
    if (val instanceof Number n) {
      return n.intValue() != 0;
    }
    return Boolean.parseBoolean(val.toString());
  }
}
