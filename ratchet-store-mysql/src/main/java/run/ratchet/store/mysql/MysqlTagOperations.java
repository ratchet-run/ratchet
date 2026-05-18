package run.ratchet.store.mysql;

import jakarta.persistence.Query;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;
import run.ratchet.api.JobStatus;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.mysql.converter.UuidByteArrayConverter;
import run.ratchet.store.spi.TagStore;

final class MysqlTagOperations implements TagStore {

  private final MysqlStoreContext ctx;

  MysqlTagOperations(MysqlStoreContext ctx) {
    this.ctx = ctx;
  }

  private static Map<String, Long> toStringCountMap(List<Object[]> rows) {
    Map<String, Long> counts = new TreeMap<>();
    for (Object[] row : rows) {
      String key = (String) row[0];
      if (key == null || key.isBlank()) {
        continue;
      }
      counts.put(key, ((Number) row[1]).longValue());
    }
    return counts;
  }

  private static String toJsonFieldPath(String fieldName) {
    String escapedFieldName = fieldName.replace("\\", "\\\\").replace("\"", "\\\"");
    return "$.\"" + escapedFieldName + "\"";
  }

  @Override
  public void insertTags(UUID jobId, List<String> tags) {
    if (tags == null || tags.isEmpty()) {
      return;
    }
    try {
      // Build a single INSERT IGNORE with one VALUES row per tag to avoid N round-trips.
      // language=MySQL
      String placeholders = String.join(",", Collections.nCopies(tags.size(), "(?,?)"));
      String sql = "INSERT IGNORE INTO scheduler_job_tag (job_id, tag) VALUES " + placeholders;
      byte[] jobIdBytes = UuidByteArrayConverter.toBytes(jobId);
      Query query = ctx.em().createNativeQuery(sql);
      int param = 1;
      for (String tag : tags) {
        query.setParameter(param++, jobIdBytes);
        query.setParameter(param++, tag);
      }
      query.executeUpdate();
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("insert job tags", e);
    }
  }

  @Override
  public int deleteTagsByJobId(UUID jobId) {
    try {
      // language=MySQL
      String sql = "DELETE FROM scheduler_job_tag WHERE job_id = ?";
      return ctx.em()
          .createNativeQuery(sql)
          .setParameter(1, UuidByteArrayConverter.toBytes(jobId))
          .executeUpdate();
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("delete job tags", e);
    }
  }

  @Override
  public List<UUID> findJobIdsByTag(String tag, int limit, int offset) {
    try {
      // language=MySQL
      String sql = "SELECT job_id FROM scheduler_job_tag WHERE tag = ? LIMIT ? OFFSET ?";
      List<?> rows =
          ctx.em()
              .createNativeQuery(sql)
              .setParameter(1, tag)
              .setParameter(2, limit)
              .setParameter(3, offset)
              .getResultList();
      return rows.stream().map(MysqlJobRowMapper::uuidOrNull).collect(Collectors.toList());
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("find job ids by tag", e);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public Map<JobStatus, Long> countJobsByStatusForTag(String tag) {
    try {
      // language=MySQL
      String sql =
          """
          SELECT s, SUM(c) FROM (
            SELECT q.status AS s, COUNT(*) AS c FROM scheduler_job_queue q
              JOIN scheduler_job_tag t ON t.job_id = q.job_id
              WHERE t.tag = ? GROUP BY q.status
            UNION ALL
            SELECT c.terminal_status AS s, COUNT(*) AS c FROM scheduler_job c
              JOIN scheduler_job_tag t ON t.job_id = c.job_id
              WHERE t.tag = ? AND c.terminal_status IS NOT NULL
              GROUP BY c.terminal_status
          ) u GROUP BY s
          """;
      List<Object[]> rows =
          ctx.em().createNativeQuery(sql).setParameter(1, tag).setParameter(2, tag).getResultList();
      Map<JobStatus, Long> counts = new EnumMap<>(JobStatus.class);
      for (Object[] row : rows) {
        counts.put(JobStatus.valueOf((String) row[0]), ((Number) row[1]).longValue());
      }
      return counts;
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("count jobs by status for tag", e);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public Map<String, Long> countJobsByParamForTag(String tag, String paramKey) {
    try {
      String jsonPath = toJsonFieldPath(paramKey);
      // language=MySQL
      String sql =
          """
          SELECT JSON_UNQUOTE(JSON_EXTRACT(j.params, ?)) AS param_value, COUNT(*)
          FROM scheduler_job j
          JOIN scheduler_job_tag t ON j.job_id = t.job_id
          WHERE t.tag = ?
            AND JSON_EXTRACT(j.params, ?) IS NOT NULL
          GROUP BY param_value
          ORDER BY param_value
          """;
      List<Object[]> rows =
          ctx.em()
              .createNativeQuery(sql)
              .setParameter(1, jsonPath)
              .setParameter(2, tag)
              .setParameter(3, jsonPath)
              .getResultList();
      return toStringCountMap(rows);
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("count jobs by param for tag", e);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public Map<String, Long> countJobsByExecutionNodeForTag(String tag) {
    try {
      // language=MySQL
      String sql =
          """
          SELECT node, SUM(c) FROM (
            SELECT q.picked_by AS node, COUNT(*) AS c
              FROM scheduler_job_queue q
              JOIN scheduler_job_tag t ON t.job_id = q.job_id
              WHERE t.tag = ? AND q.picked_by IS NOT NULL AND q.picked_by <> ''
              GROUP BY q.picked_by
            UNION ALL
            SELECT e.node_id AS node, COUNT(*) AS c
              FROM scheduler_job c2
              JOIN scheduler_job_tag t ON t.job_id = c2.job_id
              JOIN scheduler_job_execution e ON e.job_id = c2.job_id
              WHERE t.tag = ? AND c2.terminal_status IS NOT NULL
                AND e.id = (SELECT MAX(e2.id) FROM scheduler_job_execution e2
                            WHERE e2.job_id = c2.job_id)
                AND e.node_id IS NOT NULL AND e.node_id <> ''
              GROUP BY e.node_id
          ) u GROUP BY node ORDER BY node
          """;
      List<Object[]> rows =
          ctx.em().createNativeQuery(sql).setParameter(1, tag).setParameter(2, tag).getResultList();
      return toStringCountMap(rows);
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("count jobs by execution node for tag", e);
    }
  }

  @SuppressWarnings("unchecked")
  void hydrateTagsSingle(JobEntity job) {
    if (job == null || job.getId() == null) return;
    try {
      // language=MySQL
      String sql = "SELECT tag FROM scheduler_job_tag WHERE job_id = ?";
      List<String> tags =
          ctx.em()
              .createNativeQuery(sql)
              .setParameter(1, UuidByteArrayConverter.toBytes(job.getId()))
              .getResultList();
      if (!tags.isEmpty()) {
        job.setTags(tags);
      }
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("hydrate job tags", e);
    }
  }

  void hydrateTagsBatch(List<JobEntity> jobs) {
    if (jobs.isEmpty()) return;
    try {
      List<UUID> ids = new ArrayList<>(jobs.size());
      Map<UUID, JobEntity> byId = new HashMap<>();
      for (JobEntity j : jobs) {
        if (j.getId() != null) {
          ids.add(j.getId());
          byId.put(j.getId(), j);
        }
      }
      if (ids.isEmpty()) return;
      String placeholders = String.join(",", Collections.nCopies(ids.size(), "?"));
      // language=MySQL
      String sql =
          "SELECT job_id, tag FROM scheduler_job_tag WHERE job_id IN ("
              + placeholders
              + ") ORDER BY job_id";
      Query tagQuery = ctx.em().createNativeQuery(sql);
      int parameter = 1;
      for (UUID id : ids) {
        tagQuery.setParameter(parameter++, UuidByteArrayConverter.toBytes(id));
      }
      @SuppressWarnings("unchecked")
      List<Object[]> rows = tagQuery.getResultList();
      for (Object[] row : rows) {
        UUID jid = MysqlJobRowMapper.uuidOrNull(row[0]);
        String tag = (String) row[1];
        JobEntity j = byId.get(jid);
        if (j == null) continue;
        List<String> tags = j.getTags();
        if (tags == null) {
          tags = new ArrayList<>();
          j.setTags(tags);
        }
        tags.add(tag);
      }
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("hydrate job tags batch", e);
    }
  }
}
