package run.ratchet.store.postgresql;

import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobStatus;
import run.ratchet.store.spi.TagStore;
import jakarta.persistence.Query;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

final class PostgresqlTagOperations implements TagStore {

  private final PostgresqlStoreContext ctx;

  PostgresqlTagOperations(PostgresqlStoreContext ctx) {
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

  @Override
  public void insertTags(long jobId, List<String> tags) {
    if (tags == null || tags.isEmpty()) {
      return;
    }
    // language=PostgreSQL
    String sql =
        """
        INSERT INTO scheduler_job_tag (job_id, tag) VALUES (?, ?)
        ON CONFLICT (job_id, tag) DO NOTHING
        """;
    for (String tag : tags) {
      ctx.em().createNativeQuery(sql).setParameter(1, jobId).setParameter(2, tag).executeUpdate();
    }
  }

  @Override
  public int deleteTagsByJobId(long jobId) {
    // language=PostgreSQL
    String sql = "DELETE FROM scheduler_job_tag WHERE job_id = ?";
    return ctx.em().createNativeQuery(sql).setParameter(1, jobId).executeUpdate();
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<Long> findJobIdsByTag(String tag, int limit, int offset) {
    // language=PostgreSQL
    String sql =
        """
        SELECT job_id FROM scheduler_job_tag WHERE tag = ?
        ORDER BY job_id LIMIT ? OFFSET ?
        """;
    List<Number> results =
        ctx.em()
            .createNativeQuery(sql)
            .setParameter(1, tag)
            .setParameter(2, limit)
            .setParameter(3, offset)
            .getResultList();
    return results.stream().map(Number::longValue).toList();
  }

  @Override
  @SuppressWarnings("unchecked")
  public Map<JobStatus, Long> countJobsByStatusForTag(String tag) {
    // language=PostgreSQL
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
  }

  @Override
  @SuppressWarnings("unchecked")
  public Map<String, Long> countJobsByParamForTag(String tag, String paramKey) {
    // language=PostgreSQL
    String sql =
        """
        SELECT j.params ->> ?2 AS param_value, COUNT(*) FROM scheduler_job j
        JOIN scheduler_job_tag t ON j.job_id = t.job_id
        WHERE t.tag = ?1 AND j.params ->> ?2 IS NOT NULL
        GROUP BY param_value
        ORDER BY param_value
        """;
    List<Object[]> rows =
        ctx.em()
            .createNativeQuery(sql)
            .setParameter(1, tag)
            .setParameter(2, paramKey)
            .getResultList();
    return toStringCountMap(rows);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Map<String, Long> countJobsByExecutionNodeForTag(String tag) {
    // language=PostgreSQL
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
  }

  @SuppressWarnings("unchecked")
  void hydrateTagsSingle(JobEntity job) {
    if (job == null || job.getId() == null) return;
    // language=PostgreSQL
    String sql = "SELECT tag FROM scheduler_job_tag WHERE job_id = ?";
    List<String> tags =
        ctx.em().createNativeQuery(sql).setParameter(1, job.getId()).getResultList();
    if (!tags.isEmpty()) {
      job.setTags(tags);
    }
  }

  void hydrateTagsBatch(List<JobEntity> jobs) {
    if (jobs.isEmpty()) return;
    List<Long> ids = new ArrayList<>(jobs.size());
    Map<Long, JobEntity> byId = new HashMap<>();
    for (JobEntity job : jobs) {
      if (job.getId() != null) {
        ids.add(job.getId());
        byId.put(job.getId(), job);
      }
    }
    if (ids.isEmpty()) return;
    String placeholders = String.join(",", Collections.nCopies(ids.size(), "?"));
    // language=PostgreSQL
    String sql =
        "SELECT job_id, tag FROM scheduler_job_tag WHERE job_id IN ("
            + placeholders
            + ") ORDER BY job_id";
    Query tagQuery = ctx.em().createNativeQuery(sql);
    int parameter = 1;
    for (Long id : ids) {
      tagQuery.setParameter(parameter++, id);
    }
    @SuppressWarnings("unchecked")
    List<Object[]> rows = tagQuery.getResultList();
    for (Object[] row : rows) {
      long jobId = ((Number) row[0]).longValue();
      String tag = (String) row[1];
      JobEntity job = byId.get(jobId);
      if (job == null) continue;
      List<String> tags = job.getTags();
      if (tags == null) {
        tags = new ArrayList<>();
        job.setTags(tags);
      }
      tags.add(tag);
    }
  }
}
