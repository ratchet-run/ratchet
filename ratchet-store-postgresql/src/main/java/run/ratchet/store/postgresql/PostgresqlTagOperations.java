package run.ratchet.store.postgresql;

import run.ratchet.store.entity.JobStatus;
import run.ratchet.store.spi.TagStore;
import java.util.EnumMap;
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
    for (String tag : tags) {
      ctx.em()
          .createNativeQuery(
              "INSERT INTO scheduler_job_tag (job_id, tag) VALUES (?, ?) "
                  + "ON CONFLICT (job_id, tag) DO NOTHING")
          .setParameter(1, jobId)
          .setParameter(2, tag)
          .executeUpdate();
    }
  }

  @Override
  public int deleteTagsByJobId(long jobId) {
    return ctx.em()
        .createNativeQuery("DELETE FROM scheduler_job_tag WHERE job_id = ?")
        .setParameter(1, jobId)
        .executeUpdate();
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<Long> findJobIdsByTag(String tag, int limit, int offset) {
    List<Number> results =
        ctx.em()
            .createNativeQuery(
                "SELECT job_id FROM scheduler_job_tag WHERE tag = ? "
                    + "ORDER BY job_id LIMIT ? OFFSET ?")
            .setParameter(1, tag)
            .setParameter(2, limit)
            .setParameter(3, offset)
            .getResultList();
    return results.stream().map(Number::longValue).toList();
  }

  @Override
  @SuppressWarnings("unchecked")
  public Map<JobStatus, Long> countJobsByStatusForTag(String tag) {
    List<Object[]> rows =
        ctx.em()
            .createNativeQuery(
                "SELECT j.status, COUNT(*) FROM scheduler_job j "
                    + "JOIN scheduler_job_tag t ON j.job_id = t.job_id "
                    + "WHERE t.tag = ? GROUP BY j.status")
            .setParameter(1, tag)
            .getResultList();
    Map<JobStatus, Long> counts = new EnumMap<>(JobStatus.class);
    for (Object[] row : rows) {
      counts.put(JobStatus.valueOf((String) row[0]), ((Number) row[1]).longValue());
    }
    return counts;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Map<String, Long> countJobsByParamForTag(String tag, String paramKey) {
    List<Object[]> rows =
        ctx.em()
            .createNativeQuery(
                "SELECT j.params ->> ?2 AS param_value, COUNT(*) FROM scheduler_job j "
                    + "JOIN scheduler_job_tag t ON j.job_id = t.job_id "
                    + "WHERE t.tag = ?1 AND j.params ->> ?2 IS NOT NULL "
                    + "GROUP BY param_value ORDER BY param_value")
            .setParameter(1, tag)
            .setParameter(2, paramKey)
            .getResultList();
    return toStringCountMap(rows);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Map<String, Long> countJobsByExecutionNodeForTag(String tag) {
    List<Object[]> rows =
        ctx.em()
            .createNativeQuery(
                "SELECT j.picked_by, COUNT(*) FROM scheduler_job j "
                    + "JOIN scheduler_job_tag t ON j.job_id = t.job_id "
                    + "WHERE t.tag = ? AND j.picked_by IS NOT NULL AND j.picked_by <> '' "
                    + "GROUP BY j.picked_by ORDER BY j.picked_by")
            .setParameter(1, tag)
            .getResultList();
    return toStringCountMap(rows);
  }
}
