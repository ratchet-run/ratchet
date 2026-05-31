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
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import run.ratchet.api.JobStatus;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.TagStore;

final class PostgresqlTagOperations implements TagStore {

  private static final int MAX_TAG_HYDRATION_IDS = 250;
  private static final int INSERT_TAG_BATCH_SIZE = 250;

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
  public void insertTags(UUID jobId, List<String> tags) {
    if (tags == null || tags.isEmpty()) {
      return;
    }
    try {
      List<String> uniqueTags = new ArrayList<>(new LinkedHashSet<>(tags));
      for (int start = 0; start < uniqueTags.size(); start += INSERT_TAG_BATCH_SIZE) {
        int end = Math.min(start + INSERT_TAG_BATCH_SIZE, uniqueTags.size());
        insertTagChunk(jobId, uniqueTags.subList(start, end));
      }
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("insert job tags", e);
    }
  }

  private void insertTagChunk(UUID jobId, List<String> tags) {
    String placeholders = String.join(", ", Collections.nCopies(tags.size(), "(?, ?)"));
    // language=PostgreSQL
    String sql =
        """
        INSERT INTO scheduler_job_tag (job_id, tag) VALUES %s
        ON CONFLICT (job_id, tag) DO NOTHING
        """
            .formatted(placeholders);
    Query query = ctx.em().createNativeQuery(sql);
    int parameter = 1;
    for (String tag : tags) {
      query.setParameter(parameter++, jobId);
      query.setParameter(parameter++, tag);
    }
    query.executeUpdate();
  }

  @Override
  public int deleteTagsByJobId(UUID jobId) {
    try {
      // language=PostgreSQL
      String sql = "DELETE FROM scheduler_job_tag WHERE job_id = ?";
      return ctx.em().createNativeQuery(sql).setParameter(1, jobId).executeUpdate();
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("delete job tags", e);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<UUID> findJobIdsByTag(String tag, int limit, int offset) {
    try {
      // language=PostgreSQL
      String sql =
          """
          SELECT job_id FROM scheduler_job_tag WHERE tag = ?
          ORDER BY job_id LIMIT ? OFFSET ?
          """;
      List<?> results =
          ctx.em()
              .createNativeQuery(sql)
              .setParameter(1, tag)
              .setParameter(2, limit)
              .setParameter(3, offset)
              .getResultList();
      return results.stream().map(PostgresqlJobRowMapper::uuidOrNull).toList();
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("find job ids by tag", e);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public Map<JobStatus, Long> countJobsByStatusForTag(String tag) {
    try {
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
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("count jobs by status for tag", e);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public Map<String, Long> countJobsByParamForTag(String tag, String paramKey) {
    try {
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
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("count jobs by param for tag", e);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public Map<String, Long> countJobsByExecutionNodeForTag(String tag) {
    try {
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
                AND e.id = (SELECT e2.id FROM scheduler_job_execution e2
                            WHERE e2.job_id = c2.job_id
                            ORDER BY e2.attempt DESC, e2.started_at DESC, e2.id DESC
                            LIMIT 1)
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
      // language=PostgreSQL
      String sql = "SELECT tag FROM scheduler_job_tag WHERE job_id = ?";
      List<String> tags =
          ctx.em().createNativeQuery(sql).setParameter(1, job.getId()).getResultList();
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
      for (JobEntity job : jobs) {
        if (job.getId() != null) {
          ids.add(job.getId());
          byId.put(job.getId(), job);
        }
      }
      if (ids.isEmpty()) return;
      for (int start = 0; start < ids.size(); start += MAX_TAG_HYDRATION_IDS) {
        List<UUID> chunk = ids.subList(start, Math.min(start + MAX_TAG_HYDRATION_IDS, ids.size()));
        String placeholders = String.join(",", Collections.nCopies(chunk.size(), "?"));
        // language=PostgreSQL
        String sql =
            "SELECT job_id, tag FROM scheduler_job_tag WHERE job_id IN ("
                + placeholders
                + ") ORDER BY job_id";
        Query tagQuery = ctx.em().createNativeQuery(sql);
        int parameter = 1;
        for (UUID id : chunk) {
          tagQuery.setParameter(parameter++, id);
        }
        @SuppressWarnings("unchecked")
        List<Object[]> rows = tagQuery.getResultList();
        applyHydratedTagRows(rows, byId);
      }
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("hydrate job tags batch", e);
    }
  }

  private static void applyHydratedTagRows(List<Object[]> rows, Map<UUID, JobEntity> byId) {
    for (Object[] row : rows) {
      UUID jobId = PostgresqlJobRowMapper.uuidOrNull(row[0]);
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
