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
package run.ratchet.store.sqlserver;

import jakarta.persistence.Query;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import run.ratchet.api.NodeTagFilter;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.ExecutionTargetFilter;
import run.ratchet.store.sqlserver.converter.UuidByteArrayConverter;
import run.ratchet.store.util.JobClaimSqlSupport;
import run.ratchet.store.util.RowValues;

/** Reads ordered candidates without locking every due job, then locks only exact claim ids. */
final class SqlserverBoostedClaimCandidates {
  private final SqlserverStoreContext ctx;
  private final int limit;
  private final int lockBatchSize;
  private final int filterParameters;
  private final Timestamp now;
  private final NodeTagFilter tags;
  private final ExecutionTargetFilter targets;
  private final String filters;
  private final Set<UUID> emitted = new HashSet<>();
  private final List<Stream> streams = new ArrayList<>();

  SqlserverBoostedClaimCandidates(
      SqlserverStoreContext ctx,
      JobExecutionType type,
      int limit,
      NodeTagFilter tags,
      ExecutionTargetFilter targets,
      Instant claimTime) {
    this.ctx = ctx;
    // The RUNNING update binds three values plus every claimed id, and SQL Server allows 2,100
    // parameters. A larger poller batch claims 2,097 jobs; a claim may return fewer than asked.
    this.limit = Math.min(limit, 2097);
    this.now = Timestamp.from(claimTime);
    this.tags = tags;
    this.targets = targets;
    this.filters =
        JobClaimSqlSupport.buildExecutionTargetFilterSql(targets, "execution_target")
            + JobClaimSqlSupport.buildTagFilterSql(tags, "scheduler_job_queue");
    int filterParameters = tags.requireTags().size() + tags.excludeTags().size();
    if (targets != null && !targets.isAny()) {
      filterParameters +=
          targets.isExclusion()
              ? targets.excludedTargets().size()
              : targets.explicitTargets().size();
    }
    this.filterParameters = filterParameters;
    // Leave room for the page cursor, timestamp, type and score parameters.
    if (filterParameters > 2091) {
      throw new IllegalArgumentException("Boosted claim exceeds SQL Server's parameter budget");
    }
    this.lockBatchSize = Math.min(this.limit, 2098 - filterParameters);
    List<JobExecutionType> types =
        type == null
            ? List.of(
                JobExecutionType.SINGLE,
                JobExecutionType.BATCH_CHILD,
                JobExecutionType.CHAIN_STEP,
                JobExecutionType.WORKFLOW_BRANCH)
            : List.of(type);
    for (JobExecutionType jobType : types) {
      for (int priority = 0; priority <= 4; priority++) {
        streams.add(new Stream(jobType, priority));
      }
    }
  }

  List<Object[]> lock(String columns) {
    initializePages();
    List<Object[]> locked = new ArrayList<>();
    while (locked.size() < limit) {
      List<Candidate> batch = new ArrayList<>();
      while (batch.size() < Math.min(lockBatchSize, limit - locked.size())) {
        Candidate next = next();
        if (next == null) break;
        // A concurrent reschedule can move a previously seen id into another stream/page.
        if (emitted.add(UuidByteArrayConverter.fromBytes(next.id()))) batch.add(next);
      }
      if (batch.isEmpty()) break;
      String placeholders = String.join(",", Collections.nCopies(batch.size(), "?"));
      String typeSql =
          streams.size() == 5
              ? "job_type = CAST(? AS VARCHAR(8000))"
              : "job_type IN ('SINGLE','BATCH_CHILD','CHAIN_STEP','WORKFLOW_BRANCH')";
      // language=SQL Server
      String sql =
          """
          SELECT %s
          FROM scheduler_job_queue WITH
            (UPDLOCK, READPAST, ROWLOCK, FORCESEEK(pk_scheduler_job_queue(job_id)))
          WHERE job_id IN (%s) AND status = 'PENDING' AND scheduled_time <= ?
            AND %s%s
          """
              .formatted(columns, placeholders, typeSql, filters);
      Query query = ctx.em().createNativeQuery(sql);
      int p = 1;
      for (Candidate candidate : batch) query.setParameter(p++, candidate.id());
      query.setParameter(p++, now);
      if (streams.size() == 5) query.setParameter(p++, streams.get(0).type.name());
      bindFilters(query, p);
      @SuppressWarnings("unchecked")
      List<Object[]> rows = query.getResultList();
      // IN does not promise result order. Restore the merge order of the rows actually locked.
      locked.addAll(
          JobClaimSqlSupport.reorderById(
              rows,
              batch.stream()
                  .map(candidate -> UuidByteArrayConverter.fromBytes(candidate.id()))
                  .toList(),
              row -> SqlserverJobRowMapper.uuidOrNull(row[0])));
    }
    return locked;
  }

  private void initializePages() {
    // Keep each TOP/ORDER BY inside its own derived table so every branch is an ordered seek.
    // Large filters fall back to separate pages rather than exceed the statement parameter limit.
    if (streams.size() * (6 + filterParameters) > 2100) return;
    List<String> branches = new ArrayList<>();
    for (int i = 0; i < streams.size(); i++) {
      branches.add(
          "SELECT "
              + i
              + " AS stream_index, candidate.* FROM ("
              + streams.get(i).pageSql()
              + ") candidate");
    }
    Query query = ctx.em().createNativeQuery(String.join(" UNION ALL ", branches));
    int p = 1;
    for (Stream stream : streams) p = stream.bindPage(query, p);
    @SuppressWarnings("unchecked")
    List<Object[]> rows = query.getResultList();
    List<List<Object[]>> pages = new ArrayList<>();
    for (int i = 0; i < streams.size(); i++) pages.add(new ArrayList<>());
    for (Object[] row : rows) {
      pages.get(((Number) row[0]).intValue()).add(Arrays.copyOfRange(row, 1, row.length));
    }
    // UNION ALL does not preserve branch order; restore the stream order before merging.
    for (int i = 0; i < streams.size(); i++) streams.get(i).acceptPage(pages.get(i));
  }

  private int bindFilters(Query query, int p) {
    p = JobClaimSqlSupport.bindExecutionTargetFilter(query, targets, p);
    return JobClaimSqlSupport.bindTagFilter(query, tags, p);
  }

  private Candidate next() {
    Stream best = null;
    for (Stream stream : streams) {
      stream.ensureHead();
      if (stream.offset < stream.page.size()
          && (best == null || stream.head().compareTo(best.head()) < 0)) best = stream;
    }
    if (best == null) return null;
    Candidate result = best.head();
    best.offset++;
    return result;
  }

  private record Candidate(byte[] id, Instant scheduled, BigDecimal score)
      implements Comparable<Candidate> {
    @Override
    public int compareTo(Candidate other) {
      int order = other.score.compareTo(score);
      if (order == 0) order = scheduled.compareTo(other.scheduled);
      return order == 0 ? Arrays.compareUnsigned(id, other.id) : order;
    }
  }

  private final class Stream {
    private final JobExecutionType type;
    private final int priority;
    private List<Candidate> page = List.of();
    private int offset;
    private boolean exhausted;
    private Candidate cursor;

    Stream(JobExecutionType type, int priority) {
      this.type = type;
      this.priority = priority;
    }

    Candidate head() {
      return page.get(offset);
    }

    void ensureHead() {
      if (offset < page.size() || exhausted) return;
      Query query = ctx.em().createNativeQuery(pageSql());
      bindPage(query, 1);
      @SuppressWarnings("unchecked")
      List<Object[]> rows = query.getResultList();
      acceptPage(rows);
    }

    String pageSql() {
      String keyset =
          cursor == null
              ? ""
              : """
              AND (scheduled_time > ? OR (scheduled_time = ? AND job_id > ?))
              """;
      // language=SQL Server
      return """
      SELECT TOP (?) job_id, scheduled_time,
        priority + FLOOR(GREATEST(0, DATEDIFF(SECOND, scheduled_time, ?)/60.0) / ?) AS effective_priority
      FROM scheduler_job_queue WITH (INDEX(idx_claim_pending_priority))
      WHERE job_type = CAST(? AS VARCHAR(8000)) AND priority = ?
        AND status = 'PENDING' AND scheduled_time <= ?%s
      %s
      ORDER BY scheduled_time, job_id
      """
          .formatted(filters, keyset);
    }

    int bindPage(Query query, int p) {
      query.setParameter(p++, limit);
      query.setParameter(p++, now);
      query.setParameter(p++, ctx.priorityBoostIntervalMinutes());
      query.setParameter(p++, type.name());
      query.setParameter(p++, priority);
      query.setParameter(p++, now);
      p = bindFilters(query, p);
      if (cursor != null) {
        Timestamp time = Timestamp.from(cursor.scheduled());
        query.setParameter(p++, time);
        query.setParameter(p++, time);
        query.setParameter(p++, cursor.id());
      }
      return p;
    }

    void acceptPage(List<Object[]> rows) {
      page = new ArrayList<>(rows.size());
      for (Object[] row : rows) {
        page.add(
            new Candidate(
                (byte[]) row[0],
                RowValues.instantOrNull(row[1]),
                new BigDecimal(row[2].toString())));
      }
      page.sort(Candidate::compareTo);
      offset = 0;
      exhausted = page.size() < limit;
      if (!page.isEmpty()) cursor = page.get(page.size() - 1);
    }
  }
}
