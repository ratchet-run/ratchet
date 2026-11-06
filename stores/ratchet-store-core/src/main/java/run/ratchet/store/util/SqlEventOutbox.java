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
package run.ratchet.store.util;

import jakarta.persistence.EntityManager;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import run.ratchet.store.dto.OutboxEvent;
import run.ratchet.store.dto.OutboxEventType;

/** Native SQL outbox operations shared by the four SQL stores. Uses the caller's transaction. */
public final class SqlEventOutbox {
  private SqlEventOutbox() {}

  /**
   * Inserts one outbox row in the caller's transaction.
   *
   * @param sqlServer true on SQL Server, where an untyped null binds as nvarchar and is refused for
   *     a BINARY(16) column, so the nullable recurring master id gets an explicit cast
   */
  public static void insert(
      EntityManager em, OutboxEvent event, boolean sqlServer, Function<UUID, Object> uuid) {
    String recurringMaster = sqlServer ? "CAST(? AS BINARY(16))" : "?";
    em.createNativeQuery(
            """
            INSERT INTO scheduler_event_outbox
              (event_id, event_type, job_id, business_key, recurring_master_id, job_type,
               priority, node_id, event_ts, execution_time_ms, error_message, retry_attempt, created_at)
            VALUES (?, ?, ?, ?, %s, ?, ?, ?, ?, ?, ?, ?, ?)
            """
                .formatted(recurringMaster))
        .setParameter(1, uuid.apply(event.eventId()))
        .setParameter(2, event.eventType().name())
        .setParameter(3, uuid.apply(event.jobId()))
        .setParameter(4, event.businessKey())
        .setParameter(
            5, event.recurringMasterId() == null ? null : uuid.apply(event.recurringMasterId()))
        .setParameter(6, event.jobType() == null ? null : event.jobType().name())
        .setParameter(7, event.priority() == null ? null : event.priority().name())
        .setParameter(8, event.nodeId())
        .setParameter(9, Timestamp.from(event.eventTimestamp()))
        .setParameter(10, event.executionTimeMs())
        .setParameter(11, event.errorMessage())
        .setParameter(12, event.retryAttempt())
        .setParameter(13, Timestamp.from(event.createdAt()))
        .executeUpdate();
  }

  public static List<OutboxEvent> find(EntityManager em, Instant cutoff, int limit) {
    Objects.requireNonNull(cutoff, "cutoff");
    if (limit < 1) throw new IllegalArgumentException("limit must be at least one");
    List<?> rows =
        em.createNativeQuery(
                """
                SELECT event_id, event_type, job_id, business_key, recurring_master_id, job_type,
                       priority, node_id, event_ts, execution_time_ms, error_message, retry_attempt, created_at
                FROM scheduler_event_outbox
                WHERE created_at < ?
                ORDER BY created_at, event_id
                """)
            .setParameter(1, Timestamp.from(cutoff))
            .setMaxResults(limit)
            .getResultList();
    return rows.stream().map(row -> read((Object[]) row)).toList();
  }

  private static OutboxEvent read(Object[] row) {
    return new OutboxEvent(
        RowValues.uuidOrNull(row[0]),
        OutboxEventType.valueOf(RowValues.stringOrNull(row[1])),
        RowValues.uuidOrNull(row[2]),
        RowValues.stringOrNull(row[3]),
        RowValues.uuidOrNull(row[4]),
        row[5] == null ? null : JobType.valueOf(RowValues.stringOrNull(row[5])),
        row[6] == null ? null : JobPriority.valueOf(RowValues.stringOrNull(row[6])),
        RowValues.stringOrNull(row[7]),
        RowValues.instantOrNull(row[8]),
        RowValues.longOrNull(row[9]),
        RowValues.stringOrNull(row[10]),
        ((Number) row[11]).intValue(),
        RowValues.instantOrNull(row[12]));
  }

  public static int delete(EntityManager em, Collection<UUID> ids, Function<UUID, Object> uuid) {
    List<UUID> values = List.copyOf(ids);
    if (values.size() > 500)
      throw new IllegalArgumentException("At most 500 event ids are allowed");
    if (values.isEmpty()) return 0;
    String parameters =
        IntStream.range(0, values.size()).mapToObj(i -> "?").collect(Collectors.joining(", "));
    var query =
        em.createNativeQuery(
            "DELETE FROM scheduler_event_outbox WHERE event_id IN (" + parameters + ")");
    for (int i = 0; i < values.size(); i++) query.setParameter(i + 1, uuid.apply(values.get(i)));
    return query.executeUpdate();
  }
}
