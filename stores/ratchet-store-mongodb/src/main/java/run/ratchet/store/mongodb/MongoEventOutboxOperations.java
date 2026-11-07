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
package run.ratchet.store.mongodb;

import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Filters.lt;

import com.mongodb.client.ClientSession;
import com.mongodb.client.model.Sorts;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.bson.Document;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import run.ratchet.store.dto.OutboxEvent;
import run.ratchet.store.dto.OutboxEventType;
import run.ratchet.store.spi.EventOutboxStore;

/** Mongo outbox snapshots share the completion's explicit session transaction. */
final class MongoEventOutboxOperations implements EventOutboxStore {
  private final MongoStoreContext ctx;

  MongoEventOutboxOperations(MongoStoreContext ctx) {
    this.ctx = ctx;
  }

  static void insert(MongoStoreContext ctx, ClientSession session, OutboxEvent event) {
    ctx.eventOutbox()
        .insertOne(
            session,
            new Document("_id", event.eventId())
                .append("event_type", event.eventType().name())
                .append("job_id", event.jobId())
                .append("business_key", event.businessKey())
                .append("recurring_master_id", event.recurringMasterId())
                .append("job_type", event.jobType() == null ? null : event.jobType().name())
                .append("priority", event.priority() == null ? null : event.priority().name())
                .append("node_id", event.nodeId())
                .append("event_ts", DocumentMapper.toDate(event.eventTimestamp()))
                .append("execution_time_ms", event.executionTimeMs())
                .append("error_message", event.errorMessage())
                .append("retry_attempt", event.retryAttempt())
                .append("created_at", DocumentMapper.toDate(event.createdAt())));
  }

  @Override
  public List<OutboxEvent> findEventsCreatedBefore(Instant cutoff, int limit) {
    Objects.requireNonNull(cutoff, "cutoff");
    if (limit < 1) throw new IllegalArgumentException("limit must be at least one");
    List<OutboxEvent> events = new ArrayList<>();
    for (Document row :
        ctx.eventOutbox()
            .find(lt("created_at", DocumentMapper.toDate(cutoff)))
            .sort(Sorts.ascending("created_at", "_id"))
            .limit(limit)) {
      events.add(
          new OutboxEvent(
              row.get("_id", UUID.class),
              OutboxEventType.valueOf(row.getString("event_type")),
              row.get("job_id", UUID.class),
              row.getString("business_key"),
              row.get("recurring_master_id", UUID.class),
              row.getString("job_type") == null ? null : JobType.valueOf(row.getString("job_type")),
              row.getString("priority") == null
                  ? null
                  : JobPriority.valueOf(row.getString("priority")),
              row.getString("node_id"),
              row.getDate("event_ts").toInstant(),
              row.get("execution_time_ms") == null
                  ? null
                  : ((Number) row.get("execution_time_ms")).longValue(),
              row.getString("error_message"),
              row.getInteger("retry_attempt"),
              row.getDate("created_at").toInstant()));
    }
    return List.copyOf(events);
  }

  @Override
  public int deleteEvents(Collection<UUID> eventIds) {
    List<UUID> ids = List.copyOf(eventIds);
    if (ids.size() > 500) throw new IllegalArgumentException("At most 500 event ids are allowed");
    if (ids.isEmpty()) return 0;
    return (int) ctx.eventOutbox().deleteMany(in("_id", ids)).getDeletedCount();
  }
}
