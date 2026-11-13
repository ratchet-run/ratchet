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

import static com.mongodb.client.model.Aggregates.group;
import static com.mongodb.client.model.Aggregates.limit;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.sort;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.expr;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Filters.lt;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.descending;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.inc;
import static com.mongodb.client.model.Updates.set;
import static com.mongodb.client.model.Updates.setOnInsert;
import static com.mongodb.client.model.Updates.unset;
import static run.ratchet.store.mongodb.MongoFieldNames.ACTIVE_COUNT;
import static run.ratchet.store.mongodb.MongoFieldNames.ATTEMPT;
import static run.ratchet.store.mongodb.MongoFieldNames.CHILD_JOB_ID;
import static run.ratchet.store.mongodb.MongoFieldNames.CLAIM_SEQ;
import static run.ratchet.store.mongodb.MongoFieldNames.CONDITION_PRIORITY;
import static run.ratchet.store.mongodb.MongoFieldNames.CONDITION_TYPE;
import static run.ratchet.store.mongodb.MongoFieldNames.CREATED_AT;
import static run.ratchet.store.mongodb.MongoFieldNames.DEFINITION_ORDER;
import static run.ratchet.store.mongodb.MongoFieldNames.DEPENDS_ON;
import static run.ratchet.store.mongodb.MongoFieldNames.DESCRIPTION;
import static run.ratchet.store.mongodb.MongoFieldNames.EXECUTION_TARGET;
import static run.ratchet.store.mongodb.MongoFieldNames.GENERATION;
import static run.ratchet.store.mongodb.MongoFieldNames.ID;
import static run.ratchet.store.mongodb.MongoFieldNames.JOB_ID;
import static run.ratchet.store.mongodb.MongoFieldNames.JOB_TYPE;
import static run.ratchet.store.mongodb.MongoFieldNames.LAST_ERROR;
import static run.ratchet.store.mongodb.MongoFieldNames.LEASE_UNTIL;
import static run.ratchet.store.mongodb.MongoFieldNames.MAX_CONCURRENT;
import static run.ratchet.store.mongodb.MongoFieldNames.NODE_ID;
import static run.ratchet.store.mongodb.MongoFieldNames.PARENT_JOB_ID;
import static run.ratchet.store.mongodb.MongoFieldNames.PAUSED_FROM_STATUS;
import static run.ratchet.store.mongodb.MongoFieldNames.PICKED_AT;
import static run.ratchet.store.mongodb.MongoFieldNames.PICKED_BY;
import static run.ratchet.store.mongodb.MongoFieldNames.PRIORITY;
import static run.ratchet.store.mongodb.MongoFieldNames.RESOURCE_NAME;
import static run.ratchet.store.mongodb.MongoFieldNames.RETRY_DELAY_MS;
import static run.ratchet.store.mongodb.MongoFieldNames.SCHEDULED_TIME;
import static run.ratchet.store.mongodb.MongoFieldNames.STATUS;
import static run.ratchet.store.mongodb.MongoFieldNames.TERMINATED_AT;
import static run.ratchet.store.mongodb.MongoFieldNames.TS;
import static run.ratchet.store.mongodb.MongoFieldNames.UPDATED_AT;
import static run.ratchet.store.mongodb.MongoFieldNames.VERSION;
import static run.ratchet.store.mongodb.MongoFieldNames.WAIT_SINCE;
import static run.ratchet.store.mongodb.MongoStoreContext.TRANSACTION_OPTIONS;

import com.mongodb.ReadPreference;
import com.mongodb.client.ClientSession;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.DeleteResult;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.jboss.logging.Logger;
import run.ratchet.api.Nullable;
import run.ratchet.api.ResourceLimit;
import run.ratchet.api.WorkflowCondition;
import run.ratchet.api.exception.ResourceNotConfiguredException;
import run.ratchet.store.entity.JobExecutionEntity;
import run.ratchet.store.entity.JobLogEntity;
import run.ratchet.store.entity.ResourcePermitEntity;
import run.ratchet.store.entity.WorkflowConditionEntity;
import run.ratchet.store.id.UuidV7Factory;
import run.ratchet.store.spi.ResourcePermitStore.AcquireResult;
import run.ratchet.store.spi.ResourcePermitStore.WokenWaiter;
import run.ratchet.store.spi.ResourceWaiterLease;
import run.ratchet.store.util.WorkflowConditionOrdering;

/**
 * Catch-all for smaller collections: job executions, job logs, workflow conditions, and resource
 * permits/limits. Each is independent — none needs any of the other op classes.
 *
 * <p>The resource-permit flow is the subtle one: {@code tryAcquirePermit} uses a {@code \$expr}
 * predicate to compare {@code active_count} against {@code max_concurrent} in the same document,
 * guaranteeing no TOCTOU race between read and increment.
 */
final class MongoAuxiliaryOperations {

  private static final Logger log = Logger.getLogger(MongoAuxiliaryOperations.class);
  private static final int WORKFLOW_CONDITION_WARNING_THRESHOLD = 1000;
  private static final int DEFAULT_PERMIT_RETRY_DELAY_MS = 5000;

  private final MongoStoreContext ctx;

  MongoAuxiliaryOperations(MongoStoreContext ctx) {
    this.ctx = ctx;
  }

  JobExecutionEntity saveExecution(JobExecutionEntity execution) {
    if (execution.getId() == null) {
      execution.setId(UuidV7Factory.create());
    }
    Document doc = DocumentMapper.toDocument(execution);
    ctx.executions().replaceOne(eq(ID, execution.getId()), doc, new ReplaceOptions().upsert(true));
    return execution;
  }

  List<JobExecutionEntity> findExecutionsByJobId(UUID jobId, int limit, int offset) {
    if (limit == 0) {
      return List.of();
    }
    List<JobExecutionEntity> results = new ArrayList<>();
    for (Document doc :
        ctx.executions()
            .find(eq(JOB_ID, jobId))
            .sort(ascending(ATTEMPT))
            .skip(offset)
            .limit(limit)) {
      results.add(DocumentMapper.toJobExecutionEntity(doc));
    }
    return results;
  }

  Optional<JobExecutionEntity> findLatestExecution(UUID jobId) {
    Document doc =
        ctx.executions().find(eq(JOB_ID, jobId)).sort(descending(ATTEMPT)).limit(1).first();
    return doc == null ? Optional.empty() : Optional.of(DocumentMapper.toJobExecutionEntity(doc));
  }

  int countExecutionAttempts(UUID jobId) {
    return (int) ctx.executions().countDocuments(eq(JOB_ID, jobId));
  }

  void appendLog(JobLogEntity logEntry) {
    if (logEntry.getId() == null) {
      logEntry.setId(UuidV7Factory.create());
    }
    ctx.jobLogs().insertOne(DocumentMapper.toDocument(logEntry));
  }

  int purgeLogsOlderThan(Instant cutoff) {
    DeleteResult result = ctx.jobLogs().deleteMany(lt(TS, DocumentMapper.toDate(cutoff)));
    return (int) result.getDeletedCount();
  }

  WorkflowConditionEntity saveCondition(WorkflowConditionEntity condition) {
    if (condition.getId() == null) {
      condition.setId(UuidV7Factory.create());
      if (condition.getCreatedAt() == null) {
        condition.setCreatedAt(Instant.now());
      }
    }
    Document doc = DocumentMapper.toDocument(condition);
    ctx.workflowConditions()
        .replaceOne(eq(ID, condition.getId()), doc, new ReplaceOptions().upsert(true));
    return condition;
  }

  WorkflowConditionEntity findConditionById(UUID id) {
    Document doc = ctx.workflowConditions().find(eq(ID, id)).first();
    return doc == null ? null : DocumentMapper.toWorkflowConditionEntity(doc);
  }

  List<WorkflowConditionEntity> findConditionsByParentJobId(UUID parentJobId) {
    List<WorkflowConditionEntity> results = new ArrayList<>();
    // Read the primary even when the client prefers secondaries: callbacks start and cancel from
    // this list, so a lagging secondary must not hide a condition written moments before.
    for (Document doc :
        ctx.workflowConditions()
            .withReadPreference(ReadPreference.primary())
            .find(eq(PARENT_JOB_ID, parentJobId))
            .sort(ascending(CONDITION_PRIORITY, DEFINITION_ORDER))) {
      results.add(DocumentMapper.toWorkflowConditionEntity(doc));
      warnIfLargeConditionResult("parent job", parentJobId, results.size());
    }
    return WorkflowConditionOrdering.sorted(results);
  }

  List<WorkflowConditionEntity> findConditionsByChildJobId(UUID childJobId) {
    List<WorkflowConditionEntity> results = new ArrayList<>();
    for (Document doc :
        ctx.workflowConditions()
            .find(eq(CHILD_JOB_ID, childJobId))
            .sort(ascending(CONDITION_PRIORITY, DEFINITION_ORDER))) {
      results.add(DocumentMapper.toWorkflowConditionEntity(doc));
      warnIfLargeConditionResult("child job", childJobId, results.size());
    }
    return WorkflowConditionOrdering.sorted(results);
  }

  List<WorkflowConditionEntity> findConditionsByType(
      UUID parentJobId, WorkflowCondition.ConditionType type) {
    List<WorkflowConditionEntity> results = new ArrayList<>();
    for (Document doc :
        ctx.workflowConditions()
            .find(and(eq(PARENT_JOB_ID, parentJobId), eq(CONDITION_TYPE, type.name())))
            .sort(ascending(CONDITION_PRIORITY, DEFINITION_ORDER))) {
      results.add(DocumentMapper.toWorkflowConditionEntity(doc));
      warnIfLargeConditionResult("parent job/type", parentJobId, results.size());
    }
    return WorkflowConditionOrdering.sorted(results);
  }

  private static void warnIfLargeConditionResult(String query, UUID id, int resultSize) {
    if (resultSize == WORKFLOW_CONDITION_WARNING_THRESHOLD + 1) {
      log.warnf(
          "MongoDB workflow-condition query by %s %s returned more than %d rows",
          query, id, WORKFLOW_CONDITION_WARNING_THRESHOLD);
    }
  }

  void deleteConditionById(UUID id) {
    ctx.workflowConditions().deleteOne(eq(ID, id));
  }

  void deleteConditionsByParentJobId(UUID parentJobId) {
    ctx.workflowConditions().deleteMany(eq(PARENT_JOB_ID, parentJobId));
  }

  void deleteConditionsByChildJobId(UUID childJobId) {
    ctx.workflowConditions().deleteMany(eq(CHILD_JOB_ID, childJobId));
  }

  long countConditionsByParentJobId(UUID parentJobId) {
    return ctx.workflowConditions().countDocuments(eq(PARENT_JOB_ID, parentJobId));
  }

  AcquireResult tryAcquirePermit(String resource, UUID jobId, long claimSeq, String nodeId) {
    try (ClientSession session = ctx.startSession()) {
      return session.withTransaction(
          () -> {
            // The first operation must write: even a full-pool miss serializes with wake-ups.
            Document limit = incrementGeneration(session, resource);
            if (limit == null) {
              throw new ResourceNotConfiguredException(resource);
            }
            Document job =
                ctx.jobs()
                    .find(
                        session, and(eq(ID, jobId), eq(STATUS, "RUNNING"), eq(CLAIM_SEQ, claimSeq)))
                    .first();
            if (job == null) {
              return AcquireResult.SUPERSEDED;
            }
            UUID permitId = ResourcePermitEntity.idFor(resource, jobId, claimSeq);
            Document existing = ctx.resourcePermits().find(session, eq(ID, permitId)).first();
            if (existing != null) {
              Date acquiredAt = Date.from(Instant.now());
              Date previous = existing.getDate("acquired_at");
              if (previous != null && !acquiredAt.after(previous)) {
                acquiredAt = new Date(previous.getTime() + 1);
              }
              ctx.resourcePermits()
                  .updateOne(
                      session,
                      eq(ID, permitId),
                      combine(set(NODE_ID, nodeId), set("acquired_at", acquiredAt)));
              return AcquireResult.ACQUIRED;
            }

            Instant now = Instant.now();
            Document waiter = ctx.resourceWaiters().find(session, eq(ID, jobId)).first();
            int free = limit.getInteger(MAX_CONCURRENT) - limit.getInteger(ACTIVE_COUNT, 0);
            if (free <= 0 || waitersAhead(session, resource, job, waiter, now, free) >= free) {
              return parkWaiter(
                  session,
                  resource,
                  job,
                  claimSeq,
                  limit.getInteger(RETRY_DELAY_MS, DEFAULT_PERMIT_RETRY_DELAY_MS),
                  now);
            }

            Document result =
                ctx.resourceLimits()
                    .findOneAndUpdate(
                        session,
                        and(
                            eq(ID, resource),
                            expr(
                                new Document(
                                    "$lt",
                                    List.of(
                                        new Document("$ifNull", List.of("$" + ACTIVE_COUNT, 0)),
                                        "$" + MAX_CONCURRENT)))),
                        inc(ACTIVE_COUNT, 1),
                        new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));

            if (result == null) {
              return parkWaiter(
                  session,
                  resource,
                  job,
                  claimSeq,
                  limit.getInteger(RETRY_DELAY_MS, DEFAULT_PERMIT_RETRY_DELAY_MS),
                  now);
            }

            ResourcePermitEntity permit =
                ResourcePermitEntity.create(resource, jobId, claimSeq, nodeId);
            ctx.resourcePermits().insertOne(session, DocumentMapper.toDocument(permit));
            if (waiter != null) {
              ctx.resourceWaiters().deleteOne(session, eq(ID, jobId));
            }
            return AcquireResult.ACQUIRED;
          },
          TRANSACTION_OPTIONS);
    } catch (IllegalArgumentException e) {
      throw e;
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("acquire resource permit", e);
    }
  }

  private Document incrementGeneration(ClientSession session, String resource) {
    return ctx.resourceLimits()
        .findOneAndUpdate(
            session,
            eq(ID, resource),
            inc(GENERATION, 1L),
            new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));
  }

  private List<Document> liveWaiters(
      ClientSession session, String resource, Instant now, Document ahead, int limit) {
    List<Bson> pipeline = new ArrayList<>();
    Document match =
        new Document(RESOURCE_NAME, resource)
            .append(LEASE_UNTIL, new Document("$gt", Date.from(now)));
    if (ahead != null) {
      match.putAll(ahead);
    }
    pipeline.add(new Document("$match", match));
    pipeline.add(
        new Document("$sort", new Document(PRIORITY, -1).append(WAIT_SINCE, 1).append(ID, 1)));
    pipeline.add(
        new Document(
            "$lookup",
            new Document("from", "scheduler_job")
                .append("localField", ID)
                .append("foreignField", ID)
                .append("as", "job")));
    pipeline.add(new Document("$unwind", "$job"));
    pipeline.add(
        new Document(
            "$match",
            new Document("job." + STATUS, new Document("$in", List.of("PENDING", "RUNNING")))));
    pipeline.add(new Document("$limit", limit));
    return ctx.resourceWaiters().aggregate(session, pipeline).into(new ArrayList<>());
  }

  private int waitersAhead(
      ClientSession session,
      String resource,
      Document job,
      Document waiter,
      Instant now,
      int free) {
    int priority = (waiter == null ? job : waiter).getInteger(PRIORITY);
    Date since = waiter == null ? job.getDate(SCHEDULED_TIME) : waiter.getDate(WAIT_SINCE);
    Document ahead =
        new Document(ID, new Document("$ne", job.get(ID)))
            .append(
                "$or",
                List.of(
                    new Document(PRIORITY, new Document("$gt", priority)),
                    new Document(PRIORITY, priority).append(WAIT_SINCE, new Document("$lt", since)),
                    new Document(PRIORITY, priority)
                        .append(WAIT_SINCE, since)
                        .append(ID, new Document("$lt", job.get(ID)))));
    return liveWaiters(session, resource, now, ahead, free).size();
  }

  private AcquireResult parkWaiter(
      ClientSession session,
      String resource,
      Document job,
      long claimSeq,
      int retryDelayMs,
      Instant now) {
    boolean paused = false;
    UUID batchId = job.get(DEPENDS_ON, UUID.class);
    if ("BATCH_CHILD".equals(job.getString(JOB_TYPE)) && batchId != null) {
      Document parent =
          ctx.jobs()
              .findOneAndUpdate(
                  session,
                  eq(ID, batchId),
                  inc(VERSION, 1),
                  new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));
      paused = parent != null && "PAUSED".equals(parent.getString(STATUS));
    }
    UUID jobId = job.get(ID, UUID.class);
    long matched =
        ctx.jobs()
            .updateOne(
                session,
                and(eq(ID, jobId), eq(STATUS, "RUNNING"), eq(CLAIM_SEQ, claimSeq)),
                combine(
                    set(STATUS, paused ? "PAUSED" : "PENDING"),
                    set(PAUSED_FROM_STATUS, paused ? "PENDING" : null),
                    set(LAST_ERROR, "Waiting for resource: " + resource),
                    set(SCHEDULED_TIME, Date.from(now.plusMillis(retryDelayMs))),
                    set(PICKED_BY, null),
                    set(PICKED_AT, null),
                    unset(TERMINATED_AT),
                    set(UPDATED_AT, Date.from(now)),
                    inc(VERSION, 1)))
            .getMatchedCount();
    if (matched == 0) {
      return AcquireResult.SUPERSEDED;
    }
    ctx.resourceWaiters()
        .updateOne(
            session,
            eq(ID, jobId),
            combine(
                set(
                    LEASE_UNTIL,
                    Date.from(now.plusMillis(ResourceWaiterLease.leaseMillis(retryDelayMs)))),
                setOnInsert(RESOURCE_NAME, resource),
                setOnInsert(PRIORITY, job.getInteger(PRIORITY)),
                setOnInsert(WAIT_SINCE, job.getDate(SCHEDULED_TIME))),
            new UpdateOptions().upsert(true));
    return AcquireResult.AT_CAPACITY;
  }

  List<WokenWaiter> wakeWaiters(String resource) {
    try (ClientSession session = ctx.startSession()) {
      return session.withTransaction(
          () -> {
            Document limit = incrementGeneration(session, resource);
            if (limit == null) {
              return List.of();
            }
            int max = limit.getInteger(MAX_CONCURRENT);
            List<Document> heads = liveWaiters(session, resource, Instant.now(), null, max);
            if (heads.isEmpty()) {
              return List.of();
            }
            int free = max - limit.getInteger(ACTIVE_COUNT, 0);
            List<WokenWaiter> result = new ArrayList<>();
            for (Document head : heads.subList(0, Math.min(Math.max(free, 0), heads.size()))) {
              Document job = head.get("job", Document.class);
              if (!"PENDING".equals(job.getString(STATUS))) {
                continue;
              }
              UUID id = head.get(ID, UUID.class);
              if (ctx.jobs()
                      .updateOne(
                          session,
                          and(eq(ID, id), eq(STATUS, "PENDING")),
                          combine(
                              set(SCHEDULED_TIME, head.getDate(WAIT_SINCE)),
                              set(UPDATED_AT, Date.from(Instant.now())),
                              inc(VERSION, 1)))
                      .getMatchedCount()
                  == 1) {
                result.add(
                    new WokenWaiter(
                        id, head.getInteger(PRIORITY), job.getString(EXECUTION_TARGET)));
              }
            }
            return result;
          },
          TRANSACTION_OPTIONS);
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("wake resource waiters", e);
    }
  }

  List<String> findResourcesWithWaiters(@Nullable String after, int limit) {
    if (limit <= 0) {
      return List.of();
    }
    try {
      List<Bson> pipeline = new ArrayList<>();
      if (after != null) {
        pipeline.add(match(gt(RESOURCE_NAME, after)));
      }
      pipeline.add(sort(ascending(RESOURCE_NAME)));
      pipeline.add(group("$" + RESOURCE_NAME));
      pipeline.add(sort(ascending(ID)));
      pipeline.add(limit(limit));
      return ctx.resourceWaiters().aggregate(pipeline).into(new ArrayList<>()).stream()
          .map(row -> row.getString(ID))
          .toList();
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("find resources with waiters", e);
    }
  }

  int deleteStaleWaiters(int limit) {
    if (limit <= 0) {
      return 0;
    }
    try (ClientSession session = ctx.startSession()) {
      return session.withTransaction(
          () -> {
            List<Document> stale =
                ctx.resourceWaiters()
                    .aggregate(
                        session,
                        List.of(
                            new Document(
                                "$lookup",
                                new Document("from", "scheduler_job")
                                    .append("localField", ID)
                                    .append("foreignField", ID)
                                    .append("as", "job")),
                            new Document(
                                "$match",
                                new Document(
                                    "job." + STATUS,
                                    new Document("$nin", List.of("PENDING", "RUNNING", "PAUSED")))),
                            new Document("$limit", limit),
                            new Document("$project", new Document(ID, 1))))
                    .into(new ArrayList<>());
            if (stale.isEmpty()) {
              return 0;
            }
            return (int)
                ctx.resourceWaiters()
                    .deleteMany(session, in(ID, stale.stream().map(doc -> doc.get(ID)).toList()))
                    .getDeletedCount();
          },
          TRANSACTION_OPTIONS);
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("delete stale resource waiters", e);
    }
  }

  void releasePermit(String resource, UUID jobId, long claimSeq) {
    try (ClientSession session = ctx.startSession()) {
      session.withTransaction(
          () -> {
            DeleteResult dr =
                ctx.resourcePermits()
                    .deleteOne(
                        session, eq(ID, ResourcePermitEntity.idFor(resource, jobId, claimSeq)));
            if (dr.getDeletedCount() > 0) {
              ctx.resourceLimits().updateOne(session, eq(ID, resource), inc(ACTIVE_COUNT, -1));
            }
            return Boolean.TRUE;
          },
          TRANSACTION_OPTIONS);
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("release resource permit", e);
    }
  }

  void releaseAllPermits(UUID jobId) {
    try (ClientSession session = ctx.startSession()) {
      session.withTransaction(
          () -> {
            Map<String, Integer> releasedByResource = new HashMap<>();
            ctx.resourcePermits()
                .find(session, eq(JOB_ID, jobId))
                .forEach(
                    doc -> releasedByResource.merge(doc.getString(RESOURCE_NAME), 1, Integer::sum));
            DeleteResult dr = ctx.resourcePermits().deleteMany(session, eq(JOB_ID, jobId));
            if (dr.getDeletedCount() > 0) {
              decrementResourceCounts(session, releasedByResource);
            }
            return Boolean.TRUE;
          },
          TRANSACTION_OPTIONS);
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("release all permits", e);
    }
  }

  int getPermitRetryDelay(String resource) {
    Document doc = ctx.resourceLimits().find(eq(ID, resource)).first();
    if (doc == null) {
      return DEFAULT_PERMIT_RETRY_DELAY_MS;
    }
    return doc.getInteger(RETRY_DELAY_MS, DEFAULT_PERMIT_RETRY_DELAY_MS);
  }

  // Read the primary even when the client prefers secondaries: submit() checks the limit right
  // after setResourceLimit wrote it, so a lagging secondary must not hide it.
  Optional<ResourceLimit> findResourceLimit(String name) {
    try {
      Document doc =
          ctx.resourceLimits()
              .withReadPreference(ReadPreference.primary())
              .find(eq(ID, name))
              .first();
      return doc == null ? Optional.empty() : Optional.of(mapResourceLimit(doc));
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("find resource limit", e);
    }
  }

  List<ResourceLimit> listResourceLimits() {
    try {
      return ctx.resourceLimits()
          .withReadPreference(ReadPreference.primary())
          .find()
          .map(MongoAuxiliaryOperations::mapResourceLimit)
          .into(new ArrayList<>());
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("list resource limits", e);
    }
  }

  private static ResourceLimit mapResourceLimit(Document doc) {
    return new ResourceLimit(
        doc.getString(ID),
        ((Number) doc.get(MAX_CONCURRENT)).intValue(),
        Duration.ofMillis(((Number) doc.get(RETRY_DELAY_MS)).intValue()),
        doc.getString(DESCRIPTION));
  }

  void configureResource(String name, int maxConcurrent, int retryDelayMs, String description) {
    try {
      Instant now = Instant.now();
      ctx.resourceLimits()
          .updateOne(
              eq(ID, name),
              combine(
                  set(MAX_CONCURRENT, maxConcurrent),
                  set(RETRY_DELAY_MS, retryDelayMs),
                  set(DESCRIPTION, description),
                  set(UPDATED_AT, DocumentMapper.toDate(now)),
                  setOnInsert(CREATED_AT, DocumentMapper.toDate(now)),
                  setOnInsert(ACTIVE_COUNT, 0)),
              new UpdateOptions().upsert(true));
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("configure resource", e);
    }
  }

  int cleanupOrphanedPermits(List<String> staleNodeIds) {
    if (staleNodeIds.isEmpty()) {
      return 0;
    }
    try (ClientSession session = ctx.startSession()) {
      return session.withTransaction(
          () -> {
            Map<String, Integer> removedByResource = new HashMap<>();
            ctx.resourcePermits()
                .find(session, in(NODE_ID, staleNodeIds))
                .forEach(
                    doc -> removedByResource.merge(doc.getString(RESOURCE_NAME), 1, Integer::sum));
            DeleteResult result =
                ctx.resourcePermits().deleteMany(session, in(NODE_ID, staleNodeIds));
            decrementResourceCounts(session, removedByResource);
            return (int) result.getDeletedCount();
          },
          TRANSACTION_OPTIONS);
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("cleanup orphaned permits", e);
    }
  }

  private void decrementResourceCounts(ClientSession session, Map<String, Integer> counts) {
    if (counts.isEmpty()) {
      return;
    }
    List<WriteModel<Document>> writes = new ArrayList<>(counts.size());
    counts.forEach(
        (resource, count) ->
            writes.add(new UpdateOneModel<>(eq(ID, resource), inc(ACTIVE_COUNT, -count))));
    ctx.resourceLimits().bulkWrite(session, writes, new BulkWriteOptions().ordered(false));
  }
}
