package run.ratchet.store.mongodb;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.expr;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Filters.lt;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.descending;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.inc;
import static com.mongodb.client.model.Updates.set;
import static com.mongodb.client.model.Updates.setOnInsert;
import static run.ratchet.store.mongodb.MongoFieldNames.ACTIVE_COUNT;
import static run.ratchet.store.mongodb.MongoFieldNames.ALERT_SENT_AT;
import static run.ratchet.store.mongodb.MongoFieldNames.ATTEMPT;
import static run.ratchet.store.mongodb.MongoFieldNames.CHILD_JOB_ID;
import static run.ratchet.store.mongodb.MongoFieldNames.CONDITION_PRIORITY;
import static run.ratchet.store.mongodb.MongoFieldNames.CONDITION_TYPE;
import static run.ratchet.store.mongodb.MongoFieldNames.CREATED_AT;
import static run.ratchet.store.mongodb.MongoFieldNames.DESCRIPTION;
import static run.ratchet.store.mongodb.MongoFieldNames.ERROR_HASH;
import static run.ratchet.store.mongodb.MongoFieldNames.ID;
import static run.ratchet.store.mongodb.MongoFieldNames.JOB_ID;
import static run.ratchet.store.mongodb.MongoFieldNames.MAX_CONCURRENT;
import static run.ratchet.store.mongodb.MongoFieldNames.NODE_ID;
import static run.ratchet.store.mongodb.MongoFieldNames.PARENT_JOB_ID;
import static run.ratchet.store.mongodb.MongoFieldNames.RESOURCE_NAME;
import static run.ratchet.store.mongodb.MongoFieldNames.RETRY_DELAY_MS;
import static run.ratchet.store.mongodb.MongoFieldNames.TS;
import static run.ratchet.store.mongodb.MongoFieldNames.UPDATED_AT;

import com.mongodb.client.ClientSession;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.DeleteResult;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.bson.Document;
import org.jboss.logging.Logger;
import run.ratchet.api.WorkflowCondition;
import run.ratchet.store.entity.DlqAlertEntity;
import run.ratchet.store.entity.JobExecutionEntity;
import run.ratchet.store.entity.JobLogEntity;
import run.ratchet.store.entity.ResourcePermitEntity;
import run.ratchet.store.entity.WorkflowConditionEntity;
import run.ratchet.store.id.UuidV7Factory;

/**
 * Catch-all for smaller collections: job executions, job logs, workflow conditions, DLQ alerts, and
 * resource permits/limits. Each is independent — none needs any of the other op classes.
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
    for (Document doc :
        ctx.workflowConditions()
            .find(eq(PARENT_JOB_ID, parentJobId))
            .sort(ascending(CONDITION_PRIORITY))) {
      results.add(DocumentMapper.toWorkflowConditionEntity(doc));
      warnIfLargeConditionResult("parent job", parentJobId, results.size());
    }
    return results;
  }

  List<WorkflowConditionEntity> findConditionsByChildJobId(UUID childJobId) {
    List<WorkflowConditionEntity> results = new ArrayList<>();
    for (Document doc : ctx.workflowConditions().find(eq(CHILD_JOB_ID, childJobId))) {
      results.add(DocumentMapper.toWorkflowConditionEntity(doc));
      warnIfLargeConditionResult("child job", childJobId, results.size());
    }
    return results;
  }

  List<WorkflowConditionEntity> findConditionsByType(
      UUID parentJobId, WorkflowCondition.ConditionType type) {
    List<WorkflowConditionEntity> results = new ArrayList<>();
    for (Document doc :
        ctx.workflowConditions()
            .find(and(eq(PARENT_JOB_ID, parentJobId), eq(CONDITION_TYPE, type.name())))) {
      results.add(DocumentMapper.toWorkflowConditionEntity(doc));
      warnIfLargeConditionResult("parent job/type", parentJobId, results.size());
    }
    return results;
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

  DlqAlertEntity saveDlqAlert(DlqAlertEntity alert) {
    if (alert.getId() == null) {
      alert.setId(UuidV7Factory.create());
    }
    Document doc = DocumentMapper.toDocument(alert);
    ctx.dlqAlerts().replaceOne(eq(ID, alert.getId()), doc, new ReplaceOptions().upsert(true));
    return alert;
  }

  boolean existsRecentDlqAlert(UUID jobId, String errorHash, Instant cutoff) {
    return ctx.dlqAlerts()
            .countDocuments(
                and(
                    eq(JOB_ID, jobId),
                    eq(ERROR_HASH, errorHash),
                    gte(ALERT_SENT_AT, DocumentMapper.toDate(cutoff))))
        > 0;
  }

  boolean tryAcquirePermit(String resource, UUID jobId, String nodeId) {
    try (ClientSession session = ctx.startSession()) {
      return session.withTransaction(
          () -> {
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
              if (ctx.resourceLimits().find(session, eq(ID, resource)).first() == null) {
                throw new IllegalArgumentException("Resource is not configured: " + resource);
              }
              return false;
            }

            ResourcePermitEntity permit = ResourcePermitEntity.create(resource, jobId, nodeId);
            permit.setId(UuidV7Factory.create());
            ctx.resourcePermits().insertOne(session, DocumentMapper.toDocument(permit));
            return true;
          });
    } catch (IllegalArgumentException e) {
      throw e;
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("acquire resource permit", e);
    }
  }

  void releasePermit(String resource, UUID jobId) {
    try (ClientSession session = ctx.startSession()) {
      session.withTransaction(
          () -> {
            DeleteResult dr =
                ctx.resourcePermits()
                    .deleteOne(session, and(eq(RESOURCE_NAME, resource), eq(JOB_ID, jobId)));
            if (dr.getDeletedCount() > 0) {
              ctx.resourceLimits().updateOne(session, eq(ID, resource), inc(ACTIVE_COUNT, -1));
            }
            return Boolean.TRUE;
          });
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
          });
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

  void configureResource(String name, int maxConcurrent, int retryDelayMs, String description) {
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
          });
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
