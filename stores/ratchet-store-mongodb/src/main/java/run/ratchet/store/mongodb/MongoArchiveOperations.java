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

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.exists;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Filters.lt;
import static com.mongodb.client.model.Filters.lte;
import static com.mongodb.client.model.Filters.or;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.descending;
import static run.ratchet.store.mongodb.MongoFieldNames.ARCHIVED_AT;
import static run.ratchet.store.mongodb.MongoFieldNames.BUSINESS_KEY;
import static run.ratchet.store.mongodb.MongoFieldNames.ENCRYPTED_STATE;
import static run.ratchet.store.mongodb.MongoFieldNames.ENCRYPTION_KEY_ID;
import static run.ratchet.store.mongodb.MongoFieldNames.ID;
import static run.ratchet.store.mongodb.MongoFieldNames.JOB_ID;
import static run.ratchet.store.mongodb.MongoFieldNames.NAMESPACE;
import static run.ratchet.store.mongodb.MongoFieldNames.PROPERTY_KEY;
import static run.ratchet.store.mongodb.MongoFieldNames.STATE;
import static run.ratchet.store.mongodb.MongoFieldNames.STATUS;
import static run.ratchet.store.mongodb.MongoFieldNames.TARGET_CLASS;
import static run.ratchet.store.mongodb.MongoFieldNames.TERMINATED_AT;
import static run.ratchet.store.mongodb.MongoFieldNames.UPDATED_AT;
import static run.ratchet.store.mongodb.MongoFieldNames.VALUE;
import static run.ratchet.store.mongodb.MongoFieldNames.VERSION;

import com.mongodb.client.ClientSession;
import com.mongodb.client.result.DeleteResult;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.bson.Document;
import org.bson.conversions.Bson;
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.store.entity.ArchivedJobEntity;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.id.UuidV7Factory;
import run.ratchet.store.spi.ArchiveStore;
import run.ratchet.store.util.ExtensionArchiveJson;

/**
 * Archive operations over {@code scheduler_job_archive}. Terminal-state jobs matching the retention
 * cutoff are projected into {@link ArchivedJobEntity} documents. Batch archiving moves MongoDB rows
 * inside one Mongo transaction because the Jakarta transaction on the caller cannot enlist this
 * driver session.
 */
final class MongoArchiveOperations implements ArchiveStore {

  private final MongoStoreContext ctx;
  private final Clock clock;

  MongoArchiveOperations(MongoStoreContext ctx) {
    this(ctx, Clock.systemUTC());
  }

  MongoArchiveOperations(MongoStoreContext ctx, Clock clock) {
    this.ctx = ctx;
    this.clock = Objects.requireNonNull(clock, "clock");
  }

  /**
   * Archives one terminal job and atomically deletes it from the active collection. Consistent with
   * the batch variant: both insert and delete succeed together or neither does.
   */
  @Override
  public ArchivedJobEntity archiveJob(JobEntity job, String reason, String archivedBy) {
    ArchivedJobEntity archive = buildArchive(job, reason, archivedBy);
    archive.setId(UuidV7Factory.create());
    populateExtensionData(archive, job.getId());
    Document doc = DocumentMapper.toDocument(archive);
    try (ClientSession session = ctx.startSession()) {
      session.withTransaction(
          () -> {
            ctx.archives().insertOne(session, doc);
            // Only delete a job that is still terminal: a concurrent reset to PENDING must not be
            // archived away. If nothing was deleted, the snapshot is stale, so roll back.
            DeleteResult deleted =
                ctx.jobs()
                    .deleteOne(
                        session,
                        and(eq(ID, job.getId()), in(STATUS, MongoStoreContext.TERMINAL_STATUSES)));
            if (deleted.getDeletedCount() == 0) {
              throw new RatchetTransientStoreException(
                  "Archive raced a status change on job " + job.getId() + "; rolling back");
            }
            deleteExtensionData(session, List.of(job.getId()));
            return Boolean.TRUE;
          });
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("archive job", e);
    }
    return archive;
  }

  @Override
  public int archiveJobsBatch(List<JobEntity> jobList, String reason, String archivedBy) {
    if (jobList.isEmpty()) {
      return 0;
    }
    try (ClientSession session = ctx.startSession()) {
      return session.withTransaction(
          () -> {
            int archived = archiveJobsBatch(session, jobList, reason, archivedBy);
            if (archived > 0) {
              List<UUID> ids = jobList.stream().limit(archived).map(JobEntity::getId).toList();
              // Guard the delete on terminal status: a job reset to PENDING (e.g. a dashboard
              // retry)
              // between findJobsForArchiving and this transaction must not be deleted, or the retry
              // silently vanishes. Deleting fewer rows than we archived means a snapshot went
              // stale,
              // so roll the whole batch back and let the next retention pass re-pick the survivors.
              DeleteResult deleted =
                  ctx.jobs()
                      .deleteMany(
                          session,
                          and(in(ID, ids), in(STATUS, MongoStoreContext.TERMINAL_STATUSES)));
              if (deleted.getDeletedCount() != archived) {
                throw new RatchetTransientStoreException(
                    "Archive batch raced a status change: archived "
                        + archived
                        + " but deleted "
                        + deleted.getDeletedCount()
                        + "; rolling back");
              }
              deleteExtensionData(session, ids);
            }
            return archived;
          });
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("archive jobs batch", e);
    }
  }

  int archiveJobsBatch(
      ClientSession session, List<JobEntity> jobList, String reason, String archivedBy) {
    if (jobList.isEmpty()) {
      return 0;
    }
    List<UUID> jobIds = jobList.stream().map(JobEntity::getId).toList();
    Map<UUID, Map<String, String>> propertiesByJobId = extensionProperties(session, jobIds);
    Map<UUID, List<ExtensionArchiveJson.StateRow>> statesByJobId = extensionStates(session, jobIds);
    List<Document> docs = new ArrayList<>(jobList.size());
    for (JobEntity job : jobList) {
      ArchivedJobEntity archive = buildArchive(job, reason, archivedBy);
      archive.setId(UuidV7Factory.create());
      populateExtensionData(
          archive,
          propertiesByJobId.getOrDefault(job.getId(), Map.of()),
          statesByJobId.getOrDefault(job.getId(), List.of()));
      docs.add(DocumentMapper.toDocument(archive));
    }
    ctx.archives().insertMany(session, docs);
    return docs.size();
  }

  @Override
  public List<JobEntity> findJobsForArchiving(Instant olderThan, int limit) {
    List<JobEntity> results = new ArrayList<>();
    for (Document doc :
        ctx.jobs()
            .find(terminalOlderThan(olderThan))
            .sort(ascending(TERMINATED_AT, UPDATED_AT))
            .limit(limit)) {
      results.add(DocumentMapper.toJobEntity(doc));
    }
    return results;
  }

  @Override
  public long countJobsForArchiving(Instant olderThan) {
    return ctx.jobs().countDocuments(terminalOlderThan(olderThan));
  }

  @Override
  public List<ArchivedJobEntity> findArchivedJobs(
      String targetClass, String businessKey, Instant from, Instant to, int limit) {
    List<Bson> filters = new ArrayList<>();
    if (targetClass != null) {
      filters.add(eq(TARGET_CLASS, targetClass));
    }
    if (businessKey != null) {
      filters.add(eq(BUSINESS_KEY, businessKey));
    }
    if (from != null) {
      filters.add(gte(ARCHIVED_AT, DocumentMapper.toDate(from)));
    }
    if (to != null) {
      filters.add(lte(ARCHIVED_AT, DocumentMapper.toDate(to)));
    }

    Bson filter = filters.isEmpty() ? new Document() : and(filters);
    List<ArchivedJobEntity> results = new ArrayList<>();
    for (Document doc : ctx.archives().find(filter).sort(descending(ARCHIVED_AT)).limit(limit)) {
      results.add(DocumentMapper.toArchivedJobEntity(doc));
    }
    return results;
  }

  @Override
  public int purgeArchivedJobs(Instant olderThan) {
    DeleteResult result =
        ctx.archives().deleteMany(lt(ARCHIVED_AT, DocumentMapper.toDate(olderThan)));
    return (int) result.getDeletedCount();
  }

  static Bson terminalOlderThan(Instant olderThan) {
    var cutoff = DocumentMapper.toDate(olderThan);
    return and(
        in(STATUS, MongoStoreContext.TERMINAL_STATUSES),
        or(lt(TERMINATED_AT, cutoff), and(exists(TERMINATED_AT, false), lt(UPDATED_AT, cutoff))));
  }

  private ArchivedJobEntity buildArchive(JobEntity job, String reason, String archivedBy) {
    ArchivedJobEntity a = new ArchivedJobEntity();
    a.setOriginalJobId(job.getId());
    a.setFinalStatus(job.getStatus());
    a.setJobType(job.getJobType());
    a.setPriority(job.getPriority());
    a.setTotalAttempts(job.getAttempts());
    a.setMaxRetries(job.getMaxRetries());
    a.setBackoffPolicy(job.getBackoffPolicy());
    a.setBackoffParamMs(job.getBackoffParamMs());
    a.setTimeoutSec(job.getTimeoutSec());
    a.setTargetClass(job.getTargetClass());
    a.setMethodName(job.getMethodName());
    a.setBusinessKey(job.getBusinessKey());
    a.setCronExpr(job.getCronExpr());
    a.setZoneId(job.getZoneId());
    a.setOriginalScheduledTime(job.getScheduledTime());
    a.setOriginalCreatedAt(job.getCreatedAt());
    a.setFirstExecutionTime(job.getExecutionStartTime());
    a.setCompletionTime(job.getExecutionEndTime());
    a.setTotalExecutionTimeMs(job.getExecutionDurationMs());
    a.setQueueWaitMs(job.getQueueWaitMs());
    a.setArchivedAt(Instant.now(clock));
    a.setArchivedBy(archivedBy);
    a.setArchiveReason(reason);
    a.setJobResult(job.getJobResult());
    a.setResultType(job.getResultType());
    a.setFinalError(job.getLastError());
    if (job.getPayload() != null) {
      a.setPayloadSummary(job.getPayload().target() + "#" + job.getPayload().method());
    }
    a.setDependedOn(job.getDependsOn());
    a.setSupersededBy(job.getSupersededBy());
    if (job.getTags() != null && !job.getTags().isEmpty()) {
      a.setTags(String.join(",", job.getTags()));
    }
    return a;
  }

  /**
   * Copies the job's extension properties and extension-state docs onto the archive entity as the
   * denormalized JSON the archive row carries (state blobs stay as stored — ciphertext when
   * encrypted at rest).
   */
  private void populateExtensionData(ArchivedJobEntity archive, UUID jobId) {
    Map<String, String> properties = new LinkedHashMap<>();
    for (Document doc :
        ctx.jobProperties().find(eq(JOB_ID, jobId)).sort(new Document(PROPERTY_KEY, 1))) {
      String value = doc.getString(VALUE);
      if (value != null) {
        properties.put(doc.getString(PROPERTY_KEY), value);
      }
    }

    List<ExtensionArchiveJson.StateRow> states = new ArrayList<>();
    for (Document doc : ctx.jobExtensionState().find(eq(JOB_ID, jobId))) {
      states.add(
          new ExtensionArchiveJson.StateRow(
              doc.getString(NAMESPACE),
              doc.getString(STATE),
              doc.getBoolean(ENCRYPTED_STATE, false),
              doc.getString(ENCRYPTION_KEY_ID),
              doc.getInteger(VERSION, 0),
              DocumentMapper.toInstant(doc.getDate(UPDATED_AT))));
    }
    populateExtensionData(archive, properties, states);
  }

  private Map<UUID, Map<String, String>> extensionProperties(
      ClientSession session, List<UUID> jobIds) {
    Map<UUID, Map<String, String>> propertiesByJobId = new LinkedHashMap<>();
    for (Document doc :
        ctx.jobProperties().find(session, in(JOB_ID, jobIds)).sort(new Document(PROPERTY_KEY, 1))) {
      String value = doc.getString(VALUE);
      if (value != null) {
        UUID jobId = doc.get(JOB_ID, UUID.class);
        propertiesByJobId
            .computeIfAbsent(jobId, ignored -> new LinkedHashMap<>())
            .put(doc.getString(PROPERTY_KEY), value);
      }
    }
    return propertiesByJobId;
  }

  private Map<UUID, List<ExtensionArchiveJson.StateRow>> extensionStates(
      ClientSession session, List<UUID> jobIds) {
    Map<UUID, List<ExtensionArchiveJson.StateRow>> statesByJobId = new LinkedHashMap<>();
    for (Document doc : ctx.jobExtensionState().find(session, in(JOB_ID, jobIds))) {
      UUID jobId = doc.get(JOB_ID, UUID.class);
      statesByJobId
          .computeIfAbsent(jobId, ignored -> new ArrayList<>())
          .add(
              new ExtensionArchiveJson.StateRow(
                  doc.getString(NAMESPACE),
                  doc.getString(STATE),
                  doc.getBoolean(ENCRYPTED_STATE, false),
                  doc.getString(ENCRYPTION_KEY_ID),
                  doc.getInteger(VERSION, 0),
                  DocumentMapper.toInstant(doc.getDate(UPDATED_AT))));
    }
    return statesByJobId;
  }

  private void populateExtensionData(
      ArchivedJobEntity archive,
      Map<String, String> properties,
      List<ExtensionArchiveJson.StateRow> states) {
    archive.setProperties(ExtensionArchiveJson.propertiesJson(properties));
    archive.setExtensionState(ExtensionArchiveJson.extensionStateJson(states));
  }

  /**
   * Removes the hot extension docs for archived jobs in the archive session — the Mongo equivalent
   * of the SQL FK CASCADE that fires when the hot job row is deleted after archiving.
   */
  private void deleteExtensionData(ClientSession session, List<UUID> jobIds) {
    ctx.jobProperties().deleteMany(session, in(JOB_ID, jobIds));
    ctx.jobExtensionState().deleteMany(session, in(JOB_ID, jobIds));
  }
}
