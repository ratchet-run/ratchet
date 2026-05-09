package run.ratchet.store.mongodb;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Filters.lt;
import static com.mongodb.client.model.Filters.lte;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.descending;
import static run.ratchet.store.mongodb.MongoFieldNames.ARCHIVED_AT;
import static run.ratchet.store.mongodb.MongoFieldNames.BUSINESS_KEY;
import static run.ratchet.store.mongodb.MongoFieldNames.STATUS;
import static run.ratchet.store.mongodb.MongoFieldNames.TARGET_CLASS;
import static run.ratchet.store.mongodb.MongoFieldNames.UPDATED_AT;

import com.mongodb.client.result.DeleteResult;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.bson.Document;
import org.bson.conversions.Bson;
import run.ratchet.store.entity.ArchivedJobEntity;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.id.UuidV7Factory;

/**
 * Archive operations over {@code scheduler_job_archive}. Terminal-state jobs matching the retention
 * cutoff are projected into {@link ArchivedJobEntity} documents — original job rows are left alone;
 * the caller is responsible for subsequent deletion via {@code deleteJobsByIds}.
 */
final class MongoArchiveOperations {

  private final MongoStoreContext ctx;
  private final Clock clock;

  MongoArchiveOperations(MongoStoreContext ctx) {
    this(ctx, Clock.systemUTC());
  }

  MongoArchiveOperations(MongoStoreContext ctx, Clock clock) {
    this.ctx = ctx;
    this.clock = Objects.requireNonNull(clock, "clock");
  }

  ArchivedJobEntity archiveJob(JobEntity job, String reason, String archivedBy) {
    ArchivedJobEntity archive = buildArchive(job, reason, archivedBy);
    archive.setId(UuidV7Factory.create());
    ctx.archives().insertOne(DocumentMapper.toDocument(archive));
    return archive;
  }

  int archiveJobsBatch(List<JobEntity> jobList, String reason, String archivedBy) {
    if (jobList.isEmpty()) {
      return 0;
    }
    List<Document> docs = new ArrayList<>(jobList.size());
    for (JobEntity job : jobList) {
      ArchivedJobEntity archive = buildArchive(job, reason, archivedBy);
      archive.setId(UuidV7Factory.create());
      docs.add(DocumentMapper.toDocument(archive));
    }
    ctx.archives().insertMany(docs);
    return docs.size();
  }

  List<JobEntity> findJobsForArchiving(Instant olderThan, int limit) {
    List<JobEntity> results = new ArrayList<>();
    for (Document doc :
        ctx.jobs()
            .find(
                and(
                    in(STATUS, MongoStoreContext.TERMINAL_STATUSES),
                    lt(UPDATED_AT, DocumentMapper.toDate(olderThan))))
            .sort(ascending(UPDATED_AT))
            .limit(limit)) {
      results.add(DocumentMapper.toJobEntity(doc));
    }
    return results;
  }

  long countJobsForArchiving(Instant olderThan) {
    return ctx.jobs()
        .countDocuments(
            and(
                in(STATUS, MongoStoreContext.TERMINAL_STATUSES),
                lt(UPDATED_AT, DocumentMapper.toDate(olderThan))));
  }

  List<ArchivedJobEntity> findArchivedJobs(
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

  int purgeArchivedJobs(Instant olderThan) {
    DeleteResult result =
        ctx.archives().deleteMany(lt(ARCHIVED_AT, DocumentMapper.toDate(olderThan)));
    return (int) result.getDeletedCount();
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
}
