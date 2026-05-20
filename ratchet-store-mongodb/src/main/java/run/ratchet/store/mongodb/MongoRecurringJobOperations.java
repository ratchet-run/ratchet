package run.ratchet.store.mongodb;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Filters.lt;
import static com.mongodb.client.model.Filters.lte;
import static com.mongodb.client.model.Filters.ne;
import static com.mongodb.client.model.Filters.nin;
import static com.mongodb.client.model.Filters.nor;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.set;
import static run.ratchet.store.mongodb.MongoFieldNames.ARCHIVED_AT;
import static run.ratchet.store.mongodb.MongoFieldNames.ARCHIVE_REASON;
import static run.ratchet.store.mongodb.MongoFieldNames.BACKOFF_PARAM_MS;
import static run.ratchet.store.mongodb.MongoFieldNames.BACKOFF_POLICY;
import static run.ratchet.store.mongodb.MongoFieldNames.BUSINESS_KEY;
import static run.ratchet.store.mongodb.MongoFieldNames.CALLER_PRINCIPAL;
import static run.ratchet.store.mongodb.MongoFieldNames.CREATED_AT;
import static run.ratchet.store.mongodb.MongoFieldNames.CRON_EXPR;
import static run.ratchet.store.mongodb.MongoFieldNames.ID;
import static run.ratchet.store.mongodb.MongoFieldNames.IS_PAUSED;
import static run.ratchet.store.mongodb.MongoFieldNames.MAX_RETRIES_FIELD;
import static run.ratchet.store.mongodb.MongoFieldNames.NEXT_FIRE;
import static run.ratchet.store.mongodb.MongoFieldNames.ON_FAILURE_PAYLOAD;
import static run.ratchet.store.mongodb.MongoFieldNames.ON_SUCCESS_PAYLOAD;
import static run.ratchet.store.mongodb.MongoFieldNames.PARAMS;
import static run.ratchet.store.mongodb.MongoFieldNames.PAUSED_AT;
import static run.ratchet.store.mongodb.MongoFieldNames.PAYLOAD;
import static run.ratchet.store.mongodb.MongoFieldNames.PRIORITY_FIELD;
import static run.ratchet.store.mongodb.MongoFieldNames.RESOURCE_NAME;
import static run.ratchet.store.mongodb.MongoFieldNames.TAGS;
import static run.ratchet.store.mongodb.MongoFieldNames.TIMEOUT_SEC;
import static run.ratchet.store.mongodb.MongoFieldNames.ZONE_ID;

import com.mongodb.client.FindIterable;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.bson.Document;
import org.bson.conversions.Bson;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.NodeTagFilter;
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.store.spi.RecurringJobDefinition;
import run.ratchet.store.spi.RecurringJobStore;

/**
 * MongoDB implementation of {@link RecurringJobStore} over the dedicated {@code
 * scheduler_recurring_job} and {@code scheduler_recurring_job_archive} collections (CP2 split).
 *
 * <p>Single-document atomicity replaces {@code FOR UPDATE SKIP LOCKED}: claim and advance use
 * {@code findOneAndUpdate} per row. Cancel + archive uses a transaction when the cluster supports
 * one; otherwise relies on Mongo's at-most-one-document atomic guarantees.
 */
final class MongoRecurringJobOperations implements RecurringJobStore {

  private final MongoStoreContext ctx;

  MongoRecurringJobOperations(MongoStoreContext ctx) {
    this.ctx = ctx;
  }

  @Override
  public List<RecurringJobDefinition> claimDueRecurring(
      int limit, String nodeId, NodeTagFilter tagFilter) {
    if (limit <= 0) {
      return List.of();
    }
    Date now = Date.from(Instant.now());
    List<Bson> clauses = new ArrayList<>();
    clauses.add(eq(IS_PAUSED, false));
    clauses.add(lte(NEXT_FIRE, now));
    if (tagFilter != null && !tagFilter.isUnfiltered()) {
      if (!tagFilter.requireTags().isEmpty()) {
        clauses.add(in(TAGS, tagFilter.requireTags()));
      }
      if (!tagFilter.excludeTags().isEmpty()) {
        // nor(in(TAGS, excludes)) matches docs whose tags array contains none of the excluded tags,
        // and also matches docs that have no tags field at all — same semantics as the SQL
        // NOT EXISTS guard against scheduler_job_tag.
        clauses.add(nor(in(TAGS, tagFilter.excludeTags())));
      }
    }
    Bson filter = and(clauses);
    FindIterable<Document> iter =
        ctx.recurringJobs()
            .find(filter)
            .sort(new Document(PRIORITY_FIELD, -1).append(NEXT_FIRE, 1).append(ID, 1))
            .limit(limit);
    List<RecurringJobDefinition> defs = new ArrayList<>();
    for (Document doc : iter) {
      defs.add(hydrate(doc));
    }
    return defs;
  }

  @Override
  public void advanceNextFire(UUID id, Instant nextFire) {
    ctx.recurringJobs().updateOne(eq(ID, id), set(NEXT_FIRE, Date.from(nextFire)));
  }

  @Override
  public Optional<Instant> findEarliestRecurringNextFire() {
    Document doc =
        ctx.recurringJobs()
            .find(eq(IS_PAUSED, false))
            .sort(new Document(NEXT_FIRE, 1))
            .limit(1)
            .first();
    if (doc == null) {
      return Optional.empty();
    }
    Date d = doc.getDate(NEXT_FIRE);
    return d == null ? Optional.empty() : Optional.of(d.toInstant());
  }

  @Override
  public boolean pauseRecurring(UUID id) {
    UpdateResult r =
        ctx.recurringJobs()
            .updateOne(
                and(eq(ID, id), eq(IS_PAUSED, false)),
                combine(set(IS_PAUSED, true), set(PAUSED_AT, new Date())));
    return r.getModifiedCount() > 0;
  }

  @Override
  public boolean resumeRecurring(UUID id) {
    UpdateResult r =
        ctx.recurringJobs()
            .updateOne(
                and(eq(ID, id), eq(IS_PAUSED, true)),
                combine(set(IS_PAUSED, false), set(PAUSED_AT, null)));
    return r.getModifiedCount() > 0;
  }

  @Override
  public boolean cancelRecurringAndArchive(UUID id, ArchiveReason reason) {
    Document doc = ctx.recurringJobs().find(eq(ID, id)).first();
    if (doc == null) {
      return false;
    }
    archive(doc, reason);
    // Cancel any bkres for this recurring master.
    ctx.jobs(); // touch — not needed; bkres collection lives elsewhere
    ctx.recurringJobs().deleteOne(eq(ID, id));
    return true;
  }

  @Override
  public int cancelOrphanedRecurringAnnotationJobs(
      Set<String> knownBusinessKeys, Instant nodeStartTime) {
    Bson filter =
        and(
            ne(BUSINESS_KEY, null),
            lt(CREATED_AT, Date.from(nodeStartTime)),
            knownBusinessKeys.isEmpty()
                ? eq("_no_match_", false)
                : nin(BUSINESS_KEY, knownBusinessKeys));
    // Mongo's nin is awkward when the set is empty; the eq("_no_match_", false) above is a safe
    // tautology — there's nothing to NOT-IN against, so cancel nothing.
    if (knownBusinessKeys.isEmpty()) {
      // Cancel every annotation-owned recurring whose business_key is non-null and old.
      filter = and(ne(BUSINESS_KEY, null), lt(CREATED_AT, Date.from(nodeStartTime)));
    }
    return cancelMatching(filter);
  }

  @Override
  public int cancelRecurringJobsByTag(String tag) {
    return cancelMatching(eq("tags", tag));
  }

  @Override
  public boolean cancelRecurringJobByBusinessKey(String businessKey) {
    return cancelMatching(eq(BUSINESS_KEY, businessKey)) > 0;
  }

  @Override
  public int cancelRecurringJobsByBusinessKeys(Set<String> businessKeys) {
    if (businessKeys.isEmpty()) {
      return 0;
    }
    return cancelMatching(in(BUSINESS_KEY, businessKeys));
  }

  @Override
  public UUID createRecurring(RecurringJobDefinition d) {
    Document doc = toDocument(d);
    try {
      ctx.recurringJobs().insertOne(doc);
    } catch (RuntimeException e) {
      if (ctx.constraintDetector().isDuplicateBusinessKey(e)) {
        throw new RatchetTransientStoreException(
            "Active business key in use for recurring master " + d.id(), e);
      }
      throw ctx.translateTransientStoreException("create recurring", e);
    }
    return d.id();
  }

  @Override
  public boolean updateRecurring(UUID id, RecurringJobDefinition d) {
    UpdateResult r =
        ctx.recurringJobs()
            .updateOne(
                eq(ID, id),
                combine(
                    set(PRIORITY_FIELD, d.priority()),
                    set(MAX_RETRIES_FIELD, d.maxRetries()),
                    set(
                        BACKOFF_POLICY,
                        d.backoffPolicy() != null ? d.backoffPolicy().name() : "NONE"),
                    set(BACKOFF_PARAM_MS, d.backoffParamMs()),
                    set(TIMEOUT_SEC, d.timeoutSec()),
                    set(CRON_EXPR, d.cronExpr()),
                    set(ZONE_ID, d.zoneId() != null ? d.zoneId() : "UTC"),
                    set(NEXT_FIRE, Date.from(d.nextFire())),
                    set(PAYLOAD, DocumentMapper.payloadToStoredValue(d.payload())),
                    set(PARAMS, DocumentMapper.payloadToStoredValue(d.params())),
                    set(
                        ON_SUCCESS_PAYLOAD,
                        DocumentMapper.payloadToStoredValue(d.onSuccessPayload())),
                    set(
                        ON_FAILURE_PAYLOAD,
                        DocumentMapper.payloadToStoredValue(d.onFailurePayload())),
                    set(RESOURCE_NAME, d.resourceName())),
                new UpdateOptions().upsert(false));
    return r.getModifiedCount() > 0;
  }

  @Override
  public Optional<RecurringJobDefinition> getRecurring(UUID id) {
    Document doc = ctx.recurringJobs().find(eq(ID, id)).first();
    return doc == null ? Optional.empty() : Optional.of(hydrate(doc));
  }

  @Override
  public Optional<RecurringJobDefinition> findRecurringByBusinessKey(String businessKey) {
    Document doc = ctx.recurringJobs().find(eq(BUSINESS_KEY, businessKey)).limit(1).first();
    return doc == null ? Optional.empty() : Optional.of(hydrate(doc));
  }

  @Override
  public List<RecurringJobDefinition> listAll() {
    List<RecurringJobDefinition> out = new ArrayList<>();
    for (Document doc : ctx.recurringJobs().find()) {
      out.add(hydrate(doc));
    }
    return out;
  }

  private int cancelMatching(Bson filter) {
    List<Document> docs = new ArrayList<>();
    for (Document doc : ctx.recurringJobs().find(filter)) {
      docs.add(doc);
    }
    if (docs.isEmpty()) {
      return 0;
    }
    int n = 0;
    List<UUID> ids = new ArrayList<>(docs.size());
    for (Document doc : docs) {
      archive(doc, ArchiveReason.CANCELED);
      UUID id = doc.get(ID, UUID.class);
      if (id != null) {
        ids.add(id);
      }
      n++;
    }
    if (!ids.isEmpty()) {
      ctx.recurringJobs().deleteMany(in(ID, ids));
    }
    return n;
  }

  private void archive(Document live, ArchiveReason reason) {
    Document archive = new Document();
    archive.put(ID, live.get(ID));
    archive.put(CRON_EXPR, live.get(CRON_EXPR));
    archive.put(ZONE_ID, live.get(ZONE_ID));
    archive.put(PAYLOAD, live.get(PAYLOAD));
    archive.put(PARAMS, live.get(PARAMS));
    archive.put(ON_SUCCESS_PAYLOAD, live.get(ON_SUCCESS_PAYLOAD));
    archive.put(ON_FAILURE_PAYLOAD, live.get(ON_FAILURE_PAYLOAD));
    archive.put(BUSINESS_KEY, live.get(BUSINESS_KEY));
    archive.put(CREATED_AT, live.get(CREATED_AT));
    archive.put(CALLER_PRINCIPAL, live.get(CALLER_PRINCIPAL));
    archive.put(ARCHIVED_AT, new Date());
    archive.put(ARCHIVE_REASON, reason.name());
    ctx.recurringJobArchive().insertOne(archive);
  }

  private static Document toDocument(RecurringJobDefinition d) {
    Document doc = new Document();
    doc.put(ID, d.id());
    doc.put(PRIORITY_FIELD, d.priority());
    doc.put(MAX_RETRIES_FIELD, d.maxRetries());
    doc.put(BACKOFF_POLICY, d.backoffPolicy() != null ? d.backoffPolicy().name() : "NONE");
    doc.put(BACKOFF_PARAM_MS, d.backoffParamMs());
    doc.put(TIMEOUT_SEC, d.timeoutSec());
    doc.put(CRON_EXPR, d.cronExpr());
    doc.put(ZONE_ID, d.zoneId() != null ? d.zoneId() : "UTC");
    doc.put(NEXT_FIRE, Date.from(d.nextFire()));
    doc.put(IS_PAUSED, d.paused());
    doc.put(PAUSED_AT, d.pausedAt() != null ? Date.from(d.pausedAt()) : null);
    doc.put(PAYLOAD, DocumentMapper.payloadToStoredValue(d.payload()));
    doc.put(PARAMS, DocumentMapper.payloadToStoredValue(d.params()));
    doc.put(ON_SUCCESS_PAYLOAD, DocumentMapper.payloadToStoredValue(d.onSuccessPayload()));
    doc.put(ON_FAILURE_PAYLOAD, DocumentMapper.payloadToStoredValue(d.onFailurePayload()));
    doc.put(BUSINESS_KEY, d.businessKey());
    doc.put(RESOURCE_NAME, d.resourceName());
    doc.put(CREATED_AT, Date.from(d.createdAt() != null ? d.createdAt() : Instant.now()));
    doc.put(CALLER_PRINCIPAL, d.callerPrincipal());
    return doc;
  }

  private static RecurringJobDefinition hydrate(Document doc) {
    UUID id = doc.get(ID, UUID.class);
    Number priority = (Number) doc.get(PRIORITY_FIELD);
    Number maxRetries = (Number) doc.get(MAX_RETRIES_FIELD);
    BackoffPolicy backoffPolicy =
        BackoffPolicy.valueOf(
            doc.getString(BACKOFF_POLICY) != null ? doc.getString(BACKOFF_POLICY) : "NONE");
    Number backoffParamMs = (Number) doc.get(BACKOFF_PARAM_MS);
    Number timeoutSec = (Number) doc.get(TIMEOUT_SEC);
    Instant nextFire = doc.getDate(NEXT_FIRE) != null ? doc.getDate(NEXT_FIRE).toInstant() : null;
    boolean isPaused = doc.getBoolean(IS_PAUSED, false);
    Instant pausedAt = doc.getDate(PAUSED_AT) != null ? doc.getDate(PAUSED_AT).toInstant() : null;
    JobPayload payload = DocumentMapper.storedValueToPayload(doc.get(PAYLOAD));
    JobPayload params = DocumentMapper.storedValueToPayload(doc.get(PARAMS));
    JobPayload onSuccess = DocumentMapper.storedValueToPayload(doc.get(ON_SUCCESS_PAYLOAD));
    JobPayload onFailure = DocumentMapper.storedValueToPayload(doc.get(ON_FAILURE_PAYLOAD));
    Instant createdAt =
        doc.getDate(CREATED_AT) != null ? doc.getDate(CREATED_AT).toInstant() : null;

    return new RecurringJobDefinition(
        id,
        doc.getString(CRON_EXPR),
        doc.getString(ZONE_ID),
        nextFire,
        isPaused,
        pausedAt,
        priority != null ? priority.intValue() : 2,
        maxRetries != null ? maxRetries.intValue() : 0,
        backoffPolicy,
        backoffParamMs != null ? backoffParamMs.intValue() : 0,
        timeoutSec != null ? timeoutSec.intValue() : 0,
        payload,
        params,
        onSuccess,
        onFailure,
        doc.getString(BUSINESS_KEY),
        doc.getString(RESOURCE_NAME),
        createdAt,
        doc.getString(CALLER_PRINCIPAL));
  }
}
