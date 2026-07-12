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
import static run.ratchet.store.mongodb.MongoFieldNames.CLAIM_EXPIRES_AT;
import static run.ratchet.store.mongodb.MongoFieldNames.CLAIM_TOKEN;
import static run.ratchet.store.mongodb.MongoFieldNames.CREATED_AT;
import static run.ratchet.store.mongodb.MongoFieldNames.CRON_EXPR;
import static run.ratchet.store.mongodb.MongoFieldNames.EXECUTION_TARGET;
import static run.ratchet.store.mongodb.MongoFieldNames.ID;
import static run.ratchet.store.mongodb.MongoFieldNames.IS_PAUSED;
import static run.ratchet.store.mongodb.MongoFieldNames.MAX_CATCH_UP_EXECUTIONS;
import static run.ratchet.store.mongodb.MongoFieldNames.MAX_RETRIES_FIELD;
import static run.ratchet.store.mongodb.MongoFieldNames.MISFIRE_POLICY;
import static run.ratchet.store.mongodb.MongoFieldNames.NEXT_FIRE;
import static run.ratchet.store.mongodb.MongoFieldNames.ON_FAILURE_PAYLOAD;
import static run.ratchet.store.mongodb.MongoFieldNames.ON_SUCCESS_PAYLOAD;
import static run.ratchet.store.mongodb.MongoFieldNames.PAUSED_AT;
import static run.ratchet.store.mongodb.MongoFieldNames.PAYLOAD;
import static run.ratchet.store.mongodb.MongoFieldNames.PRIORITY_FIELD;
import static run.ratchet.store.mongodb.MongoFieldNames.RESOURCE_NAME;
import static run.ratchet.store.mongodb.MongoFieldNames.TAGS;
import static run.ratchet.store.mongodb.MongoFieldNames.TIMEOUT_SEC;
import static run.ratchet.store.mongodb.MongoFieldNames.ZONE_ID;

import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
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
import run.ratchet.api.NodeTagFilter;
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.spi.ProtectedSurface;
import run.ratchet.store.spi.RecurringJobDefinition;
import run.ratchet.store.spi.RecurringJobStore;
import run.ratchet.store.util.JobEncryption;

/**
 * MongoDB implementation of {@link RecurringJobStore} over the dedicated {@code
 * scheduler_recurring_job} and {@code scheduler_recurring_job_archive} collections.
 *
 * <p>Single-document atomicity replaces {@code FOR UPDATE SKIP LOCKED}: claim and advance use
 * {@code findOneAndUpdate} per row. Cancel + archive uses a transaction when the cluster supports
 * one; otherwise relies on Mongo's at-most-one-document atomic guarantees.
 */
final class MongoRecurringJobOperations implements RecurringJobStore {

  // Window during which a claimed row is invisible to other claimers. The worker is expected to
  // call advanceNextFire (which clears the claim token) or releaseClaim before the lease expires;
  // if the worker crashes mid-process, the lease times out and the row becomes claimable again
  // automatically. 5 minutes matches the upper bound of a typical executor tick + child enqueue.
  private static final long CLAIM_LEASE_SECONDS = 300L;

  // Sentinel for unclaimed rows: claim_expires_at = epoch is always strictly less than now, so
  // the claim filter (lte(claim_expires_at, now)) trivially matches. Sticking with a date
  // sentinel avoids the null-vs-missing-field comparison quirks that bit the first cut of this
  // implementation.
  private static final Date UNCLAIMED = new Date(0L);

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
    Instant now = Instant.now();
    Date nowDate = Date.from(now);
    Date leaseUntil = Date.from(now.plusSeconds(CLAIM_LEASE_SECONDS));
    UUID claimToken = UUID.randomUUID();
    List<Bson> clauses = new ArrayList<>();
    clauses.add(eq(IS_PAUSED, false));
    clauses.add(lte(NEXT_FIRE, nowDate));
    // Unclaimed or lease expired: claim_expires_at is the epoch sentinel (set on create / release
    // / advance) or a past timestamp (lease aged out after a crash).
    clauses.add(lte(CLAIM_EXPIRES_AT, nowDate));
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
    Bson sort = new Document(PRIORITY_FIELD, -1).append(NEXT_FIRE, 1).append(ID, 1);
    Bson lease = combine(set(CLAIM_TOKEN, claimToken), set(CLAIM_EXPIRES_AT, leaseUntil));
    FindOneAndUpdateOptions options =
        new FindOneAndUpdateOptions().sort(sort).returnDocument(ReturnDocument.BEFORE);

    // Per-row findOneAndUpdate replaces find()+iterate so two nodes can never observe the same
    // master in the same window. The update stamps a claim_token + claim_expires_at lease on the
    // row, hiding it from peers. The worker calls advanceNextFire (which clears the lease and
    // sets the real next_fire) or releaseClaim (which clears the lease without changing
    // next_fire). If the worker crashes the lease expires naturally after CLAIM_LEASE_SECONDS.
    List<RecurringJobDefinition> defs = new ArrayList<>();
    for (int i = 0; i < limit; i++) {
      Document before = ctx.recurringJobs().findOneAndUpdate(filter, lease, options);
      if (before == null) {
        break;
      }
      defs.add(hydrate(before));
    }
    return defs;
  }

  @Override
  public void advanceNextFire(UUID id, Instant nextFire) {
    // Advance also clears the claim lease so peers can see the row at its new next_fire.
    ctx.recurringJobs()
        .updateOne(
            eq(ID, id),
            combine(
                set(NEXT_FIRE, Date.from(nextFire)),
                set(CLAIM_TOKEN, null),
                set(CLAIM_EXPIRES_AT, UNCLAIMED)));
  }

  @Override
  public void releaseClaim(UUID id) {
    ctx.recurringJobs()
        .updateOne(eq(ID, id), combine(set(CLAIM_TOKEN, null), set(CLAIM_EXPIRES_AT, UNCLAIMED)));
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
    // Wrap archive + live-delete in a Mongo transaction so concurrent cancels can't double-
    // archive and a mid-flight crash can't leave the archive without its live counterpart.
    // Requires a replica set or sharded cluster (standalone mongod does not support sessions);
    // production deployments must use one.
    try (com.mongodb.client.ClientSession session = ctx.startSession()) {
      return session.withTransaction(
          () -> {
            Document doc = ctx.recurringJobs().find(session, eq(ID, id)).first();
            if (doc == null) {
              return Boolean.FALSE;
            }
            archive(session, doc, reason);
            ctx.recurringJobs().deleteOne(session, eq(ID, id));
            return Boolean.TRUE;
          });
    }
  }

  @Override
  public int cancelOrphanedRecurringAnnotationJobs(
      Set<String> knownBusinessKeys, Instant nodeStartTime) {
    Bson base = and(ne(BUSINESS_KEY, null), lt(CREATED_AT, Date.from(nodeStartTime)));
    Bson filter =
        knownBusinessKeys.isEmpty() ? base : and(base, nin(BUSINESS_KEY, knownBusinessKeys));
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
    boolean active = JobEncryption.activeFor(d.encryptedPayload());
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
                    set(
                        PAYLOAD,
                        DocumentMapper.encryptedRecurringColumn(
                            d.payload(), active, ProtectedSurface.PAYLOAD_ARGS, id)),
                    set(
                        ON_SUCCESS_PAYLOAD,
                        DocumentMapper.encryptedRecurringColumn(
                            d.onSuccessPayload(), active, ProtectedSurface.ON_SUCCESS_PAYLOAD, id)),
                    set(
                        ON_FAILURE_PAYLOAD,
                        DocumentMapper.encryptedRecurringColumn(
                            d.onFailurePayload(), active, ProtectedSurface.ON_FAILURE_PAYLOAD, id)),
                    set(RESOURCE_NAME, d.resourceName()),
                    set(EXECUTION_TARGET, d.executionTarget()),
                    set(MISFIRE_POLICY, d.misfirePolicy().action().name()),
                    set(MAX_CATCH_UP_EXECUTIONS, d.misfirePolicy().maxCatchUpExecutions()),
                    set("encrypted_payload", active)),
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
    // Bulk cancel must satisfy the same atomicity contract as the single-id path: archive
    // every matched doc and delete every matched doc, or neither, with no possibility of a
    // partial commit. Wrap both writes in a Mongo transaction.
    try (com.mongodb.client.ClientSession session = ctx.startSession()) {
      Integer result =
          session.withTransaction(
              () -> {
                List<Document> docs = new ArrayList<>();
                for (Document doc : ctx.recurringJobs().find(session, filter)) {
                  docs.add(doc);
                }
                if (docs.isEmpty()) {
                  return 0;
                }
                List<UUID> ids = new ArrayList<>(docs.size());
                for (Document doc : docs) {
                  archive(session, doc, ArchiveReason.CANCELED);
                  UUID id = doc.get(ID, UUID.class);
                  if (id != null) {
                    ids.add(id);
                  }
                }
                if (!ids.isEmpty()) {
                  ctx.recurringJobs().deleteMany(session, in(ID, ids));
                }
                return docs.size();
              });
      return result;
    }
  }

  private void archive(
      com.mongodb.client.ClientSession session, Document live, ArchiveReason reason) {
    ctx.recurringJobArchive().insertOne(session, archiveSnapshot(live, reason));
  }

  private Document archiveSnapshot(Document live, ArchiveReason reason) {
    Document archive = new Document();
    archive.put(ID, live.get(ID));
    archive.put(CRON_EXPR, live.get(CRON_EXPR));
    archive.put(ZONE_ID, live.get(ZONE_ID));
    archive.put(PAYLOAD, live.get(PAYLOAD));
    archive.put(ON_SUCCESS_PAYLOAD, live.get(ON_SUCCESS_PAYLOAD));
    archive.put(ON_FAILURE_PAYLOAD, live.get(ON_FAILURE_PAYLOAD));
    archive.put(BUSINESS_KEY, live.get(BUSINESS_KEY));
    archive.put(EXECUTION_TARGET, live.get(EXECUTION_TARGET));
    archive.put(CREATED_AT, live.get(CREATED_AT));
    archive.put(CALLER_PRINCIPAL, live.get(CALLER_PRINCIPAL));
    archive.put(ARCHIVED_AT, new Date());
    archive.put(ARCHIVE_REASON, reason.name());
    return archive;
  }

  private static Document toDocument(RecurringJobDefinition d) {
    Document doc = DocumentMapper.toRecurringDocument(d);
    doc.put(CLAIM_TOKEN, null);
    doc.put(CLAIM_EXPIRES_AT, UNCLAIMED);
    return doc;
  }

  private static RecurringJobDefinition hydrate(Document doc) {
    return DocumentMapper.toRecurringJobDefinition(doc);
  }
}
