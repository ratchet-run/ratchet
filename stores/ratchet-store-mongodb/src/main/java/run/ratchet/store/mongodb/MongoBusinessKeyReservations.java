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
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Filters.lt;
import static run.ratchet.store.mongodb.MongoFieldNames.BUSINESS_KEY;
import static run.ratchet.store.mongodb.MongoFieldNames.CLAIM_SEQ;
import static run.ratchet.store.mongodb.MongoFieldNames.HELD_BY_NODE;
import static run.ratchet.store.mongodb.MongoFieldNames.HELD_CLAIM_SEQ;
import static run.ratchet.store.mongodb.MongoFieldNames.HELD_UNTIL;
import static run.ratchet.store.mongodb.MongoFieldNames.ID;
import static run.ratchet.store.mongodb.MongoFieldNames.JOB_TYPE;
import static run.ratchet.store.mongodb.MongoFieldNames.OWNER_JOB_ID;
import static run.ratchet.store.mongodb.MongoFieldNames.OWNER_TABLE;
import static run.ratchet.store.mongodb.MongoFieldNames.PICKED_BY;
import static run.ratchet.store.mongodb.MongoFieldNames.RESERVED_AT;

import com.mongodb.client.ClientSession;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import org.bson.Document;
import run.ratchet.api.JobStatus;
import run.ratchet.store.BusinessKeyReservationConflictException;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.util.BusinessKeyReservations;
import run.ratchet.store.util.StatusClassifier;

/**
 * Shared active business-key ownership for queue jobs and recurring masters.
 *
 * <p>The business key is the reservation document's {@code _id}, so MongoDB's built-in unique index
 * is the single serialization point across both owner collections. Every acquire/release is called
 * inside the same {@link ClientSession} transaction as the owner mutation.
 */
final class MongoBusinessKeyReservations {

  private final MongoStoreContext ctx;

  MongoBusinessKeyReservations(MongoStoreContext ctx) {
    this.ctx = ctx;
  }

  void syncForJob(ClientSession session, JobEntity job) {
    JobStatus status = StatusClassifier.effectiveStatus(job.getStatus());
    if (status == JobStatus.CANCELED) {
      ctx.businessKeyReservations()
          .deleteOne(session, and(eq(OWNER_JOB_ID, job.getId()), exists(HELD_UNTIL, false)));
      return;
    }
    releaseByOwner(session, job.getId());
    if (StatusClassifier.isLiveStatus(status) && job.getBusinessKey() != null) {
      reserve(
          session,
          job.getBusinessKey(),
          job.getId(),
          BusinessKeyReservations.ownerTableFor(job.getJobType()));
    }
  }

  void syncForStoredJob(ClientSession session, UUID ownerJobId, JobStatus status) {
    syncForStoredJob(session, ownerJobId, status, null, null);
  }

  void syncForStoredJob(
      ClientSession session,
      UUID ownerJobId,
      JobStatus status,
      Instant keyHeldUntil,
      Document claim) {
    if (keyHeldUntil != null) {
      ctx.businessKeyReservations()
          .updateOne(
              session,
              eq(OWNER_JOB_ID, ownerJobId),
              new Document(
                  "$set",
                  new Document(HELD_UNTIL, Date.from(keyHeldUntil))
                      .append(HELD_BY_NODE, claim.getString(PICKED_BY))
                      .append(
                          HELD_CLAIM_SEQ,
                          ((Number) claim.getOrDefault(CLAIM_SEQ, 0L)).longValue())));
      return;
    }
    releaseByOwner(session, ownerJobId);
    if (!StatusClassifier.isLiveStatus(status)) {
      return;
    }
    Document owner =
        ctx.jobs()
            .find(session, eq(ID, ownerJobId))
            .projection(new Document(BUSINESS_KEY, 1).append(JOB_TYPE, 1))
            .first();
    if (owner == null) {
      return;
    }
    String businessKey = owner.getString(BUSINESS_KEY);
    if (businessKey != null) {
      reserve(
          session,
          businessKey,
          ownerJobId,
          BusinessKeyReservations.ownerTableFor(owner.getString(JOB_TYPE)));
    }
  }

  void reserveRecurring(ClientSession session, String businessKey, UUID ownerJobId) {
    if (businessKey != null) {
      reserve(session, businessKey, ownerJobId, BusinessKeyReservations.OWNER_TABLE_RECURRING);
    }
  }

  void releaseByOwner(ClientSession session, UUID ownerJobId) {
    ctx.businessKeyReservations().deleteOne(session, eq(OWNER_JOB_ID, ownerJobId));
  }

  void releaseByOwners(ClientSession session, List<UUID> ownerJobIds) {
    if (!ownerJobIds.isEmpty()) {
      ctx.businessKeyReservations().deleteMany(session, in(OWNER_JOB_ID, ownerJobIds));
    }
  }

  private void reserve(
      ClientSession session, String businessKey, UUID ownerJobId, String ownerTable) {
    Document reservation =
        new Document(ID, businessKey)
            .append(OWNER_JOB_ID, ownerJobId)
            .append(OWNER_TABLE, ownerTable)
            .append(RESERVED_AT, new Date());
    try {
      ctx.businessKeyReservations().insertOne(session, reservation);
    } catch (RuntimeException e) {
      if (ctx.constraintDetector().isDuplicateKey(e)) {
        throw new BusinessKeyReservationConflictException(businessKey, e);
      }
      // Preserve transaction error labels so ClientSession.withTransaction can retry the callback.
      throw e;
    }
  }

  boolean releaseBusinessKeyHold(UUID jobId, long claimSeq) {
    return ctx.timedStoreOperation(
        "release_business_key_hold",
        () -> {
          try {
            return ctx.businessKeyReservations()
                    .deleteOne(
                        and(
                            eq(OWNER_JOB_ID, jobId),
                            eq(HELD_CLAIM_SEQ, claimSeq),
                            exists(HELD_UNTIL)))
                    .getDeletedCount()
                > 0;
          } catch (RuntimeException e) {
            throw ctx.translateTransientStoreException("release business key hold", e);
          }
        },
        deleted -> deleted ? "deleted" : "miss");
  }

  int releaseBusinessKeyHolds(List<String> nodeIds) {
    if (nodeIds.isEmpty()) return 0;
    return ctx.timedStoreOperation(
        "release_business_key_holds",
        () -> {
          try {
            return (int)
                ctx.businessKeyReservations()
                    .deleteMany(and(in(HELD_BY_NODE, nodeIds), exists(HELD_UNTIL)))
                    .getDeletedCount();
          } catch (RuntimeException e) {
            throw ctx.translateTransientStoreException("release business key holds", e);
          }
        },
        count -> count > 0 ? "deleted" : "miss");
  }

  int deleteExpiredBusinessKeyHolds(Instant now, int limit) {
    if (limit <= 0) return 0;
    return ctx.timedStoreOperation(
        "delete_expired_business_key_holds",
        () -> {
          try {
            List<Object> ids = new ArrayList<>();
            for (Document row :
                ctx.businessKeyReservations()
                    .find(lt(HELD_UNTIL, Date.from(now)))
                    .projection(new Document(ID, 1))
                    .limit(limit)) {
              ids.add(row.get(ID));
            }
            if (ids.isEmpty()) return 0;
            return (int)
                ctx.businessKeyReservations()
                    .deleteMany(and(in(ID, ids), lt(HELD_UNTIL, Date.from(now))))
                    .getDeletedCount();
          } catch (RuntimeException e) {
            throw ctx.translateTransientStoreException("delete expired business key holds", e);
          }
        },
        count -> count > 0 ? "deleted" : "miss");
  }
}
