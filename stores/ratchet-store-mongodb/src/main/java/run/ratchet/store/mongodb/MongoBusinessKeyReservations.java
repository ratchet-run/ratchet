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

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.in;
import static run.ratchet.store.mongodb.MongoFieldNames.BUSINESS_KEY;
import static run.ratchet.store.mongodb.MongoFieldNames.ID;
import static run.ratchet.store.mongodb.MongoFieldNames.JOB_TYPE;
import static run.ratchet.store.mongodb.MongoFieldNames.OWNER_JOB_ID;
import static run.ratchet.store.mongodb.MongoFieldNames.OWNER_TABLE;
import static run.ratchet.store.mongodb.MongoFieldNames.RESERVED_AT;

import com.mongodb.client.ClientSession;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import org.bson.Document;
import run.ratchet.api.JobStatus;
import run.ratchet.api.exception.RatchetTransientStoreException;
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
    releaseByOwner(session, job.getId());
    JobStatus status = StatusClassifier.effectiveStatus(job.getStatus());
    if (StatusClassifier.isLiveStatus(status) && job.getBusinessKey() != null) {
      reserve(
          session,
          job.getBusinessKey(),
          job.getId(),
          BusinessKeyReservations.ownerTableFor(job.getJobType()));
    }
  }

  void syncForStoredJob(ClientSession session, UUID ownerJobId, JobStatus status) {
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
        throw new RatchetTransientStoreException("Active business key in use: " + businessKey, e);
      }
      // Preserve transaction error labels so ClientSession.withTransaction can retry the callback.
      throw e;
    }
  }
}
