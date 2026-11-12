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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static run.ratchet.store.mongodb.MongoFieldNames.ACTIVE_COUNT;
import static run.ratchet.store.mongodb.MongoFieldNames.ID;
import static run.ratchet.store.mongodb.MongoFieldNames.JOB_ID;
import static run.ratchet.store.mongodb.MongoFieldNames.RESOURCE_NAME;

import java.util.List;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.ResourcePermitStore.AcquireResult;

class ResourcePermitIT extends BaseDocumentStoreIT {

  @Test
  void acquireAndRelease_withinLimit() {
    store().configureResource("gpu", 2, 5000, "GPU resource pool");

    JobEntity j1 = claimNewJob();
    JobEntity j2 = claimNewJob();
    JobEntity j3 = claimNewJob();

    assertEquals(
        AcquireResult.ACQUIRED,
        store().tryAcquirePermit("gpu", j1.getId(), j1.getClaimSeq(), "node-1"));
    assertEquals(
        AcquireResult.ACQUIRED,
        store().tryAcquirePermit("gpu", j2.getId(), j2.getClaimSeq(), "node-1"));

    assertEquals(
        AcquireResult.AT_CAPACITY,
        store().tryAcquirePermit("gpu", j3.getId(), j3.getClaimSeq(), "node-1"));

    store().releasePermit("gpu", j1.getId(), j1.getClaimSeq());
    assertEquals(
        AcquireResult.ACQUIRED,
        store().tryAcquirePermit("gpu", j3.getId(), j3.getClaimSeq(), "node-1"));
    assertEquals(1, permitCount("gpu", j3));
    assertEquals(2, activeCount("gpu"));
  }

  @Test
  void releaseAllPermits_clearsForJob() {
    store().configureResource("db-conn", 3, 5000, "DB connection pool");

    JobEntity j1 = claimNewJob();
    store().tryAcquirePermit("db-conn", j1.getId(), j1.getClaimSeq(), "node-1");

    store().releaseAllPermits(j1.getId());

    JobEntity j2 = claimNewJob();
    assertEquals(
        AcquireResult.ACQUIRED,
        store().tryAcquirePermit("db-conn", j2.getId(), j2.getClaimSeq(), "node-1"));
    assertEquals(0, permitCount("db-conn", j1));
    assertEquals(1, permitCount("db-conn", j2));
    assertEquals(1, activeCount("db-conn"));
  }

  @Test
  void releaseAllPermits_clearsMultipleResourcesForJob() {
    store().configureResource("api", 1, 5000, "API slot");
    store().configureResource("disk", 1, 5000, "Disk slot");

    JobEntity first = claimNewJob();
    JobEntity second = claimNewJob();

    assertEquals(
        AcquireResult.ACQUIRED,
        store().tryAcquirePermit("api", first.getId(), first.getClaimSeq(), "node-1"));
    assertEquals(
        AcquireResult.ACQUIRED,
        store().tryAcquirePermit("disk", first.getId(), first.getClaimSeq(), "node-1"));

    assertEquals(
        AcquireResult.AT_CAPACITY,
        store().tryAcquirePermit("api", second.getId(), second.getClaimSeq(), "node-2"));
    assertEquals(
        AcquireResult.AT_CAPACITY,
        store().tryAcquirePermit("disk", second.getId(), second.getClaimSeq(), "node-2"));

    store().releaseAllPermits(first.getId());

    assertEquals(
        AcquireResult.ACQUIRED,
        store().tryAcquirePermit("api", second.getId(), second.getClaimSeq(), "node-2"));
    assertEquals(
        AcquireResult.ACQUIRED,
        store().tryAcquirePermit("disk", second.getId(), second.getClaimSeq(), "node-2"));
    assertEquals(0, permitCount("api", first));
    assertEquals(0, permitCount("disk", first));
    assertEquals(1, permitCount("api", second));
    assertEquals(1, permitCount("disk", second));
    assertEquals(1, activeCount("api"));
    assertEquals(1, activeCount("disk"));
  }

  @Test
  void configuredResource_deniesAfterPoolIsExhaustedUntilRelease() {
    store().configureResource("cpu", 1, 5000, "CPU slot");

    JobEntity first = claimNewJob();
    JobEntity second = claimNewJob();

    assertEquals(
        AcquireResult.ACQUIRED,
        store().tryAcquirePermit("cpu", first.getId(), first.getClaimSeq(), "node-1"));
    assertEquals(
        AcquireResult.AT_CAPACITY,
        store().tryAcquirePermit("cpu", second.getId(), second.getClaimSeq(), "node-2"));
    assertEquals(1, permitCount("cpu", first));
    assertEquals(0, permitCount("cpu", second));
    assertEquals(1, activeCount("cpu"));

    store().releasePermit("cpu", first.getId(), first.getClaimSeq());

    assertEquals(
        AcquireResult.ACQUIRED,
        store().tryAcquirePermit("cpu", second.getId(), second.getClaimSeq(), "node-2"));
    assertEquals(0, permitCount("cpu", first));
    assertEquals(1, permitCount("cpu", second));
    assertEquals(1, activeCount("cpu"));
  }

  @Test
  void cleanupOrphanedPermits_decrementsActiveCountsByResource() {
    store().configureResource("cleanup-api", 4, 5000, "API slots");
    store().configureResource("cleanup-disk", 3, 5000, "Disk slots");

    JobEntity staleApiOne = claimNewJob();
    JobEntity staleApiTwo = claimNewJob();
    JobEntity staleDisk = claimNewJob();
    JobEntity liveApi = claimNewJob();
    JobEntity liveDisk = claimNewJob();

    assertEquals(
        AcquireResult.ACQUIRED,
        store()
            .tryAcquirePermit(
                "cleanup-api", staleApiOne.getId(), staleApiOne.getClaimSeq(), "stale-node-1"));
    assertEquals(
        AcquireResult.ACQUIRED,
        store()
            .tryAcquirePermit(
                "cleanup-api", staleApiTwo.getId(), staleApiTwo.getClaimSeq(), "stale-node-2"));
    assertEquals(
        AcquireResult.ACQUIRED,
        store()
            .tryAcquirePermit(
                "cleanup-disk", staleDisk.getId(), staleDisk.getClaimSeq(), "stale-node-1"));
    assertEquals(
        AcquireResult.ACQUIRED,
        store()
            .tryAcquirePermit("cleanup-api", liveApi.getId(), liveApi.getClaimSeq(), "live-node"));
    assertEquals(
        AcquireResult.ACQUIRED,
        store()
            .tryAcquirePermit(
                "cleanup-disk", liveDisk.getId(), liveDisk.getClaimSeq(), "live-node"));

    int cleaned = store().cleanupOrphanedPermits(List.of("stale-node-1", "stale-node-2"));

    assertEquals(3, cleaned);
    assertEquals(1, activeCount("cleanup-api"));
    assertEquals(1, activeCount("cleanup-disk"));
    assertEquals(0, permitCount("cleanup-api", staleApiOne));
    assertEquals(0, permitCount("cleanup-api", staleApiTwo));
    assertEquals(0, permitCount("cleanup-disk", staleDisk));
    assertEquals(1, permitCount("cleanup-api", liveApi));
    assertEquals(1, permitCount("cleanup-disk", liveDisk));
  }

  @Test
  void unconfiguredResource_failsHard() {
    JobEntity job = claimNewJob();
    assertThrows(
        IllegalArgumentException.class,
        () ->
            store()
                .tryAcquirePermit(
                    "nonexistent-resource", job.getId(), job.getClaimSeq(), "node-1"));
  }

  private JobEntity claimNewJob() {
    store().save(newPendingJob());
    return store().claimNextBatch(1, "node-1").get(0);
  }

  private long permitCount(String resource, JobEntity job) {
    return database()
        .getCollection("scheduler_resource_permit")
        .countDocuments(and(eq(RESOURCE_NAME, resource), eq(JOB_ID, job.getId())));
  }

  private int activeCount(String resource) {
    Document limit =
        database().getCollection("scheduler_resource_limit").find(eq(ID, resource)).first();
    return limit.getInteger(ACTIVE_COUNT);
  }
}
