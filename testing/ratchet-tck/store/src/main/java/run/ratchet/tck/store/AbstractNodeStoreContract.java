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
package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Base contract tests for {@code NodeStore}. */
public abstract class AbstractNodeStoreContract implements JobStoreContractFixture {

  @BeforeEach
  @AfterEach
  void cleanupNodeFixture() {
    cleanupStore();
  }

  @Test
  void upsertHeartbeat_andFindById_returnsNode() {
    store().upsertHeartbeat("node-1", Instant.now());

    var node = store().findNodeById("node-1");

    assertTrue(node.isPresent(), "findNodeById should return the upserted node");
  }

  @Test
  void findInactiveNodesSince_returnsStaleNodes() {
    Instant staleTs = Instant.now().minusSeconds(3600);
    store().upsertHeartbeat("stale-node", staleTs);

    var inactive = store().findInactiveNodesSince(Instant.now().minusSeconds(1800));

    assertTrue(
        inactive.stream().anyMatch(n -> "stale-node".equals(n.getId())),
        "Stale node should appear in inactive results");
  }

  @Test
  void deleteInactiveNodesSince_removesStaleNodes() {
    Instant staleTs = Instant.now().minusSeconds(3600);
    store().upsertHeartbeat("stale-node", staleTs);

    int deleted = store().deleteInactiveNodesSince(Instant.now().minusSeconds(1800));

    assertTrue(deleted > 0, "deleteInactiveNodesSince should report removed nodes");
    assertFalse(
        store().findNodeById("stale-node").isPresent(), "Deleted node should no longer be found");
  }

  @Test
  void findNodeById_unknownNode_returnsEmpty() {
    var result = store().findNodeById("nonexistent-node");

    assertFalse(result.isPresent(), "findNodeById with unknown ID should return empty");
  }

  @Test
  void upsertHeartbeat_updatesExistingNode() {
    Instant first = Instant.now().minusSeconds(60);
    store().upsertHeartbeat("node-1", first);

    Instant second = Instant.now();
    store().upsertHeartbeat("node-1", second);

    var node = store().findNodeById("node-1").orElseThrow();
    assertTrue(
        Duration.between(first, node.getLastHeartbeat()).getSeconds() > 0,
        "Second upsert should update lastHeartbeat to a later time");
  }

  @Test
  void findInactiveNodesSince_excludesRecentHeartbeats() {
    store().upsertHeartbeat("active-node", Instant.now());
    store().upsertHeartbeat("stale-node", Instant.now().minusSeconds(3600));

    var inactive = store().findInactiveNodesSince(Instant.now().minusSeconds(1800));

    assertFalse(
        inactive.stream().anyMatch(n -> "active-node".equals(n.getId())),
        "Active node should not appear in inactive results");
    assertTrue(
        inactive.stream().anyMatch(n -> "stale-node".equals(n.getId())),
        "Stale node should appear in inactive results");
  }

  @Test
  void deleteInactiveNodesSince_preservesActiveNodes() {
    store().upsertHeartbeat("active-node", Instant.now());
    store().upsertHeartbeat("stale-node", Instant.now().minusSeconds(3600));

    store().deleteInactiveNodesSince(Instant.now().minusSeconds(1800));

    assertTrue(
        store().findNodeById("active-node").isPresent(),
        "Active node should survive the delete sweep");
    assertFalse(store().findNodeById("stale-node").isPresent(), "Stale node should be deleted");
  }

  @Test
  void getDatabaseTime_isReasonablyCloseToSystemTime() {
    Instant before = Instant.now().minusSeconds(5);
    Instant dbTime = store().getDatabaseTime();
    Instant after = Instant.now().plusSeconds(5);

    assertTrue(
        !dbTime.isBefore(before) && !dbTime.isAfter(after),
        "Database time should be within 5 seconds of system time; got " + dbTime);
  }
}
