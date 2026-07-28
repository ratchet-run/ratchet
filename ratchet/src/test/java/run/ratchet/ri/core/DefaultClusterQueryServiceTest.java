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
package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobPage;
import run.ratchet.api.NodeStatus;
import run.ratchet.api.RatchetOptions;
import run.ratchet.store.entity.NodeEntity;
import run.ratchet.store.spi.NodeStore;

class DefaultClusterQueryServiceTest {

  @Test
  void getNodesComputesLivenessLocalFlagAndPreservesStoreOrdering() {
    Instant now = Instant.parse("2026-05-12T12:00:00Z");
    Instant cutoff = now.minusSeconds(60);
    StubNodeStore store =
        new StubNodeStore(
            List.of(
                node("newest-node", now.minusSeconds(1)),
                node("local-node", cutoff),
                node("stale-node", cutoff.minusNanos(1))));
    DefaultClusterQueryService service =
        new DefaultClusterQueryService(
            store,
            () -> "local-node",
            RatchetOptions.builder().node(node -> node.orphanGraceSeconds(60)).build(),
            Clock.fixed(now, ZoneOffset.UTC));

    JobPage<NodeStatus> page = service.getNodes();

    assertEquals(1000, store.requestedLimit(), "getNodes should use the roster cap");
    assertEquals(
        List.of("newest-node", "local-node", "stale-node"),
        page.items().stream().map(NodeStatus::nodeId).toList(),
        "getNodes should preserve newest-first store ordering");
    assertTrue(page.items().get(0).active(), "heartbeat after cutoff should be active");
    assertFalse(page.items().get(0).local(), "non-local node should not be flagged local");
    assertTrue(page.items().get(1).active(), "heartbeat exactly at cutoff should be active");
    assertTrue(page.items().get(1).local(), "local node id should be flagged local");
    assertFalse(page.items().get(2).active(), "heartbeat before cutoff should be inactive");
    assertFalse(page.items().get(2).local(), "stale non-local node should not be flagged local");
    assertEquals(3L, page.totalCount());
    assertEquals(1000, page.limit());
    assertEquals(0, page.offset());
    assertFalse(page.hasMore());
    assertNull(page.nextCursor());
  }

  private static NodeEntity node(String id, Instant heartbeat) {
    NodeEntity node = new NodeEntity();
    node.setId(id);
    node.setStartedAt(heartbeat.minusSeconds(3600));
    node.setLastHeartbeat(heartbeat);
    return node;
  }

  private static final class StubNodeStore implements NodeStore {
    private final List<NodeEntity> rows;
    private int requestedLimit = -1;

    private StubNodeStore(List<NodeEntity> rows) {
      this.rows = List.copyOf(rows);
    }

    private int requestedLimit() {
      return requestedLimit;
    }

    @Override
    public void upsertHeartbeat(String nodeId, Instant ts) {
      throw new AssertionError("upsertHeartbeat must not be used by cluster queries");
    }

    @Override
    public Optional<NodeEntity> findNodeById(String nodeId) {
      throw new AssertionError("findNodeById must not be used by cluster queries");
    }

    @Override
    public List<NodeEntity> findInactiveNodesSince(Instant cutoff) {
      throw new AssertionError("findInactiveNodesSince must not be used by cluster queries");
    }

    @Override
    public List<NodeEntity> findAllNodes(int limit) {
      requestedLimit = limit;
      return rows;
    }

    @Override
    public int deleteInactiveNodesSince(Instant cutoff) {
      throw new AssertionError("deleteInactiveNodesSince must not be used by cluster queries");
    }

    @Override
    public int deleteInactiveNodesByIds(Collection<String> nodeIds) {
      throw new AssertionError("deleteInactiveNodesByIds must not be used by cluster queries");
    }

    @Override
    public Instant getDatabaseTime() {
      throw new AssertionError("getDatabaseTime must not be used by cluster queries");
    }
  }
}
