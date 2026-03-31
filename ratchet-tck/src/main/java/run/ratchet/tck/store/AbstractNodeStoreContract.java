package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Base contract tests for {@code NodeStore}. */
public abstract class AbstractNodeStoreContract implements JobStoreContractFixture {

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
  void getDatabaseTime_returnsNonNullInstant() {
    Instant dbTime = store().getDatabaseTime();

    assertNotNull(dbTime, "getDatabaseTime should return a non-null instant");
  }
}
