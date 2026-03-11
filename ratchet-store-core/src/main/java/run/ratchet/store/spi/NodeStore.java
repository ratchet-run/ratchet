package run.ratchet.store.spi;

import run.ratchet.store.entity.NodeEntity;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/** Cluster node registration and health monitoring operations. */
public interface NodeStore {

  void upsertHeartbeat(String nodeId, Instant ts);

  Optional<NodeEntity> findNodeById(String nodeId);

  List<NodeEntity> findInactiveNodesSince(Instant cutoff);

  int deleteInactiveNodesSince(Instant cutoff);
}
