package run.ratchet.store.spi;

import run.ratchet.store.entity.NodeEntity;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/** Cluster node registration and health monitoring operations. */
public interface NodeStore {

  /** Creates or updates the heartbeat row for a scheduler node. */
  void upsertHeartbeat(String nodeId, Instant ts);

  /** Loads a node registration by node ID. */
  Optional<NodeEntity> findNodeById(String nodeId);

  /** Lists nodes whose heartbeat is older than the supplied cutoff. */
  List<NodeEntity> findInactiveNodesSince(Instant cutoff);

  /** Deletes node registrations whose heartbeat is older than the supplied cutoff. */
  int deleteInactiveNodesSince(Instant cutoff);

  /** Returns the current database server time for clock skew detection. */
  Instant getDatabaseTime();
}
