package run.ratchet.store.spi;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import run.ratchet.api.Incubating;
import run.ratchet.store.entity.NodeEntity;

/** Cluster node registration and health monitoring operations. */
@Incubating
public interface NodeStore {

  /** Inserts or updates a node heartbeat. Transaction attribute: {@code REQUIRED}. */
  void upsertHeartbeat(String nodeId, Instant ts);

  /** Finds a node by id. Transaction attribute: {@code SUPPORTS}. */
  Optional<NodeEntity> findNodeById(String nodeId);

  /** Finds inactive nodes. Transaction attribute: {@code SUPPORTS}. */
  List<NodeEntity> findInactiveNodesSince(Instant cutoff);

  /** Deletes inactive node rows. Transaction attribute: {@code REQUIRED}. */
  int deleteInactiveNodesSince(Instant cutoff);

  /** Deletes the resolved inactive node rows by id. Transaction attribute: {@code REQUIRED}. */
  default int deleteInactiveNodesByIds(Collection<String> nodeIds) {
    throw new UnsupportedOperationException("deleteInactiveNodesByIds is not implemented");
  }

  /**
   * Returns the current database server time for clock skew detection. Transaction attribute:
   * {@code SUPPORTS}.
   */
  Instant getDatabaseTime();
}
