package run.ratchet.store.spi;

import run.ratchet.api.Incubating;
import run.ratchet.store.entity.NodeEntity;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/** Cluster node registration and health monitoring operations. */
@Incubating
public interface NodeStore {

  void upsertHeartbeat(String nodeId, Instant ts);

  Optional<NodeEntity> findNodeById(String nodeId);

  List<NodeEntity> findInactiveNodesSince(Instant cutoff);

  int deleteInactiveNodesSince(Instant cutoff);

  /** Returns the current database server time for clock skew detection. */
  Instant getDatabaseTime();
}
