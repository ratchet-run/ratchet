package run.ratchet.store.mysql;

import run.ratchet.store.entity.NodeEntity;
import run.ratchet.store.spi.LockStore;
import run.ratchet.store.spi.NodeStore;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

final class MysqlNodeLockOperations implements NodeStore, LockStore {

  private final MysqlStoreContext ctx;

  MysqlNodeLockOperations(MysqlStoreContext ctx) {
    this.ctx = ctx;
  }

  @Override
  public boolean tryLock(String name, Duration ttl, String nodeId) {
    ctx.em()
        .createNativeQuery(
            "INSERT INTO scheduler_lock (lock_name, owner_node, locked_at, expires_at) "
                + "VALUES (?, ?, NOW(3), DATE_ADD(NOW(3), INTERVAL ? SECOND)) "
                + "ON DUPLICATE KEY UPDATE "
                + "  owner_node = IF(expires_at < NOW(3), VALUES(owner_node), owner_node), "
                + "  locked_at = IF(expires_at < NOW(3), NOW(3), locked_at), "
                + "  expires_at = IF(expires_at < NOW(3), VALUES(expires_at), expires_at)")
        .setParameter(1, name)
        .setParameter(2, nodeId)
        .setParameter(3, ttl.toSeconds())
        .executeUpdate();

    Object owner =
        ctx.em()
            .createNativeQuery("SELECT owner_node FROM scheduler_lock WHERE lock_name = ?")
            .setParameter(1, name)
            .getSingleResult();
    return nodeId.equals(owner);
  }

  @Override
  public void unlock(String name, String nodeId) {
    ctx.em()
        .createNativeQuery("DELETE FROM scheduler_lock WHERE lock_name = ? AND owner_node = ?")
        .setParameter(1, name)
        .setParameter(2, nodeId)
        .executeUpdate();
  }

  @Override
  public boolean renewLock(String name, Duration extension, String nodeId) {
    int updated =
        ctx.em()
            .createNativeQuery(
                "UPDATE scheduler_lock SET expires_at = DATE_ADD(NOW(3), INTERVAL ? SECOND) "
                    + "WHERE lock_name = ? AND owner_node = ?")
            .setParameter(1, extension.toSeconds())
            .setParameter(2, name)
            .setParameter(3, nodeId)
            .executeUpdate();
    return updated > 0;
  }

  @Override
  public void upsertHeartbeat(String nodeId, Instant ts) {
    Timestamp tsTs = Timestamp.from(ts);
    ctx.em()
        .createNativeQuery(
            "INSERT INTO scheduler_node (node_id, heartbeat_ts, started_at) "
                + "VALUES (?, ?, ?) "
                + "ON DUPLICATE KEY UPDATE heartbeat_ts = VALUES(heartbeat_ts)")
        .setParameter(1, nodeId)
        .setParameter(2, tsTs)
        .setParameter(3, tsTs)
        .executeUpdate();
  }

  @Override
  public Optional<NodeEntity> findNodeById(String nodeId) {
    return Optional.ofNullable(ctx.em().find(NodeEntity.class, nodeId));
  }

  @Override
  public List<NodeEntity> findInactiveNodesSince(Instant cutoff) {
    return ctx.em()
        .createQuery("SELECT n FROM NodeEntity n WHERE n.lastHeartbeat < :cutoff", NodeEntity.class)
        .setParameter("cutoff", cutoff)
        .getResultList();
  }

  @Override
  public int deleteInactiveNodesSince(Instant cutoff) {
    return ctx.em()
        .createQuery("DELETE FROM NodeEntity n WHERE n.lastHeartbeat < :cutoff")
        .setParameter("cutoff", cutoff)
        .executeUpdate();
  }

  @Override
  public Instant getDatabaseTime() {
    Timestamp ts = (Timestamp) ctx.em().createNativeQuery("SELECT NOW(3)").getSingleResult();
    return ts.toInstant();
  }
}
