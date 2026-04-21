package run.ratchet.store.postgresql;

import run.ratchet.store.entity.NodeEntity;
import run.ratchet.store.spi.LockStore;
import run.ratchet.store.spi.NodeStore;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;

final class PostgresqlNodeLockOperations implements LockStore, NodeStore {

  private final PostgresqlStoreContext ctx;

  PostgresqlNodeLockOperations(PostgresqlStoreContext ctx) {
    this.ctx = ctx;
  }

  @Override
  public boolean tryLock(String name, Duration ttl, String nodeId) {
    long ttlSeconds = ttl.toSeconds();
    int updated =
        ctx.em()
            .createNativeQuery(
                "INSERT INTO scheduler_lock (lock_name, owner_node, locked_at, expires_at) "
                    + "VALUES (?, ?, statement_timestamp(), statement_timestamp() + ? * interval '1 second') "
                    + "ON CONFLICT (lock_name) DO UPDATE SET "
                    + "  owner_node = EXCLUDED.owner_node, "
                    + "  locked_at = statement_timestamp(), "
                    + "  expires_at = statement_timestamp() + ? * interval '1 second' "
                    + "WHERE scheduler_lock.expires_at < statement_timestamp() "
                    + "   OR scheduler_lock.owner_node = ?")
            .setParameter(1, name)
            .setParameter(2, nodeId)
            .setParameter(3, ttlSeconds)
            .setParameter(4, ttlSeconds)
            .setParameter(5, nodeId)
            .executeUpdate();
    return updated > 0;
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
    long extensionSeconds = extension.toSeconds();
    int updated =
        ctx.em()
            .createNativeQuery(
                "UPDATE scheduler_lock SET "
                    + "expires_at = statement_timestamp() + ? * interval '1 second' "
                    + "WHERE lock_name = ? AND owner_node = ?")
            .setParameter(1, extensionSeconds)
            .setParameter(2, name)
            .setParameter(3, nodeId)
            .executeUpdate();
    return updated > 0;
  }

  @Override
  public void upsertHeartbeat(String nodeId, Instant ts) {
    ctx.em()
        .createNativeQuery(
            "INSERT INTO scheduler_node (node_id, heartbeat_ts, started_at) "
                + "VALUES (?, ?, ?) "
                + "ON CONFLICT (node_id) DO UPDATE SET heartbeat_ts = EXCLUDED.heartbeat_ts")
        .setParameter(1, nodeId)
        .setParameter(2, Timestamp.from(ts))
        .setParameter(3, Timestamp.from(ts))
        .executeUpdate();
  }

  @Override
  public Optional<NodeEntity> findNodeById(String nodeId) {
    return Optional.ofNullable(ctx.em().find(NodeEntity.class, nodeId));
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<NodeEntity> findInactiveNodesSince(Instant cutoff) {
    return ctx.em()
        .createNativeQuery("SELECT * FROM scheduler_node WHERE heartbeat_ts < ?", NodeEntity.class)
        .setParameter(1, Timestamp.from(cutoff))
        .getResultList();
  }

  @Override
  public int deleteInactiveNodesSince(Instant cutoff) {
    return ctx.em()
        .createNativeQuery("DELETE FROM scheduler_node WHERE heartbeat_ts < ?")
        .setParameter(1, Timestamp.from(cutoff))
        .executeUpdate();
  }

  @Override
  public Instant getDatabaseTime() {
    Object ts = ctx.em().createNativeQuery("SELECT statement_timestamp()").getSingleResult();
    if (ts instanceof Instant i) {
      return i;
    }
    if (ts instanceof OffsetDateTime odt) {
      return odt.toInstant();
    }
    if (ts instanceof Timestamp t) {
      return t.toInstant();
    }
    throw new IllegalStateException(
        "Unexpected statement_timestamp() result type: "
            + (ts == null ? "null" : ts.getClass().getName()));
  }
}
