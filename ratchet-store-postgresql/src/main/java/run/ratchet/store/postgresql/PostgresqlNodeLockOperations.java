package run.ratchet.store.postgresql;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import run.ratchet.store.entity.NodeEntity;
import run.ratchet.store.spi.LockStore;
import run.ratchet.store.spi.NodeStore;

final class PostgresqlNodeLockOperations implements LockStore, NodeStore {

  private final PostgresqlStoreContext ctx;

  PostgresqlNodeLockOperations(PostgresqlStoreContext ctx) {
    this.ctx = ctx;
  }

  @Override
  public boolean tryLock(String name, Duration ttl, String nodeId) {
    requireLockName(name);
    requirePositiveDuration(ttl, "ttl");
    Objects.requireNonNull(nodeId, "nodeId");
    long ttlSeconds = ttl.toSeconds();
    // language=PostgreSQL
    String sql =
        """
        INSERT INTO scheduler_lock (lock_name, owner_node, locked_at, expires_at)
        VALUES (?, ?, statement_timestamp(),
                statement_timestamp() + ? * interval '1 second')
        ON CONFLICT (lock_name) DO UPDATE SET
          owner_node = EXCLUDED.owner_node,
          locked_at = statement_timestamp(),
          expires_at = statement_timestamp() + ? * interval '1 second'
        WHERE scheduler_lock.expires_at < statement_timestamp()
           OR scheduler_lock.owner_node = ?
        """;
    int updated =
        ctx.em()
            .createNativeQuery(sql)
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
    requireLockName(name);
    Objects.requireNonNull(nodeId, "nodeId");
    // language=PostgreSQL
    String sql = "DELETE FROM scheduler_lock WHERE lock_name = ? AND owner_node = ?";
    ctx.em().createNativeQuery(sql).setParameter(1, name).setParameter(2, nodeId).executeUpdate();
  }

  @Override
  public boolean renewLock(String name, Duration extension, String nodeId) {
    requireLockName(name);
    requirePositiveDuration(extension, "extension");
    Objects.requireNonNull(nodeId, "nodeId");
    long extensionSeconds = extension.toSeconds();
    // language=PostgreSQL
    String sql =
        """
        UPDATE scheduler_lock
        SET expires_at = statement_timestamp() + ? * interval '1 second'
        WHERE lock_name = ? AND owner_node = ?
        """;
    int updated =
        ctx.em()
            .createNativeQuery(sql)
            .setParameter(1, extensionSeconds)
            .setParameter(2, name)
            .setParameter(3, nodeId)
            .executeUpdate();
    return updated > 0;
  }

  private static void requireLockName(String name) {
    Objects.requireNonNull(name, "name");
    if (name.isBlank()) {
      throw new IllegalArgumentException("name must be non-empty");
    }
  }

  private static void requirePositiveDuration(Duration duration, String parameterName) {
    Objects.requireNonNull(duration, parameterName);
    if (duration.isZero() || duration.isNegative()) {
      throw new IllegalArgumentException(parameterName + " must be positive");
    }
  }

  @Override
  public void upsertHeartbeat(String nodeId, Instant ts) {
    // language=PostgreSQL
    String sql =
        """
        INSERT INTO scheduler_node (node_id, heartbeat_ts, started_at)
        VALUES (?, ?, ?)
        ON CONFLICT (node_id) DO UPDATE SET heartbeat_ts = EXCLUDED.heartbeat_ts
        """;
    ctx.em()
        .createNativeQuery(sql)
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
    // language=PostgreSQL
    String sql = "SELECT * FROM scheduler_node WHERE heartbeat_ts < ?";
    return ctx.em()
        .createNativeQuery(sql, NodeEntity.class)
        .setParameter(1, Timestamp.from(cutoff))
        .getResultList();
  }

  @Override
  public int deleteInactiveNodesSince(Instant cutoff) {
    // language=PostgreSQL
    String sql = "DELETE FROM scheduler_node WHERE heartbeat_ts < ?";
    return ctx.em().createNativeQuery(sql).setParameter(1, Timestamp.from(cutoff)).executeUpdate();
  }

  @Override
  public Instant getDatabaseTime() {
    // language=PostgreSQL
    String sql = "SELECT (EXTRACT(EPOCH FROM statement_timestamp()) * 1000)::bigint";
    Object epochMillis = ctx.em().createNativeQuery(sql).getSingleResult();
    if (epochMillis instanceof Number n) {
      return Instant.ofEpochMilli(n.longValue());
    }
    throw new IllegalStateException(
        "Unexpected database epoch millis result type: "
            + (epochMillis == null ? "null" : epochMillis.getClass().getName()));
  }
}
