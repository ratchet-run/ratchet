package run.ratchet.store.mysql;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import run.ratchet.store.entity.NodeEntity;
import run.ratchet.store.spi.LockStore;
import run.ratchet.store.spi.NodeStore;

final class MysqlNodeLockOperations implements NodeStore, LockStore {

  private final MysqlStoreContext ctx;

  MysqlNodeLockOperations(MysqlStoreContext ctx) {
    this.ctx = ctx;
  }

  @Override
  public boolean tryLock(String name, Duration ttl, String nodeId) {
    requireLockName(name);
    requirePositiveDuration(ttl, "ttl");
    Objects.requireNonNull(nodeId, "nodeId");
    long ttlSeconds = ttl.toSeconds();
    // language=MySQL
    String updateSql =
        """
        UPDATE scheduler_lock
        SET owner_node = ?,
            locked_at = CASE
              WHEN locked_at = NOW(6) THEN locked_at + INTERVAL 1 MICROSECOND
              ELSE NOW(6)
            END,
            expires_at = DATE_ADD(NOW(6), INTERVAL ? SECOND)
        WHERE lock_name = ?
          AND (expires_at < NOW(6) OR owner_node = ?)
        """;
    int updated =
        ctx.em()
            .createNativeQuery(updateSql)
            .setParameter(1, nodeId)
            .setParameter(2, ttlSeconds)
            .setParameter(3, name)
            .setParameter(4, nodeId)
            .executeUpdate();
    if (updated > 0) {
      return true;
    }

    // language=MySQL
    String insertSql =
        """
        INSERT IGNORE INTO scheduler_lock (lock_name, owner_node, locked_at, expires_at)
        VALUES (?, ?, NOW(6), DATE_ADD(NOW(6), INTERVAL ? SECOND))
        """;
    int inserted =
        ctx.em()
            .createNativeQuery(insertSql)
            .setParameter(1, name)
            .setParameter(2, nodeId)
            .setParameter(3, ttlSeconds)
            .executeUpdate();
    return inserted > 0;
  }

  @Override
  public void unlock(String name, String nodeId) {
    requireLockName(name);
    Objects.requireNonNull(nodeId, "nodeId");
    // language=MySQL
    String sql = "DELETE FROM scheduler_lock WHERE lock_name = ? AND owner_node = ?";
    ctx.em().createNativeQuery(sql).setParameter(1, name).setParameter(2, nodeId).executeUpdate();
  }

  @Override
  public boolean renewLock(String name, Duration extension, String nodeId) {
    requireLockName(name);
    requirePositiveDuration(extension, "extension");
    Objects.requireNonNull(nodeId, "nodeId");
    // language=MySQL
    String sql =
        """
        UPDATE scheduler_lock SET expires_at = DATE_ADD(NOW(3), INTERVAL ? SECOND)
        WHERE lock_name = ? AND owner_node = ?
        """;
    int updated =
        ctx.em()
            .createNativeQuery(sql)
            .setParameter(1, extension.toSeconds())
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
    Timestamp tsTs = Timestamp.from(ts);
    // language=MySQL
    String sql =
        """
        INSERT INTO scheduler_node (node_id, heartbeat_ts, started_at)
        VALUES (?, ?, ?)
        ON DUPLICATE KEY UPDATE heartbeat_ts = VALUES(heartbeat_ts)
        """;
    ctx.em()
        .createNativeQuery(sql)
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
    // language=JPAQL
    String jpql = "SELECT n FROM NodeEntity n WHERE n.lastHeartbeat < :cutoff";
    return ctx.em()
        .createQuery(jpql, NodeEntity.class)
        .setParameter("cutoff", cutoff)
        .getResultList();
  }

  @Override
  public int deleteInactiveNodesSince(Instant cutoff) {
    // language=JPAQL
    String jpql = "DELETE FROM NodeEntity n WHERE n.lastHeartbeat < :cutoff";
    return ctx.em().createQuery(jpql).setParameter("cutoff", cutoff).executeUpdate();
  }

  @Override
  public Instant getDatabaseTime() {
    // language=MySQL
    String sql = "SELECT CAST(ROUND(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(3)) * 1000) AS SIGNED)";
    Object epochMillis = ctx.em().createNativeQuery(sql).getSingleResult();
    if (epochMillis instanceof Number n) {
      return Instant.ofEpochMilli(n.longValue());
    }
    throw new IllegalStateException(
        "Unexpected database epoch millis result type: "
            + (epochMillis == null ? "null" : epochMillis.getClass().getName()));
  }
}
