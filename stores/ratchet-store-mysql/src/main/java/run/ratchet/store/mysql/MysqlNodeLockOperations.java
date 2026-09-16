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
package run.ratchet.store.mysql;

import static run.ratchet.store.util.LockValidation.durationMicros;
import static run.ratchet.store.util.LockValidation.requireLockName;
import static run.ratchet.store.util.LockValidation.requirePositiveDuration;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import run.ratchet.store.entity.NodeEntity;
import run.ratchet.store.spi.LockStore;
import run.ratchet.store.spi.NodeStore;

final class MysqlNodeLockOperations implements NodeStore, LockStore {

  private static final int MAX_INACTIVE_NODES = 1000;

  private final MysqlStoreContext ctx;

  MysqlNodeLockOperations(MysqlStoreContext ctx) {
    this.ctx = ctx;
  }

  @Override
  public boolean tryLock(String name, Duration ttl, String nodeId) {
    /*
     * MysqlJobStoreImpl runs both statements in one REQUIRES_NEW transaction. The no-op
     * upsert takes an exclusive record lock even when another node owns the lease. Only
     * the conditional UPDATE decides ownership; upsert counts depend on CLIENT_FOUND_ROWS.
     */
    try {
      requireLockName(name);
      requirePositiveDuration(ttl, "ttl");
      Objects.requireNonNull(nodeId, "nodeId");
      long ttlMicros = durationMicros(ttl);
      // Duplicate-key INSERT IGNORE takes shared locks that can deadlock when the holder
      // deletes its lease. Acquire the row exclusively before checking whether it is available.
      String ensureRowSql =
          """
          INSERT INTO scheduler_lock (lock_name, owner_node, locked_at, expires_at)
          VALUES (?, ?, NOW(6), DATE_ADD(NOW(6), INTERVAL ? MICROSECOND))
          ON DUPLICATE KEY UPDATE lock_name = lock_name
          """;
      ctx.em()
          .createNativeQuery(ensureRowSql)
          .setParameter(1, name)
          .setParameter(2, nodeId)
          .setParameter(3, ttlMicros)
          .executeUpdate();

      // language=MySQL
      String updateSql =
          """
          UPDATE scheduler_lock
          SET owner_node = ?,
              locked_at = CASE
                WHEN locked_at = NOW(6) THEN locked_at + INTERVAL 1 MICROSECOND
                ELSE NOW(6)
              END,
              expires_at = DATE_ADD(NOW(6), INTERVAL ? MICROSECOND)
          WHERE lock_name = ?
            AND (expires_at < NOW(6) OR owner_node = ?)
          """;
      int updated =
          ctx.em()
              .createNativeQuery(updateSql)
              .setParameter(1, nodeId)
              .setParameter(2, ttlMicros)
              .setParameter(3, name)
              .setParameter(4, nodeId)
              .executeUpdate();
      return updated > 0;
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("try lock", e);
    }
  }

  @Override
  public void unlock(String name, String nodeId) {
    try {
      requireLockName(name);
      Objects.requireNonNull(nodeId, "nodeId");
      // language=MySQL
      String sql = "DELETE FROM scheduler_lock WHERE lock_name = ? AND owner_node = ?";
      ctx.em().createNativeQuery(sql).setParameter(1, name).setParameter(2, nodeId).executeUpdate();
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("unlock", e);
    }
  }

  @Override
  public boolean renewLock(String name, Duration extension, String nodeId) {
    try {
      requireLockName(name);
      requirePositiveDuration(extension, "extension");
      Objects.requireNonNull(nodeId, "nodeId");
      // language=MySQL
      String sql =
          """
          UPDATE scheduler_lock SET expires_at = DATE_ADD(NOW(6), INTERVAL ? MICROSECOND)
          WHERE lock_name = ? AND owner_node = ?
          """;
      int updated =
          ctx.em()
              .createNativeQuery(sql)
              .setParameter(1, durationMicros(extension))
              .setParameter(2, name)
              .setParameter(3, nodeId)
              .executeUpdate();
      return updated > 0;
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("renew lock", e);
    }
  }

  @Override
  public void upsertHeartbeat(String nodeId, Instant ts) {
    try {
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
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("upsert heartbeat", e);
    }
  }

  @Override
  public Optional<NodeEntity> findNodeById(String nodeId) {
    try {
      return Optional.ofNullable(ctx.em().find(NodeEntity.class, nodeId));
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("find node by id", e);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<NodeEntity> findInactiveNodesSince(Instant cutoff) {
    try {
      // language=MySQL
      String sql = "SELECT * FROM scheduler_node WHERE heartbeat_ts < ? LIMIT ?";
      return ctx.em()
          .createNativeQuery(sql, NodeEntity.class)
          .setParameter(1, Timestamp.from(cutoff))
          .setParameter(2, MAX_INACTIVE_NODES)
          .getResultList();
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("find inactive nodes", e);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<NodeEntity> findAllNodes(int limit) {
    if (limit <= 0) {
      return List.of();
    }
    try {
      // language=MySQL
      String sql = "SELECT * FROM scheduler_node ORDER BY heartbeat_ts DESC LIMIT ?";
      return ctx.em()
          .createNativeQuery(sql, NodeEntity.class)
          .setParameter(1, limit)
          .getResultList();
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("find all nodes", e);
    }
  }

  @Override
  public int deleteInactiveNodesSince(Instant cutoff) {
    try {
      // language=MySQL
      String sql = "DELETE FROM scheduler_node WHERE heartbeat_ts < ?";
      return ctx.em()
          .createNativeQuery(sql)
          .setParameter(1, Timestamp.from(cutoff))
          .executeUpdate();
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("delete inactive nodes", e);
    }
  }

  @Override
  public int deleteInactiveNodesByIds(Collection<String> nodeIds) {
    if (nodeIds.isEmpty()) {
      return 0;
    }
    try {
      String placeholders = String.join(",", Collections.nCopies(nodeIds.size(), "?"));
      // language=MySQL
      String sql = "DELETE FROM scheduler_node WHERE node_id IN (" + placeholders + ")";
      var query = ctx.em().createNativeQuery(sql);
      int parameter = 1;
      for (String nodeId : nodeIds) {
        query.setParameter(parameter++, nodeId);
      }
      return query.executeUpdate();
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("delete inactive nodes by id", e);
    }
  }

  @Override
  public Instant getDatabaseTime() {
    try {
      // language=MySQL
      String sql = "SELECT CAST(ROUND(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(3)) * 1000) AS SIGNED)";
      Object epochMillis = ctx.em().createNativeQuery(sql).getSingleResult();
      if (epochMillis instanceof Number n) {
        return Instant.ofEpochMilli(n.longValue());
      }
      throw new IllegalStateException(
          "Unexpected database epoch millis result type: "
              + (epochMillis == null ? "null" : epochMillis.getClass().getName()));
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("get database time", e);
    }
  }
}
