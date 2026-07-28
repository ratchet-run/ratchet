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
package run.ratchet.store.sqlserver;

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

final class SqlserverNodeLockOperations implements LockStore, NodeStore {

  private static final int MAX_INACTIVE_NODES = 1000;

  private final SqlserverStoreContext ctx;

  SqlserverNodeLockOperations(SqlserverStoreContext ctx) {
    this.ctx = ctx;
  }

  @Override
  public boolean tryLock(String name, Duration ttl, String nodeId) {
    requireLockName(name);
    requirePositiveDuration(ttl, "ttl");
    Objects.requireNonNull(nodeId, "nodeId");
    try {
      long ttlMicros = durationMicros(ttl);
      // expires_at is computed as whole seconds + sub-second microsecond remainder: DATEADD's
      // interval argument is a 32-bit int and a microsecond TTL overflows it past ~35.8 minutes.
      // language=SQL Server
      String sql =
          """
          MERGE scheduler_lock WITH (HOLDLOCK) AS tgt
          USING (VALUES (?, ?, ?)) AS src(lock_name, owner_node, ttl_micros)
            ON tgt.lock_name = src.lock_name
          WHEN MATCHED AND (tgt.expires_at < SYSUTCDATETIME()
                            OR tgt.owner_node = src.owner_node) THEN UPDATE SET
            owner_node = src.owner_node,
            locked_at = SYSUTCDATETIME(),
            expires_at = DATEADD(MICROSECOND, src.ttl_micros % 1000000,
                                 DATEADD(SECOND, src.ttl_micros / 1000000, SYSUTCDATETIME()))
          WHEN NOT MATCHED THEN INSERT (lock_name, owner_node, locked_at, expires_at)
            VALUES (src.lock_name, src.owner_node, SYSUTCDATETIME(),
                    DATEADD(MICROSECOND, src.ttl_micros % 1000000,
                            DATEADD(SECOND, src.ttl_micros / 1000000, SYSUTCDATETIME())));
          """;
      int updated =
          ctx.em()
              .createNativeQuery(sql)
              .setParameter(1, name)
              .setParameter(2, nodeId)
              .setParameter(3, ttlMicros)
              .executeUpdate();
      return updated > 0;
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("try lock", e);
    }
  }

  @Override
  public void unlock(String name, String nodeId) {
    requireLockName(name);
    Objects.requireNonNull(nodeId, "nodeId");
    try {
      // language=SQL Server
      String sql = "DELETE FROM scheduler_lock WHERE lock_name = ? AND owner_node = ?";
      ctx.em().createNativeQuery(sql).setParameter(1, name).setParameter(2, nodeId).executeUpdate();
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("unlock", e);
    }
  }

  @Override
  public boolean renewLock(String name, Duration extension, String nodeId) {
    requireLockName(name);
    requirePositiveDuration(extension, "extension");
    Objects.requireNonNull(nodeId, "nodeId");
    try {
      long extensionMicros = durationMicros(extension);
      // DATEADD's interval argument is a 32-bit int; microseconds overflow it at ~35.8 minutes, so
      // add whole seconds and the sub-second microsecond remainder separately. Whole seconds fit an
      // int for any realistic lease.
      // language=SQL Server
      String sql =
          """
          UPDATE scheduler_lock
          SET expires_at = DATEADD(MICROSECOND, ? % 1000000,
                                   DATEADD(SECOND, ? / 1000000, SYSUTCDATETIME()))
          WHERE lock_name = ? AND owner_node = ?
          """;
      int updated =
          ctx.em()
              .createNativeQuery(sql)
              .setParameter(1, extensionMicros)
              .setParameter(2, extensionMicros)
              .setParameter(3, name)
              .setParameter(4, nodeId)
              .executeUpdate();
      return updated > 0;
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("renew lock", e);
    }
  }

  @Override
  public void upsertHeartbeat(String nodeId, Instant ts) {
    Objects.requireNonNull(nodeId, "nodeId");
    Objects.requireNonNull(ts, "ts");
    try {
      // language=SQL Server
      String sql =
          """
          MERGE scheduler_node WITH (HOLDLOCK) AS tgt
          USING (VALUES (?, ?, ?)) AS src(node_id, heartbeat_ts, started_at)
            ON tgt.node_id = src.node_id
          WHEN MATCHED THEN UPDATE SET heartbeat_ts = src.heartbeat_ts
          WHEN NOT MATCHED THEN INSERT (node_id, heartbeat_ts, started_at)
            VALUES (src.node_id, src.heartbeat_ts, src.started_at);
          """;
      ctx.em()
          .createNativeQuery(sql)
          .setParameter(1, nodeId)
          .setParameter(2, Timestamp.from(ts))
          .setParameter(3, Timestamp.from(ts))
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
      // language=SQL Server
      String sql =
          "SELECT * FROM scheduler_node WHERE heartbeat_ts < ?"
              + " ORDER BY (SELECT 1) OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY";
      return ctx.em()
          .createNativeQuery(sql, NodeEntity.class)
          .setParameter(1, SqlserverTimestamps.microTimestamp(cutoff))
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
      // language=SQL Server
      String sql =
          "SELECT * FROM scheduler_node ORDER BY heartbeat_ts DESC"
              + " OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY";
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
      // language=SQL Server
      String sql = "DELETE FROM scheduler_node WHERE heartbeat_ts < ?";
      return ctx.em()
          .createNativeQuery(sql)
          .setParameter(1, SqlserverTimestamps.microTimestamp(cutoff))
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
      // language=SQL Server
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
      // language=SQL Server
      String sql = "SELECT DATEDIFF_BIG(MILLISECOND, '19700101', SYSUTCDATETIME())";
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
