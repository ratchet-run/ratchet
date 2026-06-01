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
package run.ratchet.store.postgresql;

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

final class PostgresqlNodeLockOperations implements LockStore, NodeStore {

  private static final int MAX_INACTIVE_NODES = 1000;

  private final PostgresqlStoreContext ctx;

  PostgresqlNodeLockOperations(PostgresqlStoreContext ctx) {
    this.ctx = ctx;
  }

  @Override
  public boolean tryLock(String name, Duration ttl, String nodeId) {
    requireLockName(name);
    requirePositiveDuration(ttl, "ttl");
    Objects.requireNonNull(nodeId, "nodeId");
    try {
      long ttlMicros = durationMicros(ttl);
      // language=PostgreSQL
      String sql =
          """
          INSERT INTO scheduler_lock (lock_name, owner_node, locked_at, expires_at)
          VALUES (?, ?, statement_timestamp(),
                  statement_timestamp() + ? * interval '1 microsecond')
          ON CONFLICT (lock_name) DO UPDATE SET
            owner_node = EXCLUDED.owner_node,
            locked_at = statement_timestamp(),
            expires_at = statement_timestamp() + ? * interval '1 microsecond'
          WHERE scheduler_lock.expires_at < statement_timestamp()
             OR scheduler_lock.owner_node = ?
          """;
      int updated =
          ctx.em()
              .createNativeQuery(sql)
              .setParameter(1, name)
              .setParameter(2, nodeId)
              .setParameter(3, ttlMicros)
              .setParameter(4, ttlMicros)
              .setParameter(5, nodeId)
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
      // language=PostgreSQL
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
      // language=PostgreSQL
      String sql =
          """
          UPDATE scheduler_lock
          SET expires_at = statement_timestamp() + ? * interval '1 microsecond'
          WHERE lock_name = ? AND owner_node = ?
          """;
      int updated =
          ctx.em()
              .createNativeQuery(sql)
              .setParameter(1, extensionMicros)
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
    Objects.requireNonNull(nodeId, "nodeId");
    Objects.requireNonNull(ts, "ts");
    try {
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
      // language=PostgreSQL
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
  public int deleteInactiveNodesSince(Instant cutoff) {
    try {
      // language=PostgreSQL
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
      // language=PostgreSQL
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
      // language=PostgreSQL
      String sql = "SELECT (EXTRACT(EPOCH FROM statement_timestamp()) * 1000)::bigint";
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
