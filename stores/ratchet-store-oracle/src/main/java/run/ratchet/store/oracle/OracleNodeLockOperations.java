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
package run.ratchet.store.oracle;

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

final class OracleNodeLockOperations implements NodeStore, LockStore {

  private static final int MAX_INACTIVE_NODES = 1000;

  private final OracleStoreContext ctx;

  OracleNodeLockOperations(OracleStoreContext ctx) {
    this.ctx = ctx;
  }

  @Override
  public boolean tryLock(String name, Duration ttl, String nodeId) {
    /*
     * Transaction contract: OracleJobStoreImpl invokes this under REQUIRED, keeping the UPDATE and
     * fallback INSERT IGNORE in one transaction. The INSERT row count still decides the race.
     */
    try {
      requireLockName(name);
      requirePositiveDuration(ttl, "ttl");
      Objects.requireNonNull(nodeId, "nodeId");
      long ttlMicros = durationMicros(ttl);
      // language=Oracle
      String updateSql =
          """
          UPDATE scheduler_lock
          SET owner_node = ?,
              locked_at = CASE
                WHEN locked_at = CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS TIMESTAMP) THEN locked_at + INTERVAL '0.000001' SECOND
                ELSE CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS TIMESTAMP)
              END,
              expires_at = CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS TIMESTAMP) + NUMTODSINTERVAL(? / 1000000, 'SECOND')
          WHERE lock_name = ?
            AND (expires_at < CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS TIMESTAMP) OR owner_node = ?)
          """;
      int updated =
          ctx.em()
              .createNativeQuery(updateSql)
              .setParameter(1, nodeId)
              .setParameter(2, ttlMicros)
              .setParameter(3, name)
              .setParameter(4, nodeId)
              .executeUpdate();
      if (updated > 0) {
        return true;
      }

      // IGNORE_ROW_ON_DUPKEY_INDEX gives INSERT IGNORE semantics: a row that would violate the
      // lock_name primary key is silently skipped and executeUpdate returns 0, so the loser of a
      // concurrent create-lock race observes inserted == 0 without an exception — the row count
      // still decides the race.
      // language=Oracle
      String insertSql =
          """
          INSERT /*+ IGNORE_ROW_ON_DUPKEY_INDEX(scheduler_lock(lock_name)) */
          INTO scheduler_lock (lock_name, owner_node, locked_at, expires_at)
          VALUES (?, ?, CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS TIMESTAMP), CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS TIMESTAMP) + NUMTODSINTERVAL(? / 1000000, 'SECOND'))
          """;
      int inserted =
          ctx.em()
              .createNativeQuery(insertSql)
              .setParameter(1, name)
              .setParameter(2, nodeId)
              .setParameter(3, ttlMicros)
              .executeUpdate();
      return inserted > 0;
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("try lock", e);
    }
  }

  @Override
  public void unlock(String name, String nodeId) {
    try {
      requireLockName(name);
      Objects.requireNonNull(nodeId, "nodeId");
      // language=Oracle
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
      // language=Oracle
      String sql =
          """
          UPDATE scheduler_lock SET expires_at = CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS TIMESTAMP) + NUMTODSINTERVAL(? / 1000000, 'SECOND')
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
      // language=Oracle
      String sql =
          """
          MERGE INTO scheduler_node d
          USING (SELECT ? AS node_id, ? AS heartbeat_ts, ? AS started_at FROM dual) s
          ON (d.node_id = s.node_id)
          WHEN MATCHED THEN UPDATE SET d.heartbeat_ts = s.heartbeat_ts
          WHEN NOT MATCHED THEN INSERT (node_id, heartbeat_ts, started_at)
            VALUES (s.node_id, s.heartbeat_ts, s.started_at)
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
      // language=Oracle
      String sql = "SELECT * FROM scheduler_node WHERE heartbeat_ts < ? FETCH FIRST ? ROWS ONLY";
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
      // language=Oracle
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
      // language=Oracle
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
      // Epoch millis from the UTC wall clock. EXTRACT over the interval since the epoch keeps the
      // computation session-time-zone independent (no UNIX_TIMESTAMP equivalent on Oracle).
      // language=Oracle
      String sql =
          """
          SELECT ROUND(
                   (EXTRACT(DAY FROM d) * 86400
                    + EXTRACT(HOUR FROM d) * 3600
                    + EXTRACT(MINUTE FROM d) * 60
                    + EXTRACT(SECOND FROM d)) * 1000)
          FROM (SELECT CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS TIMESTAMP)
                       - TIMESTAMP '1970-01-01 00:00:00' AS d FROM dual)
          """;
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
