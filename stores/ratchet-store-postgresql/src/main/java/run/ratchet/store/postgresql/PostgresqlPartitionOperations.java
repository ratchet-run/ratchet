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

import jakarta.persistence.Query;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import run.ratchet.store.dto.PartitionGenerationWrite;
import run.ratchet.store.entity.PartitionEntity;
import run.ratchet.store.entity.PartitionOutcome;
import run.ratchet.store.entity.PartitionSetEntity;
import run.ratchet.store.spi.PartitionStore;
import run.ratchet.store.util.RowValues;

/** Native partition persistence, joining the caller's transaction. */
final class PostgresqlPartitionOperations implements PartitionStore {
  private static final String SET_COLUMNS =
      "set_id, set_key, name, partition_count, latest_generation, latest_batch_id,"
          + " sealed_generation, generation_started_at, owner_principal, version, created_at,"
          + " updated_at";
  private static final String PARTITION_COLUMNS =
      "set_id, partition_index, range_from, range_to, generation, child_job_id, outcome,"
          + " last_error, checkpoint_json";
  private final PostgresqlStoreContext ctx;
  private final PostgresqlJobCrudOperations jobs;
  private final PostgresqlBatchOperations batches;

  PostgresqlPartitionOperations(
      PostgresqlStoreContext ctx,
      PostgresqlJobCrudOperations jobs,
      PostgresqlBatchOperations batches) {
    this.ctx = ctx;
    this.jobs = jobs;
    this.batches = batches;
  }

  private Object id(UUID value) {
    return value;
  }

  private Query query(String sql, Object... values) {
    Query query = ctx.em().createNativeQuery(sql);
    for (int i = 0; i < values.length; i++) query.setParameter(i + 1, values[i]);
    return query;
  }

  private int update(String sql, Object... values) {
    return query(sql, values).executeUpdate();
  }

  @Override
  public Optional<PartitionSetEntity> findPartitionSet(String setKey) {
    return findSet("set_key = ?", setKey);
  }

  @Override
  public Optional<PartitionSetEntity> findPartitionSetById(UUID setId) {
    return findSet("set_id = ?", id(setId));
  }

  private Optional<PartitionSetEntity> findSet(String predicate, Object value) {
    List<?> rows =
        query("SELECT " + SET_COLUMNS + " FROM scheduler_partition_set WHERE " + predicate, value)
            .getResultList();
    return rows.isEmpty() ? Optional.empty() : Optional.of(mapSet((Object[]) rows.get(0)));
  }

  @Override
  public boolean createOrAdvance(PartitionGenerationWrite write) {
    try {
      jobs.reservePartitionParentKey(write.parent());
      PartitionSetEntity set = write.set();
      if (write.expectedVersion() == null) {
        int inserted =
            update(
                """
                INSERT INTO scheduler_partition_set (set_id, set_key, name, partition_count, latest_generation, latest_batch_id, sealed_generation, generation_started_at, owner_principal, version, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)
                ON CONFLICT (set_key) DO NOTHING
                """,
                id(set.getSetId()),
                set.getSetKey(),
                set.getName(),
                set.getPartitionCount(),
                set.getLatestGeneration(),
                id(set.getLatestBatchId()),
                set.getSealedGeneration(),
                Timestamp.from(set.getGenerationStartedAt()),
                set.getOwnerPrincipal(),
                Timestamp.from(set.getCreatedAt()),
                Timestamp.from(set.getUpdatedAt()));
        if (inserted == 0) return false;
        for (PartitionEntity row : write.rows()) {
          update(
              """
              INSERT INTO scheduler_partition (set_id, partition_index, range_from, range_to, generation, child_job_id)
              VALUES (?, ?, ?, ?, ?, ?)
              """,
              id(set.getSetId()),
              row.getPartitionIndex(),
              row.getRangeFrom(),
              row.getRangeTo(),
              row.getGeneration(),
              id(row.getChildJobId()));
          // Bind opaque large text only in a plain UPDATE SET, including Oracle CLOBs.
          update(
              "UPDATE scheduler_partition SET outcome = ?, last_error = ?, checkpoint_json = ?"
                  + " WHERE set_id = ? AND partition_index = ?",
              row.getOutcome() == null ? null : row.getOutcome().name(),
              row.getLastError(),
              row.getCheckpoint(),
              id(set.getSetId()),
              row.getPartitionIndex());
        }
      } else {
        int changed =
            update(
                """
                UPDATE scheduler_partition_set
                SET latest_generation = ?, latest_batch_id = ?, generation_started_at = ?, updated_at = ?, version = version + 1
                WHERE set_id = ?
                AND version = ?
                """,
                set.getLatestGeneration(),
                id(set.getLatestBatchId()),
                Timestamp.from(set.getGenerationStartedAt()),
                Timestamp.from(set.getUpdatedAt()),
                id(set.getSetId()),
                write.expectedVersion());
        if (changed == 0) return false;
        for (PartitionEntity row : write.rows()) {
          update(
              """
              UPDATE scheduler_partition
              SET generation = ?, child_job_id = ?, outcome = NULL, last_error = NULL, checkpoint_json = ?
              WHERE set_id = ?
              AND partition_index = ?
              """,
              row.getGeneration(),
              id(row.getChildJobId()),
              row.getCheckpoint(),
              id(set.getSetId()),
              row.getPartitionIndex());
        }
      }
      jobs.createWithReservedIdempotencyKey(write.parent());
      batches.saveBatch(write.batch());
      return true;
    } catch (RuntimeException ex) {
      throw ctx.translateTransientStoreException("create or advance partition generation", ex);
    }
  }

  @Override
  public List<PartitionEntity> findPartitions(UUID setId, int afterIndex, int limit) {
    return page(setId, null, afterIndex, limit);
  }

  @Override
  public List<PartitionEntity> findGenerationPartitions(
      UUID setId, int generation, int afterIndex, int limit) {
    return page(setId, generation, afterIndex, limit);
  }

  private List<PartitionEntity> page(UUID setId, Integer generation, int afterIndex, int limit) {
    if (limit <= 0) throw new IllegalArgumentException("limit must be positive");
    String sql =
        "SELECT "
            + PARTITION_COLUMNS
            + " FROM scheduler_partition WHERE set_id = ? AND partition_index > ?"
            + (generation == null ? "" : " AND generation = ?")
            + " ORDER BY partition_index";
    Query q = query(sql, id(setId), afterIndex);
    if (generation != null) q.setParameter(3, generation);
    List<?> rows = q.setMaxResults(limit).getResultList();
    List<PartitionEntity> result = new ArrayList<>();
    for (Object row : rows) result.add(mapPartition((Object[]) row));
    return result;
  }

  @Override
  public int sealPartitions(UUID setId, int generation, List<PartitionEntity> outcomes) {
    int changed = 0;
    for (PartitionEntity row : outcomes) {
      changed +=
          update(
              """
              UPDATE scheduler_partition
              SET outcome = ?, last_error = ?, checkpoint_json = ?
              WHERE set_id = ?
              AND partition_index = ?
              AND generation = ?
              AND child_job_id = ?
              AND EXISTS (SELECT 1 FROM scheduler_partition_set s
                WHERE s.set_id = ? AND s.sealed_generation < ?)
              """,
              row.getOutcome() == null ? null : row.getOutcome().name(),
              row.getLastError(),
              row.getCheckpoint(),
              id(setId),
              row.getPartitionIndex(),
              generation,
              id(row.getChildJobId()),
              id(setId),
              generation);
    }
    return changed;
  }

  @Override
  public boolean markSealed(UUID setId, int generation) {
    return update(
            "UPDATE scheduler_partition_set SET sealed_generation = ?, updated_at = ? WHERE set_id"
                + " = ? AND sealed_generation < ?",
            generation,
            Timestamp.from(Instant.now()),
            id(setId),
            generation)
        > 0;
  }

  @Override
  public List<PartitionSetEntity> findUnsealedSets(UUID afterSetId, int limit) {
    if (limit <= 0) throw new IllegalArgumentException("limit must be positive");
    String sql =
        "SELECT "
            + SET_COLUMNS
            + " FROM scheduler_partition_set WHERE sealed_generation < latest_generation"
            + (afterSetId == null ? "" : " AND set_id > ?")
            + " ORDER BY set_id";
    List<?> rows =
        (afterSetId == null ? query(sql) : query(sql, id(afterSetId)))
            .setMaxResults(limit)
            .getResultList();
    List<PartitionSetEntity> result = new ArrayList<>();
    for (Object row : rows) result.add(mapSet((Object[]) row));
    return result;
  }

  @Override
  public Optional<PartitionEntity> findPartitionByChild(UUID childJobId) {
    List<?> rows =
        query(
                "SELECT " + PARTITION_COLUMNS + " FROM scheduler_partition WHERE child_job_id = ?",
                id(childJobId))
            .setMaxResults(1)
            .getResultList();
    return rows.isEmpty() ? Optional.empty() : Optional.of(mapPartition((Object[]) rows.get(0)));
  }

  @Override
  public long countBatchChildren(UUID batchId) {
    return countChildren(batchId, false);
  }

  @Override
  public long countUnfinishedBatchChildren(UUID batchId) {
    return countChildren(batchId, true);
  }

  private long countChildren(UUID batchId, boolean unfinished) {
    String sql =
        "SELECT COUNT(*) FROM scheduler_job WHERE depends_on = ? AND job_type = 'BATCH_CHILD'"
            + (unfinished
                ? " AND (terminal_status IS NULL OR terminal_status NOT IN ('SUCCEEDED', 'FAILED',"
                    + " 'CANCELED'))"
                : "");
    return ((Number) query(sql, id(batchId)).getSingleResult()).longValue();
  }

  @Override
  public boolean deletePartitionSet(UUID setId) {
    return update(
            "DELETE FROM scheduler_partition_set WHERE set_id = ? AND sealed_generation ="
                + " latest_generation",
            id(setId))
        > 0;
  }

  private PartitionSetEntity mapSet(Object[] row) {
    PartitionSetEntity set = new PartitionSetEntity();
    set.setSetId(RowValues.uuidOrNull(row[0]));
    set.setSetKey(RowValues.stringOrNull(row[1]));
    set.setName(RowValues.stringOrNull(row[2]));
    set.setPartitionCount(((Number) row[3]).intValue());
    set.setLatestGeneration(((Number) row[4]).intValue());
    set.setLatestBatchId(RowValues.uuidOrNull(row[5]));
    set.setSealedGeneration(((Number) row[6]).intValue());
    set.setGenerationStartedAt(RowValues.instantOrNull(row[7]));
    set.setOwnerPrincipal(RowValues.stringOrNull(row[8]));
    set.setVersion(((Number) row[9]).intValue());
    set.setCreatedAt(RowValues.instantOrNull(row[10]));
    set.setUpdatedAt(RowValues.instantOrNull(row[11]));
    return set;
  }

  private PartitionEntity mapPartition(Object[] row) {
    PartitionEntity part = new PartitionEntity();
    part.setSetId(RowValues.uuidOrNull(row[0]));
    part.setPartitionIndex(((Number) row[1]).intValue());
    part.setRangeFrom(((Number) row[2]).longValue());
    part.setRangeTo(((Number) row[3]).longValue());
    part.setGeneration(((Number) row[4]).intValue());
    part.setChildJobId(RowValues.uuidOrNull(row[5]));
    part.setOutcome(
        row[6] == null ? null : PartitionOutcome.valueOf(RowValues.stringOrNull(row[6])));
    part.setLastError(RowValues.stringOrNull(row[7]));
    part.setCheckpoint(RowValues.stringOrNull(row[8]));
    return part;
  }
}
