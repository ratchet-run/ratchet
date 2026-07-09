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

import static run.ratchet.store.util.ExtensionValidation.escapeLike;
import static run.ratchet.store.util.ExtensionValidation.requireKey;
import static run.ratchet.store.util.ExtensionValidation.requireNamespace;
import static run.ratchet.store.util.ExtensionValidation.requireState;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import run.ratchet.store.converter.EncryptionHolder;
import run.ratchet.store.spi.JobExtensionStore;
import run.ratchet.store.sqlserver.converter.UuidByteArrayConverter;
import run.ratchet.store.util.EncryptionTarget;
import run.ratchet.store.util.JobEncryption;
import run.ratchet.store.util.PayloadEncryptor;
import run.ratchet.store.util.RowValues;

/**
 * SQL Server implementation of {@link JobExtensionStore}.
 *
 * <p>Property values are plaintext by design (indexed for the query layer). Extension-state blobs
 * follow the deployment-wide payload-encryption switch: when it is on, the {@code state} column
 * holds a ciphertext frame bound to {@code (job_id, namespace)}; otherwise plaintext with {@code
 * encrypted_state = 0}.
 */
final class SqlserverExtensionOperations implements JobExtensionStore {

  private final SqlserverStoreContext ctx;

  SqlserverExtensionOperations(SqlserverStoreContext ctx) {
    this.ctx = ctx;
  }

  @Override
  public void putProperty(UUID jobId, String key, String value) {
    requireKey(key);
    // language=SQL Server
    String sql =
        """
        MERGE scheduler_job_properties AS t
        USING (VALUES (?, ?, ?)) AS s(job_id, property_key, value)
          ON t.job_id = s.job_id AND t.property_key = s.property_key
        WHEN MATCHED THEN UPDATE SET t.value = s.value
        WHEN NOT MATCHED THEN INSERT (job_id, property_key, value)
          VALUES (s.job_id, s.property_key, s.value);
        """;
    ctx.em()
        .createNativeQuery(sql)
        .setParameter(1, UuidByteArrayConverter.toBytes(jobId))
        .setParameter(2, key)
        .setParameter(3, value)
        .executeUpdate();
  }

  @Override
  public Optional<String> getProperty(UUID jobId, String key) {
    requireKey(key);
    // language=SQL Server
    String sql =
        """
        SELECT value
        FROM scheduler_job_properties
        WHERE job_id = ? AND property_key = ?
        """;
    @SuppressWarnings("unchecked")
    List<Object> rows =
        ctx.em()
            .createNativeQuery(sql)
            .setParameter(1, UuidByteArrayConverter.toBytes(jobId))
            .setParameter(2, key)
            .getResultList();
    if (rows.isEmpty()) {
      return Optional.empty();
    }
    return Optional.ofNullable(RowValues.stringOrNull(rows.get(0)));
  }

  @Override
  public Map<String, String> getPropertiesByPrefix(UUID jobId, String prefix) {
    if (prefix == null) {
      throw new IllegalArgumentException("prefix must not be null");
    }
    // language=SQL Server
    String sql =
        """
        SELECT property_key, value
        FROM scheduler_job_properties
        WHERE job_id = ? AND property_key LIKE ? ESCAPE '\\'
        ORDER BY property_key
        """;
    @SuppressWarnings("unchecked")
    List<Object[]> rows =
        ctx.em()
            .createNativeQuery(sql)
            .setParameter(1, UuidByteArrayConverter.toBytes(jobId))
            .setParameter(2, escapeLike(prefix) + "%")
            .getResultList();
    Map<String, String> result = new LinkedHashMap<>();
    for (Object[] row : rows) {
      String value = RowValues.stringOrNull(row[1]);
      if (value != null) {
        result.put((String) row[0], value);
      }
    }
    return result;
  }

  @Override
  public Optional<ExtensionState> getState(UUID jobId, String namespace) {
    requireNamespace(namespace);
    // language=SQL Server
    String sql =
        """
        SELECT state, version
        FROM scheduler_job_extension_state
        WHERE job_id = ? AND namespace = ?
        """;
    @SuppressWarnings("unchecked")
    List<Object[]> rows =
        ctx.em()
            .createNativeQuery(sql)
            .setParameter(1, UuidByteArrayConverter.toBytes(jobId))
            .setParameter(2, namespace)
            .getResultList();
    if (rows.isEmpty()) {
      return Optional.empty();
    }
    Object[] row = rows.get(0);
    String stored = RowValues.stringOrNull(row[0]);
    String json =
        PayloadEncryptor.decryptValue(stored, EncryptionTarget.extensionState(jobId, namespace));
    return Optional.of(new ExtensionState(json, ((Number) row[1]).intValue()));
  }

  @Override
  public void initState(UUID jobId, String namespace, String initialState) {
    requireNamespace(namespace);
    requireState(initialState);
    boolean active = EncryptionHolder.encryptionActiveFor(false);
    String stored =
        PayloadEncryptor.encryptValue(
            initialState, active, EncryptionTarget.extensionState(jobId, namespace));
    // language=SQL Server
    String sql =
        """
        INSERT INTO scheduler_job_extension_state
          (job_id, namespace, state, encrypted_state, encryption_key_id, version, updated_at)
        VALUES (?, ?, ?, ?, ?, 0, ?)
        """;
    try {
      ctx.em()
          .createNativeQuery(sql)
          .setParameter(1, UuidByteArrayConverter.toBytes(jobId))
          .setParameter(2, namespace)
          .setParameter(3, stored)
          .setParameter(4, active)
          .setParameter(5, JobEncryption.keyId(active))
          .setParameter(6, Timestamp.from(Instant.now()))
          .executeUpdate();
    } catch (RuntimeException e) {
      if (ctx.constraintDetector().isDuplicateKey(e)) {
        throw new IllegalStateException(
            "extension state already initialized for job " + jobId + " namespace " + namespace, e);
      }
      throw e;
    }
  }

  @Override
  public boolean updateState(UUID jobId, String namespace, String newState, int expectedVersion) {
    requireNamespace(namespace);
    requireState(newState);
    if (expectedVersion < 0) {
      throw new IllegalArgumentException(
          "expectedVersion must be non-negative: " + expectedVersion);
    }
    boolean active = EncryptionHolder.encryptionActiveFor(false);
    String stored =
        PayloadEncryptor.encryptValue(
            newState, active, EncryptionTarget.extensionState(jobId, namespace));
    // language=SQL Server
    String sql =
        """
        UPDATE scheduler_job_extension_state
        SET state = ?, encrypted_state = ?, encryption_key_id = ?,
            version = version + 1, updated_at = ?
        WHERE job_id = ? AND namespace = ? AND version = ?
        """;
    int updated =
        ctx.em()
            .createNativeQuery(sql)
            .setParameter(1, stored)
            .setParameter(2, active)
            .setParameter(3, JobEncryption.keyId(active))
            .setParameter(4, Timestamp.from(Instant.now()))
            .setParameter(5, UuidByteArrayConverter.toBytes(jobId))
            .setParameter(6, namespace)
            .setParameter(7, expectedVersion)
            .executeUpdate();
    return updated > 0;
  }
}
