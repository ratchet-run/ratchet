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
package run.ratchet.store.mongodb;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.regex;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.inc;
import static com.mongodb.client.model.Updates.set;
import static run.ratchet.store.mongodb.MongoFieldNames.ENCRYPTED_STATE;
import static run.ratchet.store.mongodb.MongoFieldNames.ENCRYPTION_KEY_ID;
import static run.ratchet.store.mongodb.MongoFieldNames.JOB_ID;
import static run.ratchet.store.mongodb.MongoFieldNames.NAMESPACE;
import static run.ratchet.store.mongodb.MongoFieldNames.PROPERTY_KEY;
import static run.ratchet.store.mongodb.MongoFieldNames.STATE;
import static run.ratchet.store.mongodb.MongoFieldNames.UPDATED_AT;
import static run.ratchet.store.mongodb.MongoFieldNames.VALUE;
import static run.ratchet.store.mongodb.MongoFieldNames.VERSION;
import static run.ratchet.store.util.ExtensionValidation.requireKey;
import static run.ratchet.store.util.ExtensionValidation.requireNamespace;
import static run.ratchet.store.util.ExtensionValidation.requireState;
import static run.ratchet.store.util.ExtensionValidation.requireValue;

import com.mongodb.MongoException;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Pattern;
import org.bson.Document;
import run.ratchet.store.converter.EncryptionHolder;
import run.ratchet.store.spi.JobExtensionStore;
import run.ratchet.store.util.EncryptionTarget;
import run.ratchet.store.util.JobEncryption;
import run.ratchet.store.util.PayloadEncryptor;

/**
 * MongoDB implementation of {@link JobExtensionStore}.
 *
 * <p>Properties and extension state live in their own collections ({@code scheduler_job_properties}
 * and {@code scheduler_job_extension_state}), NOT as sub-documents on the job document: the job
 * save path is a whole-document {@code replaceOne}, which would silently wipe embedded extension
 * data on every job update. Separate collections also mirror the SQL table shape one-to-one.
 *
 * <p>Property values are plaintext by design (indexed for the query layer). Extension-state blobs
 * follow the deployment-wide payload-encryption switch: when it is on, the {@code state} field
 * holds a ciphertext frame bound to {@code (job_id, namespace)}; otherwise plaintext with {@code
 * encrypted_state = false}.
 */
final class MongoExtensionOperations implements JobExtensionStore {

  private final MongoStoreContext ctx;

  MongoExtensionOperations(MongoStoreContext ctx) {
    this.ctx = ctx;
  }

  @Override
  public void putProperty(UUID jobId, String key, String value) {
    requireKey(key);
    requireValue(value);
    ctx.jobProperties()
        .updateOne(
            and(eq(JOB_ID, jobId), eq(PROPERTY_KEY, key)),
            combine(set(VALUE, value), set(JOB_ID, jobId), set(PROPERTY_KEY, key)),
            new UpdateOptions().upsert(true));
  }

  @Override
  public Optional<String> getProperty(UUID jobId, String key) {
    requireKey(key);
    Document doc = ctx.jobProperties().find(and(eq(JOB_ID, jobId), eq(PROPERTY_KEY, key))).first();
    if (doc == null) {
      return Optional.empty();
    }
    return Optional.ofNullable(doc.getString(VALUE));
  }

  @Override
  public Map<String, String> getPropertiesByPrefix(UUID jobId, String prefix) {
    if (prefix == null) {
      throw new IllegalArgumentException("prefix must not be null");
    }
    Map<String, String> result = new LinkedHashMap<>();
    var filter =
        prefix.isEmpty()
            ? eq(JOB_ID, jobId)
            : and(eq(JOB_ID, jobId), regex(PROPERTY_KEY, "^" + Pattern.quote(prefix)));
    for (Document doc : ctx.jobProperties().find(filter).sort(new Document(PROPERTY_KEY, 1))) {
      String value = doc.getString(VALUE);
      if (value != null) {
        result.put(doc.getString(PROPERTY_KEY), value);
      }
    }
    return result;
  }

  @Override
  public Optional<ExtensionState> getState(UUID jobId, String namespace) {
    requireNamespace(namespace);
    Document doc =
        ctx.jobExtensionState().find(and(eq(JOB_ID, jobId), eq(NAMESPACE, namespace))).first();
    if (doc == null) {
      return Optional.empty();
    }
    String json =
        PayloadEncryptor.decryptValue(
            doc.getString(STATE), EncryptionTarget.extensionState(jobId, namespace));
    return Optional.of(new ExtensionState(json, doc.getInteger(VERSION, 0)));
  }

  @Override
  public void initState(UUID jobId, String namespace, String initialState) {
    requireNamespace(namespace);
    requireState(initialState);
    boolean active = EncryptionHolder.encryptionActiveFor(false);
    String stored =
        PayloadEncryptor.encryptValue(
            initialState, active, EncryptionTarget.extensionState(jobId, namespace));
    Document doc =
        new Document(JOB_ID, jobId)
            .append(NAMESPACE, namespace)
            .append(STATE, stored)
            .append(ENCRYPTED_STATE, active)
            .append(ENCRYPTION_KEY_ID, JobEncryption.keyId(active))
            .append(VERSION, 0)
            .append(UPDATED_AT, DocumentMapper.toDate(Instant.now()));
    try {
      ctx.jobExtensionState().insertOne(doc);
    } catch (MongoException e) {
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
    UpdateResult result =
        ctx.jobExtensionState()
            .updateOne(
                and(eq(JOB_ID, jobId), eq(NAMESPACE, namespace), eq(VERSION, expectedVersion)),
                combine(
                    set(STATE, stored),
                    set(ENCRYPTED_STATE, active),
                    set(ENCRYPTION_KEY_ID, JobEncryption.keyId(active)),
                    inc(VERSION, 1),
                    set(UPDATED_AT, DocumentMapper.toDate(Instant.now()))));
    return result.getModifiedCount() > 0;
  }
}
