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
package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.json.Json;
import jakarta.json.JsonObject;
import java.io.StringReader;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobStatus;
import run.ratchet.store.converter.EncryptionHolder;
import run.ratchet.store.spi.JobExtensionStore.ExtensionState;
import run.ratchet.store.util.EncryptionEnvelope;
import run.ratchet.store.util.EncryptionTarget;
import run.ratchet.store.util.PayloadEncryptor;
import run.ratchet.tck.store.AbstractPayloadEncryptionStoreContract.RecordingEngine;
import run.ratchet.tck.store.AbstractPayloadEncryptionStoreContract.SingleKeyProvider;

/**
 * Base contract tests for {@code JobExtensionStore}: write-once indexed properties and mutable
 * per-namespace extension state with optimistic CAS.
 *
 * <p>The encryption tests install a recording engine into the static {@link EncryptionHolder} (the
 * same seam the framework's installer uses at startup) and prove the state blob crosses the
 * encryption seam on write and round-trips on read; the plaintext tests run with the holder
 * disabled and assert the plaintext posture.
 */
public abstract class AbstractJobExtensionStoreContract implements JobStoreContractFixture {

  private static final String NAMESPACE = "ratchet-tck";

  @BeforeEach
  @AfterEach
  void cleanupExtensionFixture() {
    EncryptionHolder.disable();
    cleanupStore();
  }

  // ---- Properties ----

  @Test
  void putProperty_roundTripsThroughGetProperty() {
    var job = persist(newPendingJob());

    extensionStore().putProperty(job.getId(), "ratchet-tck.block_name", "invoice.send");

    assertEquals(
        "invoice.send",
        extensionStore().getProperty(job.getId(), "ratchet-tck.block_name").orElseThrow());
  }

  @Test
  void putProperty_rejectsKeyLongerThan255Characters() {
    var job = persist(newPendingJob());

    assertThrows(
        IllegalArgumentException.class,
        () -> extensionStore().putProperty(job.getId(), "x".repeat(256), "value"));
  }

  @Test
  void putProperty_rejectsValueLongerThan1024Characters() {
    var job = persist(newPendingJob());

    assertThrows(
        IllegalArgumentException.class,
        () -> extensionStore().putProperty(job.getId(), "ratchet-tck.value", "x".repeat(1025)));
  }

  @Test
  void putProperty_roundTrips255CharacterKey() {
    var job = persist(newPendingJob());
    String key = "x".repeat(255);

    extensionStore().putProperty(job.getId(), key, "value");

    assertEquals("value", extensionStore().getProperty(job.getId(), key).orElseThrow());
  }

  @Test
  void putProperty_roundTrips1024CharacterValue() {
    var job = persist(newPendingJob());
    String value = "x".repeat(1024);

    extensionStore().putProperty(job.getId(), "ratchet-tck.value", value);

    assertEquals(
        value, extensionStore().getProperty(job.getId(), "ratchet-tck.value").orElseThrow());
  }

  @Test
  void putProperty_replacesExistingValueForSameKey() {
    var job = persist(newPendingJob());

    extensionStore().putProperty(job.getId(), "ratchet-tck.block_version", "1");
    extensionStore().putProperty(job.getId(), "ratchet-tck.block_version", "2");

    assertEquals(
        "2", extensionStore().getProperty(job.getId(), "ratchet-tck.block_version").orElseThrow());
  }

  @Test
  void getProperty_returnsEmptyWhenAbsent() {
    var job = persist(newPendingJob());

    assertTrue(extensionStore().getProperty(job.getId(), "ratchet-tck.missing").isEmpty());
    assertTrue(
        extensionStore().getProperty(new UUID(0L, 424_242L), "ratchet-tck.missing").isEmpty());
  }

  @Test
  void getPropertiesByPrefix_returnsOnlyMatchingNamespace() {
    var job = persist(newPendingJob());
    extensionStore().putProperty(job.getId(), "ratchet-tck.block_name", "invoice.send");
    extensionStore().putProperty(job.getId(), "ratchet-tck.block_version", "2");
    extensionStore().putProperty(job.getId(), "other-ext.key", "ignored");

    Map<String, String> scoped =
        extensionStore().getPropertiesByPrefix(job.getId(), "ratchet-tck.");

    assertEquals(
        Map.of("ratchet-tck.block_name", "invoice.send", "ratchet-tck.block_version", "2"), scoped);
  }

  @Test
  void getPropertiesByPrefix_emptyPrefixReturnsEverything() {
    var job = persist(newPendingJob());
    extensionStore().putProperty(job.getId(), "a.one", "1");
    extensionStore().putProperty(job.getId(), "b.two", "2");

    assertEquals(2, extensionStore().getPropertiesByPrefix(job.getId(), "").size());
  }

  @Test
  void getPropertiesByPrefix_treatsLikeMetacharactersLiterally() {
    var job = persist(newPendingJob());
    extensionStore().putProperty(job.getId(), "pct%key.x", "match");
    extensionStore().putProperty(job.getId(), "pctXkey.x", "no-match");

    Map<String, String> scoped = extensionStore().getPropertiesByPrefix(job.getId(), "pct%");

    assertEquals(Map.of("pct%key.x", "match"), scoped);
  }

  @Test
  void properties_areScopedToTheOwningJob() {
    var first = persist(newPendingJob());
    var second = persist(newPendingJob());
    extensionStore().putProperty(first.getId(), "ratchet-tck.block_name", "invoice.send");

    assertTrue(extensionStore().getProperty(second.getId(), "ratchet-tck.block_name").isEmpty());
  }

  // ---- Extension state ----

  @Test
  void initState_createsRowAtVersionZero() {
    var job = persist(newPendingJob());

    extensionStore().initState(job.getId(), NAMESPACE, "{\"steps\":{}}");

    ExtensionState state = extensionStore().getState(job.getId(), NAMESPACE).orElseThrow();
    assertEquals("{\"steps\":{}}", state.json());
    assertEquals(0, state.version());
  }

  @Test
  void initState_rejectsNamespaceLongerThan64Characters() {
    var job = persist(newPendingJob());

    assertThrows(
        IllegalArgumentException.class,
        () -> extensionStore().initState(job.getId(), "x".repeat(65), "{}"));
  }

  @Test
  void initState_failsLoudWhenAlreadyInitialized() {
    var job = persist(newPendingJob());
    extensionStore().initState(job.getId(), NAMESPACE, "{}");

    assertThrows(
        IllegalStateException.class,
        () -> extensionStore().initState(job.getId(), NAMESPACE, "{}"));
  }

  @Test
  void getState_returnsEmptyWhenAbsent() {
    var job = persist(newPendingJob());

    assertTrue(extensionStore().getState(job.getId(), NAMESPACE).isEmpty());
  }

  @Test
  void updateState_appliesOnMatchingVersionAndIncrements() {
    var job = persist(newPendingJob());
    extensionStore().initState(job.getId(), NAMESPACE, "{\"v\":0}");

    assertTrue(extensionStore().updateState(job.getId(), NAMESPACE, "{\"v\":1}", 0));

    ExtensionState state = extensionStore().getState(job.getId(), NAMESPACE).orElseThrow();
    assertEquals("{\"v\":1}", state.json());
    assertEquals(1, state.version());
  }

  @Test
  void updateState_rejectsStaleVersionAndLeavesStateUntouched() {
    var job = persist(newPendingJob());
    extensionStore().initState(job.getId(), NAMESPACE, "{\"v\":0}");
    assertTrue(extensionStore().updateState(job.getId(), NAMESPACE, "{\"v\":1}", 0));

    assertFalse(
        extensionStore().updateState(job.getId(), NAMESPACE, "{\"v\":-1}", 0),
        "a stale expectedVersion must not apply");

    ExtensionState state = extensionStore().getState(job.getId(), NAMESPACE).orElseThrow();
    assertEquals("{\"v\":1}", state.json());
    assertEquals(1, state.version());
  }

  @Test
  void updateState_returnsFalseWhenRowAbsent() {
    var job = persist(newPendingJob());

    assertFalse(extensionStore().updateState(job.getId(), NAMESPACE, "{}", 0));
  }

  @Test
  void state_isScopedPerNamespace() {
    var job = persist(newPendingJob());
    extensionStore().initState(job.getId(), "ns-one", "{\"a\":1}");
    extensionStore().initState(job.getId(), "ns-two", "{\"b\":2}");
    assertTrue(extensionStore().updateState(job.getId(), "ns-one", "{\"a\":2}", 0));

    assertEquals(
        "{\"a\":2}", extensionStore().getState(job.getId(), "ns-one").orElseThrow().json());
    assertEquals(
        "{\"b\":2}", extensionStore().getState(job.getId(), "ns-two").orElseThrow().json());
    assertEquals(0, extensionStore().getState(job.getId(), "ns-two").orElseThrow().version());
  }

  // ---- Encryption posture ----

  @Test
  void state_crossesEncryptionSeamWhenEngineInstalled() {
    var job = persist(newPendingJob());
    RecordingEngine engine = new RecordingEngine();
    EncryptionHolder.install(
        List.of(engine), RecordingEngine.ALGORITHM_ID, new SingleKeyProvider(), true);

    extensionStore().initState(job.getId(), NAMESPACE, "{\"secret\":\"s3cr3t\"}");
    assertTrue(extensionStore().updateState(job.getId(), NAMESPACE, "{\"secret\":\"upd4ted\"}", 0));

    // Round-trip: decrypted blob and CAS version come back intact.
    ExtensionState state = extensionStore().getState(job.getId(), NAMESPACE).orElseThrow();
    assertEquals("{\"secret\":\"upd4ted\"}", state.json());
    assertEquals(1, state.version());

    // The engine actually saw both writes — the blob was encrypted before it reached the store,
    // not merely round-tripped as plaintext.
    assertTrue(engine.encryptedPlaintexts.contains("{\"secret\":\"s3cr3t\"}"));
    assertTrue(engine.encryptedPlaintexts.contains("{\"secret\":\"upd4ted\"}"));
    assertTrue(engine.decryptCount.get() > 0, "read path must route through the engine");
  }

  // ---- Archive retention ----

  @Test
  void archiveJob_copiesPropertiesAndExtensionStateOntoArchiveRow() {
    var job = persist(newPendingJob());
    extensionStore().putProperty(job.getId(), "ratchet-tck.block_name", "invoice.send");
    extensionStore().initState(job.getId(), NAMESPACE, "{\"steps\":{}}");
    store().compareAndSwapStatus(job.getId(), JobStatus.PENDING, JobStatus.RUNNING, null);
    store()
        .markJobSucceeded(
            job.getId(), null, null, Instant.EPOCH, Instant.EPOCH.plusSeconds(1), 100L, 50L);
    var completed = store().findById(job.getId()).orElseThrow();

    var archived = archiveStore().archiveJob(completed, "tck", "tck-node");

    assertTrue(
        archived.getProperties() != null
            && archived.getProperties().contains("ratchet-tck.block_name")
            && archived.getProperties().contains("invoice.send"),
        "archive row must carry the job's properties as JSON");
    assertTrue(
        archived.getExtensionState() != null && archived.getExtensionState().contains(NAMESPACE),
        "archive row must carry the job's extension state as JSON");
  }

  /**
   * The archive copy preserves encrypted extension state as stored: the archive JSON carries the
   * ciphertext verbatim (never the plaintext, never a re-encryption), keeps the {@code
   * encrypted_state} / {@code encryption_key_id} metadata, and the ciphertext stays decryptable
   * against the original job id and namespace — the AAD binding {@link
   * EncryptionTarget#extensionState} established on the hot row. The decrypt target is rebuilt from
   * the archive row alone ({@code original_job_id} + the entry's namespace) to prove the row is
   * self-sufficient.
   */
  @Test
  void archiveJob_preservesEncryptedExtensionStateAsDecryptableCiphertext() {
    var job = persist(newPendingJob());
    RecordingEngine engine = new RecordingEngine();
    EncryptionHolder.install(
        List.of(engine), RecordingEngine.ALGORITHM_ID, new SingleKeyProvider(), true);
    extensionStore().initState(job.getId(), NAMESPACE, "{\"secret\":\"s3cr3t\"}");
    store().compareAndSwapStatus(job.getId(), JobStatus.PENDING, JobStatus.RUNNING, null);
    store()
        .markJobSucceeded(
            job.getId(), null, null, Instant.EPOCH, Instant.EPOCH.plusSeconds(1), 100L, 50L);
    var completed = store().findById(job.getId()).orElseThrow();
    int encryptCallsBeforeArchive = engine.encryptedPlaintexts.size();
    int decryptCallsBeforeArchive = engine.decryptCount.get();

    var archived = archiveStore().archiveJob(completed, "tck", "tck-node");

    // Verbatim copy: archiving must move the ciphertext without routing it through the engine.
    assertEquals(
        encryptCallsBeforeArchive,
        engine.encryptedPlaintexts.size(),
        "archive copy must not re-encrypt the extension state");
    assertEquals(
        decryptCallsBeforeArchive,
        engine.decryptCount.get(),
        "archive copy must not decrypt the extension state");

    String json = archived.getExtensionState();
    assertNotNull(json, "archive row must carry the job's extension state as JSON");
    assertFalse(json.contains("s3cr3t"), "archive copy must stay ciphertext, never leak plaintext");

    JsonObject entry = Json.createReader(new StringReader(json)).readArray().getJsonObject(0);
    assertEquals(NAMESPACE, entry.getString("namespace"));
    assertTrue(entry.getBoolean("encrypted_state"), "archive entry must keep the encrypted flag");
    assertTrue(
        EncryptionEnvelope.isFramed(entry.getString("state")),
        "the copied state must be an envelope-framed ciphertext, not plaintext");
    assertEquals(
        "tck-key-1",
        entry.getString("encryption_key_id"),
        "archive entry must keep the key id for key-rotation accounting");

    String decrypted =
        PayloadEncryptor.decryptValue(
            entry.getString("state"),
            EncryptionTarget.extensionState(
                archived.getOriginalJobId(), entry.getString("namespace")));
    assertEquals("{\"secret\":\"s3cr3t\"}", decrypted);
  }

  @Test
  void state_isPlaintextWhenEncryptionDisabled() {
    var job = persist(newPendingJob());

    extensionStore().initState(job.getId(), NAMESPACE, "{\"plain\":true}");

    ExtensionState state = extensionStore().getState(job.getId(), NAMESPACE).orElseThrow();
    assertEquals("{\"plain\":true}", state.json());
  }
}
