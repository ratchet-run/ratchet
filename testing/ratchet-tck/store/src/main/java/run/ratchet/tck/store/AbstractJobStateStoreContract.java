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
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobStatus;
import run.ratchet.store.converter.EncryptionHolder;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.JobStateStore.StateWriteResult;
import run.ratchet.store.spi.JobStateStore.VersionedState;
import run.ratchet.store.util.EncryptionEnvelope;
import run.ratchet.store.util.EncryptionTarget;
import run.ratchet.store.util.ExtensionValidation;
import run.ratchet.store.util.PayloadEncryptor;
import run.ratchet.tck.store.AbstractPayloadEncryptionStoreContract.RecordingEngine;
import run.ratchet.tck.store.AbstractPayloadEncryptionStoreContract.SingleKeyProvider;

/** Contract tests for {@code JobStateStore}. */
public abstract class AbstractJobStateStoreContract implements JobStoreContractFixture {

  private static final String NAMESPACE = "ratchet-tck";

  @BeforeEach
  @AfterEach
  void cleanupExtensionFixture() {
    EncryptionHolder.disable();
    cleanupStore();
  }

  private JobEntity claimedStateJob() {
    persist(newPendingJob());
    return store().claimNextBatch(1, "state-owner").get(0);
  }

  @Test
  void fencedState_liveClaimAndConflictsPreserveState() {
    var job = claimedStateJob();
    long claim = job.getClaimSeq();
    assertEquals(
        StateWriteResult.UPDATED, stateStore().initState(job.getId(), NAMESPACE, "initial", claim));
    assertEquals(
        StateWriteResult.VERSION_CONFLICT,
        stateStore().initState(job.getId(), NAMESPACE, "duplicate", claim));
    assertEquals(
        StateWriteResult.VERSION_CONFLICT,
        stateStore().updateState(job.getId(), NAMESPACE, "stale", 1, claim));
    assertEquals(
        StateWriteResult.SUPERSEDED,
        stateStore().updateState(job.getId(), NAMESPACE, "wrong owner", 0, claim + 1));
    assertEquals(
        StateWriteResult.SUPERSEDED,
        stateStore().initState(job.getId(), "other", "wrong owner", claim + 1));
    assertTrue(stateStore().getState(job.getId(), "other").isEmpty());
    assertEquals(
        StateWriteResult.VERSION_CONFLICT,
        stateStore().updateState(job.getId(), "other", "missing", 0, claim));
    assertEquals(
        StateWriteResult.SUPERSEDED,
        stateStore().initState(job.getId(), NAMESPACE, "duplicate wrong owner", claim + 1));
    assertEquals(
        new VersionedState("initial", 0),
        stateStore().getState(job.getId(), NAMESPACE).orElseThrow());
    assertEquals(
        StateWriteResult.UPDATED,
        stateStore().updateState(job.getId(), NAMESPACE, "updated", 0, claim));
    assertEquals(
        new VersionedState("updated", 1),
        stateStore().getState(job.getId(), NAMESPACE).orElseThrow());
  }

  @Test
  void fencedState_recoveryRejectsOldOwnerBeforeAndAfterReclaim() {
    var job = claimedStateJob();
    long oldClaim = job.getClaimSeq();
    stateStore().initState(job.getId(), NAMESPACE, "initial", oldClaim);
    assertEquals(1, store().resetOrphanJobsBefore(Instant.now().plusSeconds(60), 3, 100).reset());
    assertEquals(oldClaim, store().findById(job.getId()).orElseThrow().getClaimSeq().longValue());
    assertFencedSuperseded(job.getId(), oldClaim);
    var next = store().claimNextBatch(1, "next-owner").get(0);
    assertTrue(next.getClaimSeq() > oldClaim);
    assertFencedSuperseded(job.getId(), oldClaim);
    assertEquals(
        StateWriteResult.UPDATED,
        stateStore().updateState(job.getId(), NAMESPACE, "next", 0, next.getClaimSeq()));
  }

  private void assertFencedSuperseded(UUID id, long claim) {
    assertEquals(
        StateWriteResult.SUPERSEDED, stateStore().initState(id, "absent", "rejected", claim));
    assertEquals(
        StateWriteResult.SUPERSEDED, stateStore().updateState(id, NAMESPACE, "rejected", 0, claim));
    assertTrue(stateStore().getState(id, "absent").isEmpty());
    assertEquals(
        new VersionedState("initial", 0), stateStore().getState(id, NAMESPACE).orElseThrow());
  }

  @Test
  void fencedState_rejectsSucceededJob() {
    var job = claimedStateJob();
    stateStore().initState(job.getId(), NAMESPACE, "initial", job.getClaimSeq());
    store()
        .markJobSucceeded(
            job.getId(), null, null, Instant.EPOCH, Instant.EPOCH.plusSeconds(1), 100L, 50L);
    assertFencedSuperseded(job.getId(), job.getClaimSeq());
  }

  @Test
  void fencedState_rejectsCancelledJob() {
    var job = claimedStateJob();
    stateStore().initState(job.getId(), NAMESPACE, "initial", job.getClaimSeq());
    store().cancelJob(job.getId());
    assertFencedSuperseded(job.getId(), job.getClaimSeq());
  }

  @Test
  void fencedState_missingJobIsSuperseded() {
    UUID id = UUID.randomUUID();
    assertEquals(StateWriteResult.SUPERSEDED, stateStore().initState(id, NAMESPACE, "{}", 1));
    assertEquals(StateWriteResult.SUPERSEDED, stateStore().updateState(id, NAMESPACE, "{}", 0, 1));
  }

  @Test
  void fencedState_sizeLimitsAndValidation() {
    var job = claimedStateJob();
    long claim = job.getClaimSeq();
    assertThrows(
        IllegalArgumentException.class,
        () -> stateStore().initState(job.getId(), NAMESPACE, stateOverLimit(), claim));
    assertTrue(stateStore().getState(job.getId(), NAMESPACE).isEmpty());
    assertEquals(
        StateWriteResult.UPDATED,
        stateStore().initState(job.getId(), NAMESPACE, stateAtLimit(), claim));
    assertThrows(
        IllegalArgumentException.class,
        () -> stateStore().updateState(job.getId(), NAMESPACE, stateOverLimit(), 0, claim));
    assertEquals(
        new VersionedState(stateAtLimit(), 0),
        stateStore().getState(job.getId(), NAMESPACE).orElseThrow());
    assertEquals(
        StateWriteResult.UPDATED,
        stateStore().updateState(job.getId(), NAMESPACE, stateAtLimit(), 0, claim));
    assertEquals(
        new VersionedState(stateAtLimit(), 1),
        stateStore().getState(job.getId(), NAMESPACE).orElseThrow());
    assertThrows(
        IllegalArgumentException.class,
        () -> stateStore().initState(job.getId(), "x".repeat(65), "{}", claim));
    assertThrows(
        IllegalArgumentException.class,
        () -> stateStore().updateState(job.getId(), "x".repeat(65), "{}", 0, claim));
    assertThrows(
        IllegalArgumentException.class,
        () -> stateStore().updateState(job.getId(), NAMESPACE, "{}", -1, claim));
  }

  @Test
  void fencedState_atLimitCrossesEncryptionSeam() {
    var job = claimedStateJob();
    RecordingEngine engine = new RecordingEngine();
    EncryptionHolder.install(
        List.of(engine), RecordingEngine.ALGORITHM_ID, new SingleKeyProvider(), true);
    String value = stateAtLimit();
    assertEquals(
        StateWriteResult.UPDATED,
        stateStore().initState(job.getId(), NAMESPACE, value, job.getClaimSeq()));
    assertEquals(
        StateWriteResult.UPDATED,
        stateStore().updateState(job.getId(), NAMESPACE, value, 0, job.getClaimSeq()));
    int beforeRead = engine.decryptCount.get();
    assertEquals(
        new VersionedState(value, 1), stateStore().getState(job.getId(), NAMESPACE).orElseThrow());
    assertEquals(2, engine.encryptedPlaintexts.stream().filter(value::equals).count());
    assertTrue(engine.decryptCount.get() > beforeRead);
  }

  // ---- Extension state ----

  @Test
  void initState_createsRowAtVersionZero() {
    var job = persist(newPendingJob());

    stateStore().initState(job.getId(), NAMESPACE, "{\"steps\":{}}");

    VersionedState state = stateStore().getState(job.getId(), NAMESPACE).orElseThrow();
    assertEquals("{\"steps\":{}}", state.json());
    assertEquals(0, state.version());
  }

  @Test
  void initState_rejectsNamespaceLongerThan64Characters() {
    var job = persist(newPendingJob());

    assertThrows(
        IllegalArgumentException.class,
        () -> stateStore().initState(job.getId(), "x".repeat(65), "{}"));
  }

  @Test
  void initState_failsLoudWhenAlreadyInitialized() {
    var job = persist(newPendingJob());
    stateStore().initState(job.getId(), NAMESPACE, "{}");

    assertThrows(
        IllegalStateException.class, () -> stateStore().initState(job.getId(), NAMESPACE, "{}"));
  }

  @Test
  void getState_returnsEmptyWhenAbsent() {
    var job = persist(newPendingJob());

    assertTrue(stateStore().getState(job.getId(), NAMESPACE).isEmpty());
  }

  @Test
  void updateState_appliesOnMatchingVersionAndIncrements() {
    var job = persist(newPendingJob());
    stateStore().initState(job.getId(), NAMESPACE, "{\"v\":0}");

    assertTrue(stateStore().updateState(job.getId(), NAMESPACE, "{\"v\":1}", 0));

    VersionedState state = stateStore().getState(job.getId(), NAMESPACE).orElseThrow();
    assertEquals("{\"v\":1}", state.json());
    assertEquals(1, state.version());
  }

  @Test
  void updateState_rejectsStaleVersionAndLeavesStateUntouched() {
    var job = persist(newPendingJob());
    stateStore().initState(job.getId(), NAMESPACE, "{\"v\":0}");
    assertTrue(stateStore().updateState(job.getId(), NAMESPACE, "{\"v\":1}", 0));

    assertFalse(
        stateStore().updateState(job.getId(), NAMESPACE, "{\"v\":-1}", 0),
        "a stale expectedVersion must not apply");

    VersionedState state = stateStore().getState(job.getId(), NAMESPACE).orElseThrow();
    assertEquals("{\"v\":1}", state.json());
    assertEquals(1, state.version());
  }

  @Test
  void updateState_returnsFalseWhenRowAbsent() {
    var job = persist(newPendingJob());

    assertFalse(stateStore().updateState(job.getId(), NAMESPACE, "{}", 0));
  }

  @Test
  void state_isScopedPerNamespace() {
    var job = persist(newPendingJob());
    stateStore().initState(job.getId(), "ns-one", "{\"a\":1}");
    stateStore().initState(job.getId(), "ns-two", "{\"b\":2}");
    assertTrue(stateStore().updateState(job.getId(), "ns-one", "{\"a\":2}", 0));

    assertEquals("{\"a\":2}", stateStore().getState(job.getId(), "ns-one").orElseThrow().json());
    assertEquals("{\"b\":2}", stateStore().getState(job.getId(), "ns-two").orElseThrow().json());
    assertEquals(0, stateStore().getState(job.getId(), "ns-two").orElseThrow().version());
  }

  // ---- Encryption posture ----

  @Test
  void state_crossesEncryptionSeamWhenEngineInstalled() {
    var job = persist(newPendingJob());
    RecordingEngine engine = new RecordingEngine();
    EncryptionHolder.install(
        List.of(engine), RecordingEngine.ALGORITHM_ID, new SingleKeyProvider(), true);

    stateStore().initState(job.getId(), NAMESPACE, "{\"secret\":\"s3cr3t\"}");
    assertTrue(stateStore().updateState(job.getId(), NAMESPACE, "{\"secret\":\"upd4ted\"}", 0));

    // Round-trip: decrypted blob and CAS version come back intact.
    VersionedState state = stateStore().getState(job.getId(), NAMESPACE).orElseThrow();
    assertEquals("{\"secret\":\"upd4ted\"}", state.json());
    assertEquals(1, state.version());

    // The engine actually saw both writes — the blob was encrypted before it reached the store,
    // not merely round-tripped as plaintext.
    assertTrue(engine.encryptedPlaintexts.contains("{\"secret\":\"s3cr3t\"}"));
    assertTrue(engine.encryptedPlaintexts.contains("{\"secret\":\"upd4ted\"}"));
    assertTrue(engine.decryptCount.get() > 0, "read path must route through the engine");
  }

  @Test
  void state_isEncryptedForOptedInJobWhenGlobalSwitchOff() {
    RecordingEngine engine = new RecordingEngine();
    EncryptionHolder.install(
        List.of(engine), RecordingEngine.ALGORITHM_ID, new SingleKeyProvider(), false);
    var job = newPendingJob();
    job.setEncryptedPayload(true);
    job = persist(job);

    stateStore().initState(job.getId(), NAMESPACE, "{\"secret\":\"initial\"}");
    assertTrue(stateStore().updateState(job.getId(), NAMESPACE, "{\"secret\":\"updated\"}", 0));

    int decryptCallsBeforeRead = engine.decryptCount.get();
    VersionedState state = stateStore().getState(job.getId(), NAMESPACE).orElseThrow();
    assertEquals("{\"secret\":\"updated\"}", state.json());
    assertEquals(1, state.version());
    assertTrue(engine.encryptedPlaintexts.contains("{\"secret\":\"initial\"}"));
    assertTrue(engine.encryptedPlaintexts.contains("{\"secret\":\"updated\"}"));
    // No raw-state fixture hook exists; decryption proves stored bytes crossed the engine.
    assertTrue(
        engine.decryptCount.get() > decryptCallsBeforeRead,
        "read path must decrypt the stored state");
  }

  @Test
  void state_isPlaintextForNonOptedJobWhenGlobalSwitchOff() {
    RecordingEngine engine = new RecordingEngine();
    EncryptionHolder.install(
        List.of(engine), RecordingEngine.ALGORITHM_ID, new SingleKeyProvider(), false);
    var job = newPendingJob();
    job.setEncryptedPayload(false);
    job = persist(job);

    stateStore().initState(job.getId(), NAMESPACE, "{\"plain\":0}");
    assertTrue(stateStore().updateState(job.getId(), NAMESPACE, "{\"plain\":1}", 0));

    assertEquals(
        "{\"plain\":1}", stateStore().getState(job.getId(), NAMESPACE).orElseThrow().json());
    assertTrue(engine.encryptedPlaintexts.isEmpty(), "non-opted job state must remain plaintext");
    assertEquals(0, engine.decryptCount.get());
  }

  @Test
  void initState_failsWhenJobDoesNotExist() {
    UUID jobId = UUID.randomUUID();

    IllegalStateException failure =
        assertThrows(
            IllegalStateException.class, () -> stateStore().initState(jobId, NAMESPACE, "{}"));

    assertTrue(
        String.valueOf(failure.getMessage()).contains(jobId.toString()),
        "the missing-job failure must identify the job");
    assertTrue(stateStore().getState(jobId, NAMESPACE).isEmpty());
    assertFalse(stateStore().updateState(jobId, NAMESPACE, "{}", 0));
  }

  // ---- Archive retention ----

  @Test
  void archiveJob_copiesStateOntoArchiveRow() {
    var job = persist(newPendingJob());
    stateStore().initState(job.getId(), NAMESPACE, "{\"steps\":{}}");
    store().compareAndSwapStatus(job.getId(), JobStatus.PENDING, JobStatus.RUNNING, null);
    store()
        .markJobSucceeded(
            job.getId(), null, null, Instant.EPOCH, Instant.EPOCH.plusSeconds(1), 100L, 50L);
    var completed = store().findById(job.getId()).orElseThrow();

    var archived = archiveStore().archiveJob(completed, "tck", "tck-node");

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
    stateStore().initState(job.getId(), NAMESPACE, "{\"secret\":\"s3cr3t\"}");
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

    stateStore().initState(job.getId(), NAMESPACE, "{\"plain\":true}");

    VersionedState state = stateStore().getState(job.getId(), NAMESPACE).orElseThrow();
    assertEquals("{\"plain\":true}", state.json());
  }

  /** A JSON string of exactly {@link ExtensionValidation#MAX_STATE_BYTES} UTF-8 bytes. */
  private static String stateAtLimit() {
    return "\"" + "é".repeat((ExtensionValidation.MAX_STATE_BYTES - 2) / 2) + "\"";
  }

  /** A JSON string one UTF-8 byte over {@link ExtensionValidation#MAX_STATE_BYTES}. */
  private static String stateOverLimit() {
    return "\"" + "é".repeat((ExtensionValidation.MAX_STATE_BYTES - 2) / 2) + "x\"";
  }

  @Test
  void initState_acceptsStateAtTheSizeLimit() {
    var job = persist(newPendingJob());
    String value = stateAtLimit();
    stateStore().initState(job.getId(), NAMESPACE, value);
    assertEquals(
        new VersionedState(value, 0), stateStore().getState(job.getId(), NAMESPACE).orElseThrow());
  }

  @Test
  void updateState_acceptsStateAtTheSizeLimit() {
    var job = persist(newPendingJob());
    stateStore().initState(job.getId(), NAMESPACE, "{}");
    String value = stateAtLimit();
    assertTrue(stateStore().updateState(job.getId(), NAMESPACE, value, 0));
    assertEquals(
        new VersionedState(value, 1), stateStore().getState(job.getId(), NAMESPACE).orElseThrow());
  }

  @Test
  void initState_rejectsStateOverTheSizeLimit() {
    var job = persist(newPendingJob());
    assertThrows(
        IllegalArgumentException.class,
        () -> stateStore().initState(job.getId(), NAMESPACE, stateOverLimit()));
    assertTrue(stateStore().getState(job.getId(), NAMESPACE).isEmpty());
  }

  @Test
  void updateState_rejectsStateOverTheSizeLimit() {
    var job = persist(newPendingJob());
    stateStore().initState(job.getId(), NAMESPACE, "{}");
    assertThrows(
        IllegalArgumentException.class,
        () -> stateStore().updateState(job.getId(), NAMESPACE, stateOverLimit(), 0));
    assertEquals(
        new VersionedState("{}", 0), stateStore().getState(job.getId(), NAMESPACE).orElseThrow());
  }

  @Test
  void state_atTheSizeLimitCrossesEncryptionSeam() {
    var job = persist(newPendingJob());
    RecordingEngine engine = new RecordingEngine();
    EncryptionHolder.install(
        List.of(engine), RecordingEngine.ALGORITHM_ID, new SingleKeyProvider(), true);
    String value = stateAtLimit();
    stateStore().initState(job.getId(), NAMESPACE, value);
    assertTrue(stateStore().updateState(job.getId(), NAMESPACE, value, 0));
    assertEquals(
        new VersionedState(value, 1), stateStore().getState(job.getId(), NAMESPACE).orElseThrow());
    assertEquals(2, engine.encryptedPlaintexts.stream().filter(value::equals).count());
    assertTrue(engine.decryptCount.get() > 0, "read path must route through the engine");
  }

  @Test
  void archiveJob_copiesStateAtTheSizeLimit() {
    var job = persist(newPendingJob());
    String value = stateAtLimit();
    stateStore().initState(job.getId(), NAMESPACE, value);
    store().compareAndSwapStatus(job.getId(), JobStatus.PENDING, JobStatus.RUNNING, null);
    store()
        .markJobSucceeded(
            job.getId(), null, null, Instant.EPOCH, Instant.EPOCH.plusSeconds(1), 100L, 50L);
    var completed = store().findById(job.getId()).orElseThrow();
    archiveStore().archiveJob(completed, "tck", "tck-node");
    var archived =
        archiveStore()
            .findArchivedJobs(completed.getPayload().target(), null, null, null, 10)
            .stream()
            .filter(row -> row.getOriginalJobId().equals(job.getId()))
            .findFirst()
            .orElseThrow();
    JsonObject entry =
        Json.createReader(new StringReader(archived.getExtensionState()))
            .readArray()
            .getJsonObject(0);
    assertEquals(NAMESPACE, entry.getString("namespace"));
    assertEquals(value, entry.getString("state"));
  }
}
