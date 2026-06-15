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
package run.ratchet.testsuite.tck;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.inject.Inject;
import java.util.List;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit5.ArquillianExtension;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import run.ratchet.api.JobHandle;
import run.ratchet.tck.api.AbstractSignalPayloadContract;
import run.ratchet.tck.api.RatchetTckRuntime;
import run.ratchet.tck.api.TckJobs;

/**
 * End-to-end coverage for payload encryption in the full reference implementation, with a real
 * AES-256-GCM {@link RecordingPayloadEncryption} engine and a {@link RecordingKeyProvider} enabled.
 * The inherited signal-payload round-trip contract runs with the engine installed (global switch
 * off, so non-opted-in jobs stay plaintext and the contract is unaffected). The added test opts a
 * job in with {@code withEncryptedPayload()} and proves the engine was genuinely exercised on the
 * signal write and read paths — the result/signal boundary the store-level TCK cannot cover, since
 * those are encrypted in the RI rather than the store.
 */
@ExtendWith(ArquillianExtension.class)
class RiPayloadEncryptionIT extends AbstractSignalPayloadContract {

  @Inject private RiRatchetTckRuntime runtime;

  @Override
  protected RatchetTckRuntime runtime() {
    return runtime;
  }

  @Deployment
  public static WebArchive createDeployment() {
    return RiTckDeployment.createWith(
        new Package[] {}, RecordingPayloadEncryption.class, RecordingKeyProvider.class);
  }

  @Test
  void signalPayloadIsEncryptedAtRestAndDecryptedOnDelivery() {
    RecordingPayloadEncryption.reset();

    JobHandle handle =
        runtime()
            .scheduler()
            .enqueue(TckJobs::recordRawSignalPayload)
            .awaitSignal("cipher-signal-tck", defaultTimeout())
            .withEncryptedPayload()
            .submit();
    runtime().probe().track(handle);

    assertEquals(1, runtime().scheduler().deliverSignal(handle.id(), "classified-signal"));
    assertTrue(runtime().probe().awaitCompleted(handle, defaultTimeout()));

    // The running job observed the decrypted, JSON-native payload.
    assertEquals(List.of("String:classified-signal"), TckJobs.rawSignalPayloads());

    // The engine was genuinely exercised: the signal payload was handed to it on write, and the
    // read path decrypted before the job saw it.
    assertTrue(
        RecordingPayloadEncryption.ENCRYPTED_PLAINTEXTS.stream()
            .anyMatch(p -> p.contains("classified-signal")),
        "signal payload was not handed to the engine before storage");
    assertTrue(RecordingPayloadEncryption.DECRYPT_COUNT.get() > 0, "read path did not decrypt");
  }
}
