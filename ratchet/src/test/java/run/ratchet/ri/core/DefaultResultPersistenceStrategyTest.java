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
package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.RatchetOptions;
import run.ratchet.ri.testsupport.EncryptionTestKit;
import run.ratchet.ri.testutil.JsonbTestPayloadSerializer;
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.spi.ProtectedSurface;
import run.ratchet.spi.SerializedJobResult;
import run.ratchet.store.converter.EncryptionHolder;
import run.ratchet.store.util.EncryptionTarget;
import run.ratchet.store.util.PayloadEncryptor;

class DefaultResultPersistenceStrategyTest {

  @AfterEach
  void disableEncryption() {
    EncryptionHolder.disable();
  }

  @Test
  void resultLimitUsesUtf8ByteLength() {
    DefaultResultPersistenceStrategy strategy =
        strategy(RatchetOptions.builder().payload(payload -> payload.maxResultBytes(5)).build());

    SerializedJobResult result = strategy.serialize(new UUID(0L, 42L), "éé");

    assertEquals(String.class.getName(), result.type());
    assertTrue(result.json().contains("\"_truncated\":true"));
    assertTrue(result.json().contains("\"_originalSize\":6"));
  }

  @Test
  void resultAtByteLimitIsPersistedWithoutTruncation() {
    DefaultResultPersistenceStrategy strategy =
        strategy(RatchetOptions.builder().payload(payload -> payload.maxResultBytes(6)).build());

    SerializedJobResult result = strategy.serialize(new UUID(0L, 41L), "éé");

    assertEquals(String.class.getName(), result.type());
    assertEquals("\"éé\"", result.json());
  }

  @Test
  void zeroResultLimitPersistsLargeResults() {
    DefaultResultPersistenceStrategy strategy =
        strategy(RatchetOptions.builder().payload(payload -> payload.maxResultBytes(0)).build());

    SerializedJobResult result = strategy.serialize(new UUID(0L, 45L), "larger than five bytes");

    assertEquals(String.class.getName(), result.type());
    assertEquals("\"larger than five bytes\"", result.json());
  }

  @Test
  void nullResultPersistsAsEmptyResult() {
    SerializedJobResult result = strategy(defaultOptions()).serialize(new UUID(0L, 43L), null);

    assertNull(result.json());
    assertNull(result.type());
  }

  @Test
  void serializationFailureIsPropagated() {
    DefaultResultPersistenceStrategy strategy =
        new DefaultResultPersistenceStrategy(
            defaultOptions(), new ThrowingPayloadSerializer(), null);

    IllegalStateException failure =
        assertThrows(
            IllegalStateException.class,
            () -> strategy.serialize(new UUID(0L, 44L), "unserializable"));

    assertTrue(failure.getMessage().contains("Result serialization failed"));
    assertEquals("cannot serialize", failure.getCause().getMessage());
  }

  @Test
  void resultIsEncryptedAtRestAndRoundTripsThroughTheRiReadPath() {
    // Global switch on, so every result is encrypted regardless of the per-job flag lookup.
    EncryptionTestKit.install(true);
    UUID jobId = new UUID(0L, 99L);
    DefaultResultPersistenceStrategy strategy = strategy(defaultOptions());

    SerializedJobResult result = strategy.serialize(jobId, "top-secret-result");

    // What lands in the JSON/JSONB column is the encrypted envelope, not the cleartext result.
    assertEquals(String.class.getName(), result.type());
    assertNotEquals("\"top-secret-result\"", result.json());
    assertFalse(result.json().contains("top-secret-result"));
    // The reference implementation's read path decrypts before deserializing.
    assertEquals(
        "\"top-secret-result\"",
        PayloadEncryptor.decryptJsonColumn(
            result.json(), EncryptionTarget.rowBound(ProtectedSurface.RESULT, jobId)));
  }

  private static DefaultResultPersistenceStrategy strategy(RatchetOptions options) {
    // jobCrudStore is null: these tests drive the global switch, not the per-job flag lookup.
    return new DefaultResultPersistenceStrategy(options, new JsonbTestPayloadSerializer(), null);
  }

  private static RatchetOptions defaultOptions() {
    return RatchetOptions.builder().build();
  }

  private static final class ThrowingPayloadSerializer implements PayloadSerializer {

    @Override
    public String serialize(Object payload) {
      throw new IllegalArgumentException("cannot serialize");
    }

    @Override
    public <T> T deserialize(String json, Class<T> type) {
      throw new UnsupportedOperationException("not used");
    }
  }
}
