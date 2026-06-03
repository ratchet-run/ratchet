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
package run.ratchet.ri.security;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * {@code ri.security.PayloadMasker} is a thin pass-through to {@code store.util.PayloadMasker}; the
 * authoritative masking behavior (nesting, case-insensitivity, arrays, scalar/array roots,
 * serialization failure) is covered by the store-core {@code PayloadMaskerTest}. These cases only
 * prove the facade forwards each overload.
 */
class PayloadMaskerTest {

  @Test
  void maskPayload_nullInput_returnsNull() {
    assertNull(PayloadMasker.maskPayload(null));
  }

  @Test
  void maskPayload_validJson_maskesSensitiveFields() {
    String json = "{\"username\":\"alice\",\"password\":\"s3cret\",\"data\":\"public\"}";
    String masked = PayloadMasker.maskPayload(json);

    assertEquals(
        "{\"username\":\"alice\",\"password\":\"***REDACTED***\",\"data\":\"public\"}", masked);
  }

  @Test
  void maskPayload_objectOverload_masksSerializedObjects() {
    String result = PayloadMasker.maskPayload(Map.of("password", "secret", "username", "alice"));

    assertFalse(result.contains("secret"));
    assertTrue(result.contains("\"password\":\"***REDACTED***\""));
    assertTrue(result.contains("\"username\":\"alice\""));
  }
}
