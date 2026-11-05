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
package run.ratchet.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class AttemptTokenTest {
  private final UUID id = UUID.randomUUID();

  @Test
  void validatesIdentityAndSequence() {
    assertThrows(NullPointerException.class, () -> new AttemptToken(null, 1));
    assertThrows(IllegalArgumentException.class, () -> new AttemptToken(id, 0));
    assertThrows(IllegalArgumentException.class, () -> new AttemptToken(id, -1));
  }

  @Test
  void comparesOnlyWithinOneJob() {
    AttemptToken token = new AttemptToken(id, 2);
    assertTrue(token.isNewerThan(new AttemptToken(id, 1)));
    assertFalse(token.isNewerThan(new AttemptToken(id, 2)));
    assertFalse(token.isNewerThan(new AttemptToken(id, 3)));
    assertThrows(NullPointerException.class, () -> token.isNewerThan(null));
    assertThrows(
        IllegalArgumentException.class,
        () -> token.isNewerThan(new AttemptToken(UUID.randomUUID(), 1)));
  }

  @Test
  void parsesOnlyCanonicalTokens() {
    AttemptToken token = new AttemptToken(id, Long.MAX_VALUE);
    assertEquals(id + ":" + Long.MAX_VALUE, token.toString());
    assertEquals(token, AttemptToken.parse(token.toString()));
    assertThrows(NullPointerException.class, () -> AttemptToken.parse(null));
    for (String text :
        new String[] {
          "",
          id.toString(),
          ":1",
          "bad:1",
          id + ":",
          id + ":x",
          id + ":0",
          id + ":-1",
          id + ":1:2",
          id + ":+1",
          id + ":01",
          id + ":9223372036854775808",
          "1-1-1-1-1:1"
        }) {
      assertThrows(IllegalArgumentException.class, () -> AttemptToken.parse(text), text);
    }
  }

  @Test
  void serializationRoundTrips() throws Exception {
    AttemptToken token = new AttemptToken(id, 7);
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
      out.writeObject(token);
    }
    try (ObjectInputStream in =
        new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      assertEquals(token, in.readObject());
    }
  }
}
