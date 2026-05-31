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
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class NodeIdentityTest {

  @Test
  void exposesWrappedValue() {
    NodeIdentity identity = new NodeIdentity("node-42");
    assertEquals("node-42", identity.value());
  }

  @Test
  void rejectsNullValue() {
    assertThrows(NullPointerException.class, () -> new NodeIdentity(null));
  }

  @Test
  void rejectsBlankValue() {
    assertThrows(IllegalArgumentException.class, () -> new NodeIdentity(""));
    assertThrows(IllegalArgumentException.class, () -> new NodeIdentity("   "));
    assertThrows(IllegalArgumentException.class, () -> new NodeIdentity("\t\n"));
  }

  @Test
  void acceptsCharactersInAllowedSet() {
    new NodeIdentity("nodeA");
    new NodeIdentity("node-42");
    new NodeIdentity("node_42");
    new NodeIdentity("pod.svc.cluster.local");
    new NodeIdentity("10.0.1.42:31415");
    new NodeIdentity("550e8400-e29b-41d4-a716-446655440000");
  }

  @Test
  void rejectsJmsSelectorMetacharacters() {
    // single-quote, backslash, percent — all significant to JMS string literals / LIKE patterns
    assertThrows(IllegalArgumentException.class, () -> new NodeIdentity("alice's-node"));
    assertThrows(IllegalArgumentException.class, () -> new NodeIdentity("node\\one"));
    assertThrows(IllegalArgumentException.class, () -> new NodeIdentity("node%two"));
  }

  @Test
  void rejectsWhitespaceAndControlChars() {
    assertThrows(IllegalArgumentException.class, () -> new NodeIdentity("node one"));
    assertThrows(IllegalArgumentException.class, () -> new NodeIdentity("node\tfoo"));
    assertThrows(IllegalArgumentException.class, () -> new NodeIdentity("node\nfoo"));
  }
}
