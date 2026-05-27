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
