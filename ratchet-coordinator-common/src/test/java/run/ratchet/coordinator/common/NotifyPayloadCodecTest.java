package run.ratchet.coordinator.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import run.ratchet.api.JobPriority;
import run.ratchet.api.NodeIdentity;

class NotifyPayloadCodecTest {

  private final NotifyPayloadCodec codec = new NotifyPayloadCodec();

  @ParameterizedTest
  @EnumSource(JobPriority.class)
  void roundTripPreservesAllJobPriorityValues(JobPriority priority) {
    NodeIdentity node = new NodeIdentity("10.0.1.42:31415:abcd");
    String json = codec.encode(NotifyPayload.current(node, priority));

    NotifyPayload decoded = codec.decode(json);

    assertEquals(NotifyPayloadCodec.CURRENT_VERSION, decoded.version());
    assertEquals(node, decoded.node());
    assertEquals(priority, decoded.priority());
  }

  @Test
  void encodedEnvelopeContainsExpectedFieldsWithoutCorrelationId() {
    String json = codec.encode(NotifyPayload.current(new NodeIdentity("nodeA"), JobPriority.HIGH));

    assertTrue(json.contains("\"v\":1"), json);
    assertTrue(json.contains("\"node\":\"nodeA\""), json);
    assertTrue(json.contains("\"prio\":\"HIGH\""), json);
    assertFalse(json.contains("\"cid\""), json);
  }

  @Test
  void encodedEnvelopeStaysSmallEnoughForTransportPayloadCaps() {
    String json =
        codec.encode(
            NotifyPayload.current(
                new NodeIdentity("hostname-with-some-length:7777:0123456789abcdef"),
                JobPriority.NORMAL));

    assertTrue(
        json.getBytes().length < 200,
        () -> "envelope unexpectedly large (" + json.length() + " bytes): " + json);
  }

  @Test
  void unknownEnvelopeVersionRejected() {
    String v2 = "{\"v\":2,\"node\":\"x\",\"prio\":\"HIGH\"}";
    DecodeException ex = assertThrows(DecodeException.class, () -> codec.decode(v2));
    assertTrue(ex.getMessage().contains("version 2"), ex.getMessage());
  }

  @Test
  void unknownPriorityStringRejected() {
    String json = "{\"v\":1,\"node\":\"x\",\"prio\":\"BANANA\"}";
    DecodeException ex = assertThrows(DecodeException.class, () -> codec.decode(json));
    assertTrue(ex.getMessage().contains("BANANA"), ex.getMessage());
  }

  @Test
  void blankNodeIdentityRejected() {
    String json = "{\"v\":1,\"node\":\"\",\"prio\":\"HIGH\"}";
    assertThrows(DecodeException.class, () -> codec.decode(json));
  }

  @Test
  void missingFieldsRejected() {
    assertThrows(DecodeException.class, () -> codec.decode("{\"v\":1,\"prio\":\"HIGH\"}"));
    assertThrows(DecodeException.class, () -> codec.decode("{\"v\":1,\"node\":\"x\"}"));
    assertThrows(DecodeException.class, () -> codec.decode("{\"node\":\"x\",\"prio\":\"HIGH\"}"));
  }

  @Test
  void malformedJsonRejected() {
    assertThrows(DecodeException.class, () -> codec.decode("not json"));
    assertThrows(DecodeException.class, () -> codec.decode("{"));
    assertThrows(DecodeException.class, () -> codec.decode(""));
    assertThrows(DecodeException.class, () -> codec.decode(null));
  }

  @Test
  void wrongFieldTypesRejected() {
    assertThrows(
        DecodeException.class,
        () -> codec.decode("{\"v\":\"1\",\"node\":\"x\",\"prio\":\"HIGH\"}"));
    assertThrows(
        DecodeException.class, () -> codec.decode("{\"v\":1,\"node\":42,\"prio\":\"HIGH\"}"));
    assertThrows(DecodeException.class, () -> codec.decode("[\"not\",\"an\",\"object\"]"));
  }

  @Test
  void unknownFieldsAreIgnoredForForwardCompat() {
    String json =
        "{\"v\":1,\"node\":\"x\",\"prio\":\"NORMAL\",\"futureField\":\"someValue\","
            + "\"anotherFutureField\":42}";

    NotifyPayload decoded = codec.decode(json);

    assertNotNull(decoded);
    assertEquals("x", decoded.node().value());
    assertEquals(JobPriority.NORMAL, decoded.priority());
  }
}
