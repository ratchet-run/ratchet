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
package run.ratchet.tck.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobHandle;

/**
 * Base contract for the raw payload delivered through {@code deliverSignal(.., Serializable)}.
 *
 * <p>A raw signal payload is retained as its JSON structure only; the executing job observes it in
 * its JSON-native form through {@code JobContext.signalPayload(Class)} — a {@code String}, a {@code
 * Number}, a {@code Boolean}, a {@code List}, or a {@code Map} — never the original concrete class.
 * Each contract delivers one of the five documented shapes and asserts the shape the job observes,
 * locking the round-trip across every conformant store.
 *
 * <p>The numeric case asserts a {@code Number} by value rather than a concrete type: a JSON number
 * maps back to {@code java.math.BigDecimal} under the default JSON-B serializer, and the portable
 * guarantee is "a {@code Number}", not "the same boxed type that was delivered". Likewise the list
 * and map cases assert the collection interface, not a provider-specific implementation class.
 *
 * @see TckJobs#recordRawSignalPayload()
 */
public abstract class AbstractSignalPayloadContract {

  @AfterEach
  void clearAfterEach() {
    runtime().clear();
    TckJobs.resetAll();
  }

  @Test
  void rawStringPayloadRoundTripsAsJsonNativeString() {
    assertRawSignalRoundTrips("raw-string-tck", "hello-raw", "String:hello-raw");
  }

  @Test
  void rawNumberPayloadRoundTripsAsJsonNativeNumber() {
    assertRawSignalRoundTrips("raw-number-tck", 42, "Number:42");
  }

  @Test
  void rawBooleanPayloadRoundTripsAsJsonNativeBoolean() {
    assertRawSignalRoundTrips("raw-boolean-tck", true, "Boolean:true");
  }

  @Test
  void rawListPayloadRoundTripsAsJsonNativeList() {
    // A concrete Serializable list: the bare List.of(..) type does not satisfy the Serializable
    // parameter bound at compile time, mirroring how callers must shape a list payload.
    assertRawSignalRoundTrips("raw-list-tck", new ArrayList<>(List.of("x", "y")), "List:[x, y]");
  }

  @Test
  void rawMapPayloadRoundTripsAsJsonNativeMap() {
    assertRawSignalRoundTrips(
        "raw-map-tck", new LinkedHashMap<>(Map.of("a", "1", "b", "2")), "Map:{a=1, b=2}");
  }

  /**
   * Submits a signal-waiting job, delivers {@code payload} as a raw signal, and asserts the running
   * job observed it as {@code expectedToken} (see {@link TckJobs#recordRawSignalPayload()}).
   */
  private void assertRawSignalRoundTrips(
      String signalKey, Serializable payload, String expectedToken) {
    JobHandle handle =
        runtime()
            .scheduler()
            .enqueue(TckJobs::recordRawSignalPayload)
            .awaitSignal(signalKey, defaultTimeout())
            .submit();
    runtime().probe().track(handle);

    assertEquals(
        1,
        runtime().scheduler().deliverSignal(handle.id(), payload),
        "delivering a raw payload should unblock exactly the waiting job");

    assertTrue(
        runtime().probe().awaitCompleted(handle, defaultTimeout()),
        "raw signal should unblock and complete the job");
    assertEquals(List.of(expectedToken), TckJobs.rawSignalPayloads());
  }

  protected abstract RatchetTckRuntime runtime();

  protected Duration defaultTimeout() {
    return Duration.ofSeconds(10);
  }
}
