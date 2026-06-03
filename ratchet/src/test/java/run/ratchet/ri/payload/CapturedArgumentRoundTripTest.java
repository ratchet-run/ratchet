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
package run.ratchet.ri.payload;

import static org.junit.jupiter.api.Assertions.assertEquals;

import jakarta.json.bind.Jsonb;
import jakarta.json.bind.JsonbBuilder;
import java.lang.reflect.Method;
import org.junit.jupiter.api.Test;
import org.objectweb.asm.Type;
import run.ratchet.api.SerializableCheckedRunnable;
import run.ratchet.store.entity.JobPayload;

/**
 * A lambda that captures a non-String literal must round-trip through JSON-B persistence and still
 * invoke its target with the captured value. The captured argument lands in {@link
 * JobPayload#args()} (a {@code List<Object>}); JSON-B has no element type to deserialize back to,
 * so a captured {@code long} returns as a {@code BigDecimal}, and {@code JobTask} hands it straight
 * to {@code Method.invoke} against a {@code long} parameter with no coercion.
 */
class CapturedArgumentRoundTripTest {

  private static volatile long received;

  public static final class NumericTarget {
    public static void process(long id) {
      received = id;
    }
  }

  @Test
  void capturedLongArgSurvivesJsonRoundTripAndInvokesTarget() throws Exception {
    long capturedId = 4_815_162_342L;
    JobPayload payload =
        JobPayloadFactory.fromLambda(
            (SerializableCheckedRunnable) () -> NumericTarget.process(capturedId));

    // Capture is correct: the argument is the captured long before persistence.
    assertEquals(java.util.List.of(capturedId), payload.args());

    // Round-trip through the same JSON-B configuration the store uses to persist JobPayload.
    JobPayload reloaded;
    try (Jsonb jsonb = JsonbBuilder.create()) {
      reloaded = jsonb.fromJson(jsonb.toJson(payload), JobPayload.class);
    }

    // Resolve and invoke exactly as JobTask does: by descriptor, coerce, then Method.invoke.
    received = 0;
    Method target = resolve(reloaded);
    target.invoke(
        null, ArgumentCoercion.coerce(target.getParameterTypes(), reloaded.args().toArray()));

    assertEquals(capturedId, received);
  }

  private static Method resolve(JobPayload payload) throws Exception {
    Class<?> target = Class.forName(payload.target());
    for (Method method : target.getMethods()) {
      if (method.getName().equals(payload.method())
          && Type.getMethodDescriptor(method).equals(payload.methodDescriptor())) {
        return method;
      }
    }
    throw new NoSuchMethodException(payload.method() + payload.methodDescriptor());
  }
}
