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
package run.ratchet.spring.boot.it.aotpreflight;

import java.util.List;
import org.springframework.aot.AotDetector;
import org.springframework.stereotype.Component;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.SerializableCheckedRunnable;
import run.ratchet.ri.core.internal.JobPayloadInvoker;
import run.ratchet.spi.JobInvocation;
import run.ratchet.spi.JobInvocationResolver;
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.store.entity.JobPayload;

/** Independently records the five native payload preflight scenarios. */
@Component
public class AotPreflightScenarios {

  // Submission-site discovery sentinel. The no-store preflight must not instantiate the scheduler.
  @SuppressWarnings("unused")
  private JobSchedulerService schedulerService;

  private final AotPreflightJobs jobs;
  private final JobInvocationResolver invocationResolver;
  private final PayloadSerializer payloadSerializer;
  private final JobPayloadInvoker payloadInvoker;

  public AotPreflightScenarios(
      AotPreflightJobs jobs,
      JobInvocationResolver invocationResolver,
      PayloadSerializer payloadSerializer,
      JobPayloadInvoker payloadInvoker) {
    this.jobs = jobs;
    this.invocationResolver = invocationResolver;
    this.payloadSerializer = payloadSerializer;
    this.payloadInvoker = payloadInvoker;
  }

  public List<Evidence> runAll() {
    return List.of(
        runMethodReferences(),
        runWrapperLambda(),
        runJsonbPayloadRoundTrip(),
        runJobPayloadInvocation(),
        runManifestRejection());
  }

  public Evidence runMethodReferences() {
    try {
      SerializableCheckedRunnable boundReference = jobs::boundJob;
      SerializableCheckedRunnable staticReference = AotPreflightJobs::staticJob;
      JobInvocation bound = invocationResolver.resolve(boundReference);
      JobInvocation staticInvocation = invocationResolver.resolve(staticReference);
      assertInvocation(bound, "boundJob", "()V", false, List.of());
      assertInvocation(staticInvocation, "staticJob", "()V", true, List.of());
      return Evidence.passed("method-references", "bound and static method references resolved");
    } catch (Throwable failure) {
      return Evidence.failed("method-references", failure);
    }
  }

  public Evidence runWrapperLambda() {
    try {
      String capturedString = "wrapper";
      WrapperCapture capturedRecord = new WrapperCapture("record", 17);
      JobInvocation invocation =
          new AotPreflightWrapperSubmitter()
              .resolve(invocationResolver, capturedString, capturedRecord);
      assertInvocation(
          invocation,
          "wrapperJob",
          "(Ljava/lang/String;L" + WrapperCapture.class.getName().replace('.', '/') + ";)V",
          true,
          List.of(capturedString, capturedRecord));
      return Evidence.passed(
          "wrapper-lambda", "inline wrapper captured a String and record payload");
    } catch (Throwable failure) {
      return Evidence.failed("wrapper-lambda", failure);
    }
  }

  public Evidence runJsonbPayloadRoundTrip() {
    try {
      PayloadEnvelope expected = payloadFixture();
      String json = payloadSerializer.serialize(expected);
      PayloadEnvelope restored = payloadSerializer.deserialize(json, PayloadEnvelope.class);
      if (!expected.equals(restored)) {
        throw new AssertionError("Unexpected JSON-B payload round-trip: " + restored);
      }
      return Evidence.passed(
          "jsonb-payload-round-trip", "nested DTO round-tripped through PayloadSerializer JSON-B");
    } catch (Throwable failure) {
      return Evidence.failed("jsonb-payload-round-trip", failure);
    }
  }

  public Evidence runJobPayloadInvocation() {
    try {
      PayloadEnvelope expected = payloadFixture();
      Object jsonValue =
          payloadSerializer.deserialize(payloadSerializer.serialize(expected), Object.class);
      JobPayload serializedPayload =
          new JobPayload(
              AotPreflightJobs.class.getName(),
              "payloadJob",
              "(L" + PayloadEnvelope.class.getName().replace('.', '/') + ";)Ljava/lang/String;",
              true,
              List.of(jsonValue));
      JobPayload materialized =
          payloadInvoker.materializeArguments(serializedPayload, payloadSerializer);
      Object result = payloadInvoker.invoke(materialized);
      if (!"invoice:7".equals(result)) {
        throw new AssertionError("Unexpected JobPayloadInvoker result: " + result);
      }
      return Evidence.passed(
          "job-payload-invocation", "JobPayloadInvoker materialized and invoked the DTO target");
    } catch (Throwable failure) {
      return Evidence.failed("job-payload-invocation", failure);
    }
  }

  public Evidence runManifestRejection() {
    String className = AotPreflightUnregisteredJob.class.getName();
    try {
      JobPayload unregisteredPayload = new JobPayload(className, "run", "()V", true, List.of());
      payloadInvoker.invoke(unregisteredPayload);
      if (!AotDetector.useGeneratedArtifacts()) {
        return Evidence.passed(
            "manifest-rejection", "manifest inactive during the non-AOT application bootstrap");
      }
      return new Evidence(
          "manifest-rejection", false, "AOT manifest unexpectedly invoked " + className);
    } catch (SecurityException expected) {
      String message = expected.getMessage();
      if (message == null
          || !message.contains(className)
          || !message.contains("META-INF/ratchet/aot-registered-classes.txt")
          || !message.contains("ratchet.class-policy.allowed-packages")
          || !message.contains("rebuild")) {
        throw new AssertionError("Manifest rejection was not actionable: " + message, expected);
      }
      return Evidence.passed("manifest-rejection", message);
    } catch (Throwable failure) {
      return Evidence.failed("manifest-rejection", failure);
    }
  }

  private static PayloadEnvelope payloadFixture() {
    return new PayloadEnvelope("invoice", new NestedPayload(7, List.of("native", "jsonb")));
  }

  private static void assertInvocation(
      JobInvocation invocation,
      String expectedMethod,
      String expectedDescriptor,
      boolean expectedStatic,
      List<Object> expectedArguments) {
    if (!AotPreflightJobs.class.getName().equals(invocation.targetClass())
        || !expectedMethod.equals(invocation.methodName())
        || !expectedDescriptor.equals(invocation.methodDescriptor())
        || expectedStatic != invocation.staticMethod()
        || !expectedArguments.equals(invocation.arguments())) {
      throw new AssertionError("Unexpected invocation: " + invocation);
    }
  }

  /** Captured record proving inline-wrapper argument extraction. */
  public record WrapperCapture(String label, int sequence) {}

  /** Root DTO persisted by both JSON-B preflight scenarios. */
  public record PayloadEnvelope(String name, NestedPayload nested) {}

  /** Nested DTO reached through {@link PayloadEnvelope}. */
  public record NestedPayload(int sequence, List<String> tags) {}

  /** One scenario verdict emitted identically by the JVM and native executable. */
  public record Evidence(String scenario, boolean passed, String detail) {

    private static Evidence passed(String scenario, String detail) {
      return new Evidence(scenario, true, detail);
    }

    private static Evidence failed(String scenario, Throwable failure) {
      return new Evidence(
          scenario, false, failure.getClass().getName() + ": " + failure.getMessage());
    }

    public String toJson() {
      return "{\"scenario\":\""
          + escape(scenario)
          + "\",\"passed\":"
          + passed
          + ",\"detail\":\""
          + escape(detail)
          + "\"}";
    }

    private static String escape(String value) {
      return value.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n");
    }
  }
}
