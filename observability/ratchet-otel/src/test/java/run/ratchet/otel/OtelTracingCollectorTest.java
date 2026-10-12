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
package run.ratchet.otel;

import static io.opentelemetry.api.common.AttributeKey.longKey;
import static io.opentelemetry.api.common.AttributeKey.stringKey;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import run.ratchet.spi.TracingCollector;

class OtelTracingCollectorTest {

  @Test
  void additionalAttributesArePassedThroughUnchanged() {
    InMemorySpanExporter exporter = InMemorySpanExporter.create();
    SdkTracerProvider tracerProvider =
        SdkTracerProvider.builder().addSpanProcessor(SimpleSpanProcessor.create(exporter)).build();
    OpenTelemetrySdk openTelemetry =
        OpenTelemetrySdk.builder().setTracerProvider(tracerProvider).build();
    OtelTracingCollector collector = new OtelTracingCollector(openTelemetry);

    try {
      TracingCollector.ExecutionScope scope =
          collector.jobExecutionStarted(
              new UUID(0L, 1L),
              JobType.SINGLE,
              JobPriority.NORMAL,
              Map.of(),
              Map.of(
                  "ratchet.signal.key", "approval",
                  "ratchet.signal.outcome", "APPROVED",
                  "ratchet.signal.delivered_by.present", "true",
                  "ratchet.signal.wait_ms", "2000",
                  "deployment.region", "us-east-1"));
      scope.success(10L);

      var span = exporter.getFinishedSpanItems().get(0);
      assertEquals("approval", span.getAttributes().get(stringKey("ratchet.signal.key")));
      assertEquals("APPROVED", span.getAttributes().get(stringKey("ratchet.signal.outcome")));
      assertEquals(
          "true", span.getAttributes().get(stringKey("ratchet.signal.delivered_by.present")));
      assertEquals("2000", span.getAttributes().get(stringKey("ratchet.signal.wait_ms")));
      assertEquals("us-east-1", span.getAttributes().get(stringKey("deployment.region")));
    } finally {
      tracerProvider.close();
    }
  }

  @Test
  void failureExportsOnlySanitizedExceptionAttributes() {
    InMemorySpanExporter exporter = InMemorySpanExporter.create();
    try (SdkTracerProvider provider =
        SdkTracerProvider.builder()
            .addSpanProcessor(SimpleSpanProcessor.create(exporter))
            .build()) {
      OtelTracingCollector collector =
          new OtelTracingCollector(OpenTelemetrySdk.builder().setTracerProvider(provider).build());
      collector
          .jobExecutionStarted(
              new UUID(0, 1), JobType.SINGLE, JobPriority.NORMAL, Map.of(), Map.of())
          .failure(IllegalStateException.class.getName(), "***REDACTED***", 2);
      var span = exporter.getFinishedSpanItems().get(0);
      assertEquals(StatusCode.ERROR, span.getStatus().getStatusCode());
      assertEquals("***REDACTED***", span.getStatus().getDescription());
      assertEquals("failure", span.getAttributes().get(stringKey("ratchet.outcome")));
      assertEquals(2L, span.getAttributes().get(longKey("ratchet.attempt")));
      assertEquals(1, span.getEvents().size());
      var event = span.getEvents().get(0);
      assertEquals("exception", event.getName());
      assertEquals(
          IllegalStateException.class.getName(),
          event.getAttributes().get(stringKey("exception.type")));
      assertEquals("***REDACTED***", event.getAttributes().get(stringKey("exception.message")));
      assertNull(event.getAttributes().get(stringKey("exception.stacktrace")));
      assertNull(span.getAttributes().get(stringKey("exception.stacktrace")));
    }
  }
}
