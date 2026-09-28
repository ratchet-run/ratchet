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
package run.ratchet.store.entity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.store.converter.JsonMapColumns;
import run.ratchet.store.converter.PayloadSerializerHolder;

class JsonMapColumnsEntityMappingTest {

  private static final Map<String, Object> OBJECT_MAP =
      Map.of("active", true, "count", new BigDecimal("2"), "details", Map.of("name", "worker"));

  @BeforeEach
  @AfterEach
  void resetSerializer() {
    PayloadSerializerHolder.set(null);
  }

  @Test
  void paramsSerializeOnPersistAndUpdate() throws ReflectiveOperationException {
    JobEntity job = requiredJob();
    Map<String, String> params = new HashMap<>(Map.of("key", "value"));
    job.setParams(params);
    assertSame(params, job.getParams());
    assertNull(jsonColumn(job, "params"));

    job.prePersist();
    assertEquals(params, JsonMapColumns.readStringMap(jsonColumn(job, "params")));

    params.put("key", "updated");
    assertEquals("value", JsonMapColumns.readStringMap(jsonColumn(job, "params")).get("key"));
    job.preUpdate();
    assertEquals(params, JsonMapColumns.readStringMap(jsonColumn(job, "params")));

    job.setParams(null);
    job.preUpdate();
    assertNull(jsonColumn(job, "params"));
  }

  @Test
  void traceContextSerializesOnPersistAndUpdate() throws ReflectiveOperationException {
    JobEntity job = requiredJob();
    Map<String, String> traceContext = new HashMap<>(Map.of("traceparent", "parent"));
    job.setTraceContext(traceContext);
    assertSame(traceContext, job.getTraceContext());
    assertNull(jsonColumn(job, "traceContext"));

    job.prePersist();
    assertEquals(traceContext, JsonMapColumns.readStringMap(jsonColumn(job, "traceContext")));

    traceContext.put("traceparent", "updated");
    job.preUpdate();
    assertEquals(traceContext, JsonMapColumns.readStringMap(jsonColumn(job, "traceContext")));

    job.setTraceContext(null);
    job.preUpdate();
    assertNull(jsonColumn(job, "traceContext"));
  }

  @Test
  void mdcSerializesTheDefensiveCopy() throws ReflectiveOperationException {
    Map<String, Object> mdc = new HashMap<>(OBJECT_MAP);
    JobLogEntity log = logEntry(mdc);
    assertNull(jsonColumn(log, "mdc"));
    mdc.put("count", new BigDecimal("3"));

    log.syncJsonMaps();
    assertEquals(OBJECT_MAP, JsonMapColumns.readObjectMap(jsonColumn(log, "mdc")));
    assertEquals(OBJECT_MAP, log.getMdc());
    assertThrows(UnsupportedOperationException.class, () -> log.getMdc().put("extra", "value"));
  }

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(strings = {"null", "{}", "{\"key\":\"value\"}"})
  void gettersDecodeLoadedJsonIncludingNull(String json) throws ReflectiveOperationException {
    Map<String, String> expected = null;
    if (json != null && !json.isEmpty() && !json.equals("null")) {
      expected = json.equals("{}") ? Map.of() : Map.of("key", "value");
    }
    JobEntity job = new JobEntity();
    setJsonColumn(job, "params", json);
    setJsonColumn(job, "traceContext", json);
    JobLogEntity log = unloadedLog();
    setJsonColumn(log, "mdc", json);

    assertEquals(expected, job.getParams());
    assertEquals(expected, job.getTraceContext());
    assertEquals(expected, log.getMdc());

    // Once decoded (including null), getters must use the cached map.
    setJsonColumn(job, "params", "{bad}");
    setJsonColumn(job, "traceContext", "{bad}");
    setJsonColumn(log, "mdc", "{bad}");
    assertEquals(expected, job.getParams());
    assertEquals(expected, job.getTraceContext());
    assertEquals(expected, log.getMdc());
  }

  @Test
  void objectMapGettersDecodeMixedTypes() throws ReflectiveOperationException {
    String json = "{\"active\":true,\"count\":2,\"details\":{\"name\":\"worker\"}}";
    JobLogEntity log = unloadedLog();
    setJsonColumn(log, "mdc", json);

    assertEquals(OBJECT_MAP, log.getMdc());
    assertThrows(UnsupportedOperationException.class, () -> log.getMdc().put("extra", "value"));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void nullMapsClearJsonInLifecycleCallbacks(boolean update) throws ReflectiveOperationException {
    JobEntity job = requiredJob();
    setJsonColumn(job, "params", "{\"old\":\"value\"}");
    setJsonColumn(job, "traceContext", "{\"old\":\"value\"}");
    job.setParams(null);
    job.setTraceContext(null);
    assertNull(job.getParams());
    assertNull(job.getTraceContext());
    if (update) {
      job.preUpdate();
    } else {
      job.prePersist();
    }
    assertNull(jsonColumn(job, "params"));
    assertNull(jsonColumn(job, "traceContext"));

    JobLogEntity log = logEntry(null);
    setJsonColumn(log, "mdc", "{\"old\":\"value\"}");
    assertNull(log.getMdc());
    log.syncJsonMaps();
    assertNull(jsonColumn(log, "mdc"));
  }

  @Test
  void lifecycleCallbacksPreserveUndecodedJson() throws ReflectiveOperationException {
    String json = " {\"key\":\"value\"} ";
    JobEntity job = requiredJob();
    setJsonColumn(job, "params", json);
    setJsonColumn(job, "traceContext", json);
    job.prePersist();
    assertEquals(json, jsonColumn(job, "params"));
    assertEquals(json, jsonColumn(job, "traceContext"));
    job.preUpdate();
    assertEquals(json, jsonColumn(job, "params"));
    assertEquals(json, jsonColumn(job, "traceContext"));

    JobLogEntity log = unloadedLog();
    setJsonColumn(log, "mdc", json);
    log.syncJsonMaps();
    assertEquals(json, jsonColumn(log, "mdc"));
  }

  @Test
  void settersAndConstructorDoNotInvokeTheSerializer() {
    PayloadSerializer serializer = mock(PayloadSerializer.class);
    PayloadSerializerHolder.set(serializer);
    JobEntity job = new JobEntity();
    job.setParams(Map.of("key", "value"));
    job.setTraceContext(Map.of("traceparent", "parent"));
    JobLogEntity log = logEntry(OBJECT_MAP);

    assertEquals(Map.of("key", "value"), job.getParams());
    assertEquals(Map.of("traceparent", "parent"), job.getTraceContext());
    assertEquals(OBJECT_MAP, log.getMdc());
    verifyNoInteractions(serializer);
  }

  @Test
  void malformedJsonRaisesIllegalArgumentException() throws ReflectiveOperationException {
    JobEntity job = new JobEntity();
    setJsonColumn(job, "params", "{bad}");
    setJsonColumn(job, "traceContext", "{bad}");
    JobLogEntity log = unloadedLog();
    setJsonColumn(log, "mdc", "{bad}");

    assertThrows(IllegalArgumentException.class, job::getParams);
    assertThrows(IllegalArgumentException.class, job::getTraceContext);
    assertThrows(IllegalArgumentException.class, log::getMdc);
  }

  @Test
  void stringMapsRejectNonStringEntries() throws ReflectiveOperationException {
    JobEntity job = new JobEntity();
    setJsonColumn(job, "params", "{\"key\":1}");
    setJsonColumn(job, "traceContext", "{\"key\":1}");

    assertTrue(
        assertThrows(IllegalArgumentException.class, job::getParams)
            .getMessage()
            .startsWith("JSON map column contains non-String entry"));
    assertTrue(
        assertThrows(IllegalArgumentException.class, job::getTraceContext)
            .getMessage()
            .startsWith("JSON map column contains non-String entry"));
  }

  private static JobEntity requiredJob() {
    JobEntity job = new JobEntity();
    job.setScheduledTime(Instant.parse("2026-05-07T12:00:00Z"));
    job.setJobType(JobExecutionType.SINGLE);
    job.setPayload(new JobPayload("com.example.Job", "run", "()V", false, List.of()));
    job.setIdempotencyKey("idem-1");
    return job;
  }

  private static JobLogEntity logEntry(Map<String, Object> mdc) {
    return new JobLogEntity(
        UUID.randomUUID(),
        Instant.parse("2026-05-07T12:00:00Z"),
        JobLogEntity.LogLevel.INFO,
        "Log",
        mdc);
  }

  private static JobLogEntity unloadedLog() throws ReflectiveOperationException {
    var constructor = JobLogEntity.class.getDeclaredConstructor();
    constructor.setAccessible(true);
    return constructor.newInstance();
  }

  private static String jsonColumn(Object entity, String name) throws ReflectiveOperationException {
    Field field = entity.getClass().getDeclaredField(name);
    assertEquals(String.class, field.getType());
    field.setAccessible(true);
    return (String) field.get(entity);
  }

  private static void setJsonColumn(Object entity, String name, String json)
      throws ReflectiveOperationException {
    Field field = entity.getClass().getDeclaredField(name);
    assertEquals(String.class, field.getType());
    field.setAccessible(true);
    field.set(entity, json);
  }
}
