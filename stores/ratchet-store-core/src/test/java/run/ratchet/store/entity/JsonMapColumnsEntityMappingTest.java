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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
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
  void paramsSerializeImmediatelyAndDefensivelyCopy() throws ReflectiveOperationException {
    JobEntity job = new JobEntity();
    Map<String, String> params = new HashMap<>(Map.of("key", "value"));
    job.setParams(params);
    assertEquals(params, JsonMapColumns.readStringMap(jsonColumn(job, "params")));
    assertEquals(params, job.getParams());

    params.put("key", "updated");
    assertEquals(Map.of("key", "value"), job.getParams());
    assertEquals(job.getParams(), JsonMapColumns.readStringMap(jsonColumn(job, "params")));
    assertThrows(UnsupportedOperationException.class, () -> job.getParams().put("extra", "value"));
  }

  @Test
  void traceContextSerializesImmediatelyAndDefensivelyCopies() throws ReflectiveOperationException {
    JobEntity job = new JobEntity();
    Map<String, String> traceContext = new HashMap<>(Map.of("traceparent", "parent"));
    job.setTraceContext(traceContext);
    assertEquals(traceContext, JsonMapColumns.readStringMap(jsonColumn(job, "traceContext")));
    assertEquals(traceContext, job.getTraceContext());

    traceContext.put("traceparent", "updated");
    assertEquals(Map.of("traceparent", "parent"), job.getTraceContext());
    assertEquals(
        job.getTraceContext(), JsonMapColumns.readStringMap(jsonColumn(job, "traceContext")));
    assertThrows(
        UnsupportedOperationException.class, () -> job.getTraceContext().put("extra", "value"));
  }

  @Test
  void mdcConstructorSerializesTheDefensiveCopyImmediately() throws ReflectiveOperationException {
    Map<String, Object> mdc = new LinkedHashMap<>();
    mdc.put("first", "value");
    mdc.put("second", "other");
    JobLogEntity log = logEntry(mdc);
    assertEquals(mdc, JsonMapColumns.readObjectMap(jsonColumn(log, "mdc")));
    assertEquals(mdc, log.getMdc());

    mdc.put("first", "updated");
    mdc.put("third", "extra");
    assertEquals(Map.of("first", "value", "second", "other"), log.getMdc());
    assertEquals(List.of("first", "second"), List.copyOf(log.getMdc().keySet()));
    assertEquals(log.getMdc(), JsonMapColumns.readObjectMap(jsonColumn(log, "mdc")));
    assertThrows(UnsupportedOperationException.class, () -> log.getMdc().put("extra", "value"));
  }

  @Test
  void nestedMdcValuesAreCopiedAndFrozen() throws ReflectiveOperationException {
    Map<String, Object> details = new HashMap<>(Map.of("name", "worker"));
    List<Object> tags = new ArrayList<>(List.of("a"));
    JobLogEntity log = logEntry(new HashMap<>(Map.of("details", details, "tags", tags)));

    details.put("name", "changed");
    tags.add("b");
    assertEquals(Map.of("name", "worker"), log.getMdc().get("details"));
    assertEquals(List.of("a"), log.getMdc().get("tags"));
    assertNestedValuesAreFrozen(log.getMdc());

    JobLogEntity loaded = logEntry(null);
    setJsonColumn(loaded, "mdc", jsonColumn(log, "mdc"));
    assertNestedValuesAreFrozen(loaded.getMdc());
  }

  @SuppressWarnings("unchecked")
  private static void assertNestedValuesAreFrozen(Map<String, Object> map) {
    assertThrows(
        UnsupportedOperationException.class,
        () -> ((Map<String, Object>) map.get("details")).put("name", "mutated"));
    assertThrows(
        UnsupportedOperationException.class, () -> ((List<Object>) map.get("tags")).add("b"));
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
    JobLogEntity log = new JobLogEntity();
    setJsonColumn(log, "mdc", json);

    assertEquals(expected, job.getParams());
    assertEquals(expected, job.getTraceContext());
    assertEquals(expected, log.getMdc());
    assertEquals(json, jsonColumn(job, "params"));
    assertEquals(json, jsonColumn(job, "traceContext"));
    assertEquals(json, jsonColumn(log, "mdc"));

    if (expected != null) {
      assertThrows(
          UnsupportedOperationException.class, () -> job.getParams().put("extra", "value"));
      assertThrows(
          UnsupportedOperationException.class, () -> job.getTraceContext().put("extra", "value"));
      assertThrows(UnsupportedOperationException.class, () -> log.getMdc().put("extra", "value"));
    }

    // Repeated reads of the same JSON use the cache, including null and empty maps.
    PayloadSerializer serializer = mock(PayloadSerializer.class);
    PayloadSerializerHolder.set(serializer);
    assertEquals(expected, job.getParams());
    assertEquals(expected, job.getTraceContext());
    assertEquals(expected, log.getMdc());
    verifyNoInteractions(serializer);
  }

  @Test
  void objectMapGettersDecodeMixedTypes() throws ReflectiveOperationException {
    String json = "{\"active\":true,\"count\":2,\"details\":{\"name\":\"worker\"}}";
    JobLogEntity log = new JobLogEntity();
    setJsonColumn(log, "mdc", json);

    assertEquals(OBJECT_MAP, log.getMdc());
  }

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(strings = {"{}", "{\"new\":\"value\"}"})
  void providerLoadedChangesInvalidateReadCaches(String json) throws ReflectiveOperationException {
    JobEntity job = new JobEntity();
    job.setParams(Map.of("old", "value"));
    job.setTraceContext(Map.of("old", "value"));
    JobLogEntity log = logEntry(Map.of("old", "value"));

    setJsonColumn(job, "params", json);
    setJsonColumn(job, "traceContext", json);
    setJsonColumn(log, "mdc", json);

    assertEquals(JsonMapColumns.readStringMap(json), job.getParams());
    assertEquals(JsonMapColumns.readStringMap(json), job.getTraceContext());
    assertEquals(JsonMapColumns.readObjectMap(json), log.getMdc());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void settersReplaceLoadedJsonAndCachesImmediately(boolean decodeFirst)
      throws ReflectiveOperationException {
    String json = "{\"old\":\"value\"}";
    JobEntity job = new JobEntity();
    setJsonColumn(job, "params", json);
    setJsonColumn(job, "traceContext", json);
    if (decodeFirst) {
      assertEquals(Map.of("old", "value"), job.getParams());
      assertEquals(Map.of("old", "value"), job.getTraceContext());
    }

    job.setParams(Map.of("new", "params"));
    job.setTraceContext(Map.of("new", "trace"));

    assertEquals(Map.of("new", "params"), job.getParams());
    assertEquals(job.getParams(), JsonMapColumns.readStringMap(jsonColumn(job, "params")));
    assertEquals(Map.of("new", "trace"), job.getTraceContext());
    assertEquals(
        job.getTraceContext(), JsonMapColumns.readStringMap(jsonColumn(job, "traceContext")));

    job.setParams(null);
    job.setTraceContext(null);
    assertNull(job.getParams());
    assertNull(jsonColumn(job, "params"));
    assertNull(job.getTraceContext());
    assertNull(jsonColumn(job, "traceContext"));
  }

  @Test
  void nullMdcConstructorWritesNullJson() throws ReflectiveOperationException {
    JobLogEntity log = logEntry(null);
    assertNull(log.getMdc());
    assertNull(jsonColumn(log, "mdc"));
  }

  @Test
  void emptyMapsWriteEmptyJsonObjectsImmediately() throws ReflectiveOperationException {
    JobEntity job = new JobEntity();
    job.setParams(Map.of());
    job.setTraceContext(Map.of());
    JobLogEntity log = logEntry(Map.of());

    assertEquals("{}", jsonColumn(job, "params"));
    assertEquals("{}", jsonColumn(job, "traceContext"));
    assertEquals("{}", jsonColumn(log, "mdc"));
    assertEquals(Map.of(), job.getParams());
    assertEquals(Map.of(), job.getTraceContext());
    assertEquals(Map.of(), log.getMdc());
  }

  @Test
  void malformedJsonRaisesIllegalArgumentException() throws ReflectiveOperationException {
    JobEntity job = new JobEntity();
    setJsonColumn(job, "params", "{bad}");
    setJsonColumn(job, "traceContext", "{bad}");
    JobLogEntity log = new JobLogEntity();
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

  private static JobLogEntity logEntry(Map<String, Object> mdc) {
    return new JobLogEntity(
        UUID.randomUUID(),
        Instant.parse("2026-05-07T12:00:00Z"),
        JobLogEntity.LogLevel.INFO,
        "Log",
        mdc);
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
