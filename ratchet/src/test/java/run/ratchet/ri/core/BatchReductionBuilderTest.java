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
package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import run.ratchet.api.SerializableBinaryOperator;
import run.ratchet.api.SerializableConsumer;
import run.ratchet.api.SerializableFunction;
import run.ratchet.api.TypeRef;
import run.ratchet.ri.payload.JobPayloadFactory;
import run.ratchet.spi.ClassPolicy;
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.store.entity.JobPayload;

class BatchReductionBuilderTest {
  private final ClassPolicy policy = mock(ClassPolicy.class);
  private final PayloadSerializer serializer = mock(PayloadSerializer.class);

  @Test
  void reducingMustPrecedeItemsProcessingAndSuccess() {
    var batch = new DefaultBatchBuilder("reduce", mock(BatchSubmitter.class));
    batch.forEach(List.<String>of(), BatchReductionBuilderTest::ignore);
    assertThrows(IllegalStateException.class, () -> batch.reducing(0L, Long::sum));
    var success = new DefaultBatchBuilder("reduce", mock(BatchSubmitter.class));
    success.thenOnBatchSuccess(BatchReductionBuilderTest::done);
    assertThrows(IllegalStateException.class, () -> success.reducing(0L, Long::sum));
    var streaming =
        new DefaultStreamingBatchBuilder<String>("reduce", mock(StreamingBatchSubmitter.class));
    streaming.process(BatchReductionBuilderTest::ignore);
    assertThrows(IllegalStateException.class, () -> streaming.reducing(0L, Long::sum));
    var items =
        new DefaultStreamingBatchBuilder<String>("reduce", mock(StreamingBatchSubmitter.class));
    items.fromStream(Stream.empty());
    assertThrows(IllegalStateException.class, () -> items.reducing(0L, Long::sum));
  }

  @Test
  void nullInputsAndDuplicateSuccessCallbacksAreRejected() {
    var batch = new DefaultBatchBuilder("reduce", mock(BatchSubmitter.class));
    assertThrows(NullPointerException.class, () -> batch.reducing(null, Long::sum));
    assertThrows(NullPointerException.class, () -> batch.reducing(0L, null));
    var reducing = batch.reducing(0L, Long::sum);
    reducing.thenOnBatchSuccess(BatchReductionBuilderTest::receive);
    assertThrows(
        IllegalStateException.class,
        () -> reducing.thenOnBatchSuccess(BatchReductionBuilderTest::receive));
  }

  @Test
  void jdkSumNeedsNoPolicyEntryAndIdentityMustRoundTrip() {
    when(serializer.serialize(0L)).thenReturn("0");
    when(serializer.deserialize("0", (Type) Long.class)).thenReturn(0L);
    var reducer = reducer(null, 0L, (SerializableBinaryOperator<Long>) Long::sum);
    assertDoesNotThrow(() -> reducer.validate(policy, serializer));
    verifyNoInteractions(policy);
    when(serializer.deserialize("0", (Type) Long.class))
        .thenThrow(new IllegalArgumentException("unsupported"));
    assertThrows(IllegalArgumentException.class, () -> reducer.validate(policy, serializer));
  }

  @Test
  void genericReturnTypesArePreservedAndGenericCombinersNeedTypeRef() {
    allowTargets();
    Type map = new TypeRef<Map<String, Long>>() {}.getType();
    when(serializer.serialize(Map.of())).thenReturn("{}");
    when(serializer.deserialize("{}", map)).thenReturn(Map.of());
    var maps =
        reducer(
            null,
            Map.of(),
            (SerializableBinaryOperator<Map<String, Long>>) BatchReductionBuilderTest::merge);
    assertDoesNotThrow(() -> maps.validate(policy, serializer));
    var generic =
        reducer(
            null,
            Set.of(),
            (SerializableBinaryOperator<Set<String>>) BatchReductionBuilderTest::union);
    assertTrue(
        assertThrows(IllegalArgumentException.class, () -> generic.validate(policy, serializer))
            .getMessage()
            .contains("TypeRef"));
    Type set = new TypeRef<Set<String>>() {}.getType();
    when(serializer.serialize(Set.of())).thenReturn("[]");
    when(serializer.deserialize("[]", set)).thenReturn(Set.of());
    var explicit =
        reducer(
            set,
            Set.of(),
            (SerializableBinaryOperator<Set<String>>) BatchReductionBuilderTest::union);
    assertDoesNotThrow(() -> explicit.validate(policy, serializer));
  }

  @Test
  void callbackSupertypesAndWrongChildResultsAreRejected() {
    allowTargets();
    when(serializer.serialize(0L)).thenReturn("0");
    when(serializer.deserialize("0", (Type) Long.class)).thenReturn(0L);
    var reducer = reducer(null, 0L, (SerializableBinaryOperator<Long>) Long::sum);
    reducer.callback(
        JobPayloadFactory.fromLambda(
            (SerializableConsumer<Number>) BatchReductionBuilderTest::receiveNumber));
    assertThrows(IllegalArgumentException.class, () -> reducer.validate(policy, serializer));
    var valid = reducer(null, 0L, (SerializableBinaryOperator<Long>) Long::sum);
    valid.validate(policy, serializer);
    JobPayload wrong =
        JobPayloadFactory.fromLambda(
            (SerializableFunction<String, String>) BatchReductionBuilderTest::text);
    assertThrows(IllegalArgumentException.class, () -> valid.validateChild(wrong, policy));
  }

  @Test
  @SuppressWarnings({"rawtypes", "unchecked"})
  void rawTypeRefIsRejected() {
    assertThrows(IllegalStateException.class, () -> new TypeRef() {});
  }

  @Test
  void typeNamesRoundTripAndPolicyRunsBeforeClassLoading() {
    Type type = new TypeRef<Map<String, List<Set<Long>>>>() {}.getType();
    assertEquals(type, BatchResultTypes.parse(type.getTypeName(), policy));
    for (String invalid :
        List.of(
            "java.lang.String[]", "java.util.Set<?>", "T", "java.util.Set<java.lang.String>[]")) {
      assertThrows(IllegalArgumentException.class, () -> BatchResultTypes.parse(invalid, policy));
    }
    assertThrows(
        IllegalArgumentException.class,
        () -> BatchResultTypes.parse("not.loadable.Denied", policy));
    verify(policy).isAllowed("not.loadable.Denied");
    verify(policy, never()).isAllowedForResultType("not.loadable.Denied");
  }

  private void allowTargets() {
    when(policy.isAllowed(BatchReductionBuilderTest.class.getName())).thenReturn(true);
  }

  private static BatchReducer reducer(
      Type type, Object identity, SerializableBinaryOperator<?> combiner) {
    return new BatchReducer(type, identity, JobPayloadFactory.fromLambda(combiner));
  }

  public static Map<String, Long> merge(Map<String, Long> a, Map<String, Long> b) {
    return a;
  }

  public static <T> Set<T> union(Set<T> a, Set<T> b) {
    return a;
  }

  public static void receive(Long value) {}

  public static void receiveNumber(Number value) {}

  public static void done() {}

  public static void ignore(String value) {}

  public static String text(String value) {
    return value;
  }
}
