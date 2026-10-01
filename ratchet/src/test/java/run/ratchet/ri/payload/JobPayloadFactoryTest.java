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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobContext;
import run.ratchet.api.SerializableBiConsumer;
import run.ratchet.api.SerializableCheckedRunnable;
import run.ratchet.api.SerializableConsumer;
import run.ratchet.api.SerializablePredicate;
import run.ratchet.store.entity.JobPayload;

class JobPayloadFactoryTest {
  public static final class Item implements Serializable {
    public void noArg() {}

    public void withArg(int value) {}

    public boolean isOk() {
      return true;
    }
  }

  @Test
  void batchParameterReceiversAreRejectedForBothLambdaAndReference() {
    Item item = new Item();
    for (SerializableConsumer<Item> action :
        List.<SerializableConsumer<Item>>of(
            value -> value.noArg(), value -> value.withArg(1), Item::noArg)) {
      var error =
          assertThrows(
              IllegalArgumentException.class,
              () -> JobPayloadFactory.toInvocation(action, List.of(item)));
      assertTrue(error.getMessage().contains("batch action calls a method on its item parameter"));
      assertTrue(error.getMessage().contains("item -> service.refresh(item)"));
    }
    var invocation =
        JobPayloadFactory.toInvocation(
            (SerializableConsumer<Item>) value -> duplicateItem(value, value), List.of(item));
    assertEquals(List.of(item, item), invocation.arguments());
    assertEquals(null, invocation.runtimeArgIndexes());
  }

  public static void duplicateItem(Item first, Item second) {}

  @Test
  void conditionParameterReceiverIsAllowedOnlyWithoutArguments() {
    for (SerializablePredicate<Item> predicate :
        List.<SerializablePredicate<Item>>of(value -> value.isOk(), Item::isOk)) {
      JobPayload payload = JobPayloadFactory.fromConditionLambda(predicate);
      assertEquals(List.of(), payload.args());
      assertEquals(null, payload.runtimeArgIndexes());
    }
    var error =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                JobPayloadFactory.fromConditionLambda(
                    (SerializableConsumer<Item>) value -> value.withArg(1)));
    assertTrue(error.getMessage().contains("condition calls a method on its parameter"));
  }

  @Test
  void callbackContextReceiverIsRejected() {
    var error =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                JobPayloadFactory.fromLambda(
                    (SerializableConsumer<JobContext>) ctx -> ctx.jobId()));
    assertTrue(error.getMessage().contains("callback calls a method on its own parameter"));
  }

  @Test
  void indexedBindingPreservesReorderingAndDuplicateUsesWithoutArityOverride() {
    SerializableBiConsumer<String, String> reordered =
        (first, second) -> PayloadTarget.capture(second, first);
    assertEquals(
        List.of("b", "a"),
        JobPayloadFactory.toInvocation(reordered, List.of("a", "b")).arguments());
    SerializableBiConsumer<String, String> duplicate =
        (first, second) -> PayloadTarget.capture(first, first);
    assertEquals(
        List.of("a", "a"),
        JobPayloadFactory.toInvocation(duplicate, List.of("a", "b")).arguments());
    SerializableConsumer<String> withPlainNull = item -> PayloadTarget.capture(null, item);
    assertEquals(
        List.of("fallback", "item"),
        JobPayloadFactory.toInvocation(withPlainNull, List.of("item", "fallback")).arguments());
    SerializableConsumer<String> ignoresItem = item -> PayloadTarget.noArgs();
    var noArgs = JobPayloadFactory.toInvocation(ignoresItem, List.of("unused"));
    assertEquals(List.of(), noArgs.arguments());
    assertEquals(null, noArgs.runtimeArgIndexes());
  }

  @Test
  void callbackParametersArePersistedAsRuntimeSlots() {
    assertRuntimeCallback((ctx, err) -> PayloadTarget.record(ctx, err), List.of(0, 1));
    assertRuntimeCallback(PayloadTarget::record, List.of(0, 1));
    assertRuntimeCallback(new PayloadTarget()::boundRecord, List.of(0, 1));
    assertRuntimeCallback((ctx, err) -> PayloadTarget.record2(err, ctx), List.of(1, 0));
  }

  private void assertRuntimeCallback(
      SerializableBiConsumer<JobContext, Throwable> callback, List<Integer> indexes) {
    JobPayload payload = JobPayloadFactory.fromLambda(callback);
    assertEquals(Arrays.asList(null, null), payload.args());
    assertEquals(indexes, payload.runtimeArgIndexes());
    assertEquals(
        Arrays.asList(null, null),
        Arrays.asList(new AsmLambdaAnalyzer().analyze(callback).capturedArgs()));
  }

  @Test
  void capturedValuesAndRuntimeParametersKeepTheirPositions() {
    String orderId = "order-42";
    JobPayload payload =
        JobPayloadFactory.fromLambda(
            (SerializableBiConsumer<JobContext, Throwable>)
                (ctx, err) -> PayloadTarget.withKey(orderId, err));
    assertEquals(Arrays.asList(orderId, null), payload.args());
    assertEquals(Arrays.asList(null, 1), payload.runtimeArgIndexes());
  }

  @Test
  void successCallbackSupportsInlineAndMethodReference() {
    for (SerializableConsumer<JobContext> callback :
        List.<SerializableConsumer<JobContext>>of(
            ctx -> PayloadTarget.success(ctx), PayloadTarget::success)) {
      JobPayload payload = JobPayloadFactory.fromLambda(callback);
      assertEquals(Arrays.asList((Object) null), payload.args());
      assertEquals(List.of(0), payload.runtimeArgIndexes());
    }
  }

  @Test
  void forEachArgumentsAreBoundByIndexAtCreation() {
    for (SerializableConsumer<String> callback :
        List.<SerializableConsumer<String>>of(
            item -> PayloadTarget.process(item),
            PayloadTarget::process,
            new PayloadTarget()::boundProcess)) {
      var invocation = JobPayloadFactory.toInvocation(callback, List.of("x"));
      assertEquals(List.of("x"), invocation.arguments());
      assertEquals(null, invocation.runtimeArgIndexes());
    }
  }

  @Test
  void nestedAdaptersPreserveRuntimeIndexesAndSubstitutedNulls() {
    SerializableBiConsumer<JobContext, Throwable> target = PayloadTarget::record;
    SerializableBiConsumer<JobContext, Throwable> wrapper = (ctx, err) -> target.accept(ctx, err);
    assertRuntimeCallback(wrapper, List.of(0, 1));
    SerializableBiConsumer<Throwable, JobContext> reversedTarget = PayloadTarget::record2;
    SerializableBiConsumer<JobContext, Throwable> reversedWrapper =
        (ctx, err) -> reversedTarget.accept(err, ctx);
    assertRuntimeCallback(reversedWrapper, List.of(1, 0));
    SerializableConsumer<String> stringTarget = PayloadTarget::process;
    SerializableConsumer<String> stringWrapper = item -> stringTarget.accept(item);
    var invocation = JobPayloadFactory.toInvocation(stringWrapper, Arrays.asList((Object) null));
    assertEquals(Arrays.asList((Object) null), invocation.arguments());
    assertEquals(null, invocation.runtimeArgIndexes());
  }

  @Test
  void parameterReceiversAndMultipleCallsGiveCallbackGuidance() {
    var receiverError =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                JobPayloadFactory.fromLambda(
                    (SerializableBiConsumer<JobContext, Throwable>)
                        (ctx, err) -> err.printStackTrace()));
    assertTrue(receiverError.getMessage().contains("own parameter"));
    assertTrue(receiverError.getMessage().contains("method reference"));
    var multiError =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                JobPayloadFactory.fromLambda(
                    (SerializableBiConsumer<JobContext, Throwable>)
                        (ctx, err) -> PayloadTarget.process(ctx.jobId().toString())));
    assertTrue(multiError.getMessage().contains("method reference"));
    assertTrue(multiError.getMessage().contains("parameters straight"));
  }

  @Test
  void constructorArgumentDoesNotShiftEarlierCapturedArguments() {
    String first = "first";

    JobPayload payload =
        JobPayloadFactory.fromLambda(
            (SerializableCheckedRunnable)
                () -> PayloadTarget.capture(first, new String("constructed")),
            List.of("second"));

    assertEquals(PayloadTarget.class.getName(), payload.target());
    assertEquals("capture", payload.method());
    assertEquals(List.of(first, "second"), payload.args());
  }

  @Test
  void reflectionLookupsAreCachedAcrossRepeatedConversions() throws Exception {
    StringFunction target = PayloadTarget::uppercase;
    StringFunction wrapper = value -> target.apply("cached");
    // The ClassValue caches are static and shared across tests; start from empty
    // per-class maps so the size assertions below measure only this test's work.
    ConcurrentMap<?, ?> visibility = cachedMap("VISIBILITY_CACHE", PayloadTarget.class);
    ConcurrentMap<?, ?> functional =
        cachedMap("FUNCTIONAL_INTERFACE_METHOD_CACHE", StringFunction.class);
    visibility.clear();
    functional.clear();

    JobPayload first = JobPayloadFactory.fromLambda(wrapper);
    assertEquals(1, visibility.size(), "first conversion must memoize one visibility verdict");
    assertEquals(
        1, functional.size(), "first conversion must memoize one functional-interface lookup");
    JobPayload second = JobPayloadFactory.fromLambda(wrapper);

    assertEquals(PayloadTarget.class.getName(), first.target());
    assertEquals(List.of("cached"), first.args());
    assertEquals(List.of("cached"), second.args());
    assertEquals(1, visibility.size(), "repeat conversions must reuse the memoized verdict");
    assertEquals(1, functional.size(), "repeat conversions must reuse the memoized lookup");
  }

  @Test
  void lambdaSerializationRejectsSerializableReplacementObjectsWithClearMessage() {
    IllegalStateException thrown =
        assertThrows(
            IllegalStateException.class,
            () ->
                LambdaSerialization.toSerializedLambda(
                    new SerializableReplacement(), "Expected a serializable lambda"));

    assertTrue(thrown.getMessage().contains("Expected a serializable lambda"));
    assertInstanceOf(ClassCastException.class, thrown.getCause());
  }

  private static ConcurrentMap<?, ?> cachedMap(String name, Class<?> keyedOn)
      throws ReflectiveOperationException {
    Field field = JobPayloadFactory.class.getDeclaredField(name);
    field.setAccessible(true);
    ClassValue<?> cache = (ClassValue<?>) field.get(null);
    return (ConcurrentMap<?, ?>) cache.get(keyedOn);
  }

  @FunctionalInterface
  interface StringFunction extends Serializable {
    String apply(String value);
  }

  private static final class SerializableReplacement implements Serializable {
    @SuppressWarnings("unused")
    private Object writeReplace() {
      return "replacement";
    }
  }

  public static final class PayloadTarget {
    public static void noArgs() {}

    public static void record(JobContext ctx, Throwable error) {}

    public static void record2(Throwable error, JobContext ctx) {}

    public static void withKey(String key, Throwable error) {}

    public static void success(JobContext ctx) {}

    public static void process(String item) {}

    public void boundRecord(JobContext ctx, Throwable error) {}

    public void boundProcess(String item) {}

    public static void capture(String first, String second) {}

    public static String uppercase(String value) {
      return value.toUpperCase();
    }
  }
}
