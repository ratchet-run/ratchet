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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.function.IntBinaryOperator;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobContext;
import run.ratchet.api.SerializableBiConsumer;
import run.ratchet.api.SerializableCheckedRunnable;
import run.ratchet.api.SerializableConsumer;
import run.ratchet.ri.payload.AsmLambdaAnalyzer.RuntimeParameter;

class AsmLambdaAnalyzerTest {
  public record Order(String id, int quantity) {}

  public record Envelope(Order order) {}

  public static final class Pojo {
    private final String label;

    public Pojo(String label) {
      this.label = label;
    }

    public String getLabel() {
      return label;
    }
  }

  public enum Mode {
    FAST
  }

  public static final String STATIC_REFERENCE = new String("static");
  private final String field = new String("field");

  public static void order(Order value) {}

  public static void envelope(Envelope value) {}

  public static void pojo(Pojo value) {}

  public static void text(String value) {}

  public static void mode(Mode value) {}

  @Test
  void inlineRecordAndCapturedConstructorArgumentsAreResolved() {
    String id = "o-1";
    SerializableCheckedRunnable task = () -> order(new Order(id, 3));
    assertEquals(List.of(new Order(id, 3)), JobPayloadFactory.fromLambda(task).args());
    assertEquals(new Order(id, 3), new AsmLambdaAnalyzer().analyze(task).capturedArgs()[0]);
  }

  @Test
  void inlinePojoAndNestedConstructionAreResolved() {
    assertEquals(
        "label",
        ((Pojo)
                JobPayloadFactory.fromLambda(
                        (SerializableCheckedRunnable) () -> pojo(new Pojo("label")))
                    .args()
                    .get(0))
            .getLabel());
    assertEquals(
        List.of(new Envelope(new Order("nested", 2))),
        JobPayloadFactory.fromLambda(
                (SerializableCheckedRunnable) () -> envelope(new Envelope(new Order("nested", 2))))
            .args());
  }

  @Test
  void enumConstantUsesItsPersistedName() {
    assertEquals(
        List.of("FAST"),
        JobPayloadFactory.fromLambda((SerializableCheckedRunnable) () -> mode(Mode.FAST)).args());
  }

  @Test
  void staticReferenceAndInstanceFieldFailInsteadOfBecomingNull() {
    assertUnresolved((SerializableCheckedRunnable) () -> text(STATIC_REFERENCE));
    assertUnresolved((SerializableCheckedRunnable) () -> text(field));
  }

  @Test
  void unresolvedNestedAdapterSlotCannotBeReplacedByWrapperArgument() {
    SerializableConsumer<String> target = ignored -> text(field);
    SerializableCheckedRunnable wrapper = () -> target.accept("replacement");
    assertUnresolved(wrapper);
  }

  @Test
  void literalNullRemainsNullIncludingInsideAnAdapter() {
    SerializableConsumer<String> target = ignored -> text(null);
    assertEquals(
        Arrays.asList((Object) null),
        JobPayloadFactory.fromLambda(
                (SerializableCheckedRunnable) () -> target.accept("replacement"))
            .args());
  }

  @Test
  void callResultKeepsTheSingleInvocationDiagnostic() {
    String label = " label ";
    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                JobPayloadFactory.fromLambda(
                    (SerializableCheckedRunnable) () -> text(label.trim())));
    assertTrue(error.getMessage().contains("requires exactly one method invocation"));
    assertTrue(error.getMessage().contains("Found 2 invocations"));
  }

  private static void assertUnresolved(Serializable task) {
    IllegalArgumentException error =
        assertThrows(IllegalArgumentException.class, () -> JobPayloadFactory.fromLambda(task));
    assertTrue(error.getMessage().contains("argument 1"));
    assertTrue(error.getMessage().contains("java.lang.String"));
    assertTrue(error.getMessage().contains(".text"));
    assertTrue(error.getMessage().contains("local variable"));
    assertThrows(IllegalArgumentException.class, () -> new AsmLambdaAnalyzer().analyze(task));
  }

  public static final class ThrowingArgument {
    public ThrowingArgument() {
      throw new IllegalStateException("constructor failure");
    }
  }

  public static final class CountedArgument {
    static int constructions;

    public CountedArgument() {
      constructions++;
    }
  }

  public static void throwing(ThrowingArgument value) {}

  public static void counted(CountedArgument value) {}

  private static void hidden(CountedArgument value) {}

  @Test
  void constructorFailureNamesTheTypeAndPreservesTheCause() {
    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                JobPayloadFactory.fromLambda(
                    (SerializableCheckedRunnable) () -> throwing(new ThrowingArgument())));
    assertTrue(error.getMessage().contains(ThrowingArgument.class.getName()));
    assertEquals("constructor failure", error.getCause().getMessage());
  }

  @Test
  void cheapRejectionsDoNotRunConstructors() {
    CountedArgument.constructions = 0;
    assertThrows(
        IllegalArgumentException.class,
        () ->
            JobPayloadFactory.fromLambda(
                (SerializableCheckedRunnable) () -> hidden(new CountedArgument())));
    assertEquals(0, CountedArgument.constructions);
    assertThrows(
        IllegalArgumentException.class,
        () ->
            JobPayloadFactory.fromLambda(
                (SerializableCheckedRunnable)
                    () -> {
                      counted(new CountedArgument());
                      text("second");
                    }));
    assertEquals(0, CountedArgument.constructions);
  }

  @Test
  void capturedArgumentWithAnUnknownFieldReceiverStillWorks() {
    String context = "captured";
    assertEquals(
        List.of(context),
        JobPayloadFactory.fromLambda(
                (SerializableCheckedRunnable) () -> instanceTarget.label(context))
            .args());
  }

  private static boolean staticOwnerInitialized;
  private static boolean enumInitialized;

  public static final class UninitializedStatics {
    public static final String VALUE;

    static {
      staticOwnerInitialized = true;
      VALUE = "value";
    }
  }

  public enum UninitializedMode {
    FAST;

    static {
      enumInitialized = true;
    }
  }

  public static void lazyMode(UninitializedMode mode) {}

  @Test
  void metadataInspectionDoesNotInitializeStaticOwnersOrEnums() {
    assertUnresolved((SerializableCheckedRunnable) () -> text(UninitializedStatics.VALUE));
    assertEquals(false, staticOwnerInitialized);
    assertEquals(
        List.of("FAST"),
        JobPayloadFactory.fromLambda(
                (SerializableCheckedRunnable) () -> lazyMode(UninitializedMode.FAST))
            .args());
    assertEquals(false, enumInitialized);
  }

  @Test
  void runtimeArgumentsAndNullConstructorArgumentsRemainDistinct() {
    assertEquals(
        List.of(new Order(null, 3)),
        JobPayloadFactory.fromLambda((SerializableCheckedRunnable) () -> order(new Order(null, 3)))
            .args());
    SerializableConsumer<Order> callback = value -> order(value);
    assertEquals(Arrays.asList((Object) null), JobPayloadFactory.fromLambda(callback).args());
    assertEquals(List.of(0), JobPayloadFactory.fromLambda(callback).runtimeArgIndexes());
  }

  public record Pair(Order first, Order second) {}

  public record PrimitiveArgument(long number, double fraction, boolean enabled, char code) {}

  public static void pair(Pair value) {}

  public static void primitiveArgument(PrimitiveArgument value) {}

  @Test
  void distinctAllocationsKeepTheirIdentityAndWideConstructorArgumentsKeepTheirSlots() {
    assertEquals(
        List.of(new Pair(new Order("first", 1), new Order("second", 2))),
        JobPayloadFactory.fromLambda(
                (SerializableCheckedRunnable)
                    () -> pair(new Pair(new Order("first", 1), new Order("second", 2))))
            .args());
    assertEquals(
        List.of(new PrimitiveArgument(42L, 0.5, true, 'a')),
        JobPayloadFactory.fromLambda(
                (SerializableCheckedRunnable)
                    () -> primitiveArgument(new PrimitiveArgument(42L, 0.5, true, 'a')))
            .args());
  }

  public record EnumArgument(Mode mode) {}

  public static void enumArgument(EnumArgument value) {}

  @Test
  void enumConstructorArgumentsAreResolvedWhenTheObjectIsConstructed() {
    assertEquals(
        List.of(new EnumArgument(Mode.FAST)),
        JobPayloadFactory.fromLambda(
                (SerializableCheckedRunnable) () -> enumArgument(new EnumArgument(Mode.FAST)))
            .args());
  }

  public static void twoCounted(CountedArgument first, CountedArgument second) {}

  @Test
  void localAliasesOfAConstructedObjectAreEvaluatedOnlyOnce() {
    CountedArgument.constructions = 0;
    var payload =
        JobPayloadFactory.fromLambda(
            (SerializableCheckedRunnable)
                () -> {
                  CountedArgument value = new CountedArgument();
                  twoCounted(value, value);
                });
    assertSame(payload.args().get(0), payload.args().get(1));
    assertEquals(1, CountedArgument.constructions);
  }

  public static void wideString(long a, double b, String value) {}

  @Test
  void wideCapturesBeforeOneParameterKeepIndexZero() {
    long a = Long.parseLong("42");
    double b = Double.parseDouble("0.5");
    SerializableConsumer<String> callback = value -> wideString(a, b, value);
    var invocation = JobPayloadFactory.toInvocation(callback);
    assertEquals(Arrays.asList(a, b, null), invocation.arguments());
    assertEquals(Arrays.asList(null, null, 0), invocation.runtimeArgIndexes());
  }

  private final Target instanceTarget = new Target();

  @Test
  void nonStaticSyntheticLambdaSkipsTheCapturedThisSlot() {
    SerializableConsumer<JobContext> callback = ctx -> instanceTarget.success(ctx);
    var step =
        AsmLambdaAnalyzer.inspect(
                LambdaSerialization.toSerializedLambda(callback, "Expected lambda"))
            .last();
    assertEquals(List.of(new RuntimeParameter(0)), step.arguments());
  }

  @Test
  void inlineParametersAndReceiversRemainDistinctFromUnknownValues() {
    SerializableBiConsumer<JobContext, Throwable> callback =
        (ctx, error) -> Target.record(ctx, error);
    var step =
        AsmLambdaAnalyzer.inspect(
                LambdaSerialization.toSerializedLambda(callback, "Expected lambda"))
            .last();
    assertEquals(List.of(new RuntimeParameter(0), new RuntimeParameter(1)), step.arguments());
    assertEquals(
        Arrays.asList(null, null),
        Arrays.asList(new AsmLambdaAnalyzer().analyze(callback).capturedArgs()));

    assertEquals(List.of(0, 1), new AsmLambdaAnalyzer().analyze(callback).runtimeArgIndexes());

    SerializableConsumer<JobContext> receiver = ctx -> ctx.jobId();
    var receiverStep =
        AsmLambdaAnalyzer.inspect(
                LambdaSerialization.toSerializedLambda(receiver, "Expected lambda"))
            .last();
    assertEquals(new RuntimeParameter(0), receiverStep.receiver());
  }

  @Test
  void wideCapturesAndFunctionalParametersUseTheirActualLocalSlots() {
    long capturedLong = Long.parseLong("42");
    double capturedDouble = Double.parseDouble("0.5");
    WideCallback callback =
        (number, fraction, ctx) -> Target.wide(capturedLong, capturedDouble, number, fraction, ctx);
    var step =
        AsmLambdaAnalyzer.inspect(
                LambdaSerialization.toSerializedLambda(callback, "Expected lambda"))
            .last();
    assertEquals(
        List.of(
            capturedLong,
            capturedDouble,
            new RuntimeParameter(0),
            new RuntimeParameter(1),
            new RuntimeParameter(2)),
        step.arguments());
  }

  @Test
  void unboundMethodReferenceKeepsItsImplicitReceiverAndShiftsTargetParameters() {
    UnboundCallback callback = Target::instance;
    var step =
        AsmLambdaAnalyzer.inspect(
                LambdaSerialization.toSerializedLambda(callback, "Expected lambda"))
            .last();
    assertEquals(new RuntimeParameter(0), step.receiver());
    assertEquals(List.of(new RuntimeParameter(1)), step.arguments());
    UnboundPredicate predicate = Target::isOk;
    var predicateStep =
        AsmLambdaAnalyzer.inspect(
                LambdaSerialization.toSerializedLambda(predicate, "Expected lambda"))
            .last();
    assertEquals(new RuntimeParameter(0), predicateStep.receiver());
    assertEquals(List.of(), predicateStep.arguments());
  }

  @FunctionalInterface
  interface WideCallback extends Serializable {
    void accept(long number, double fraction, JobContext ctx);
  }

  @FunctionalInterface
  interface UnboundCallback extends Serializable {
    void accept(Target target, JobContext ctx);
  }

  @FunctionalInterface
  interface UnboundPredicate extends Serializable {
    boolean test(Target target);
  }

  public static final class Target {
    public static void record(JobContext ctx, Throwable error) {}

    public static void wide(
        long capturedLong, double capturedDouble, long number, double fraction, JobContext ctx) {}

    public void instance(JobContext ctx) {}

    public void success(JobContext ctx) {}

    public void label(String value) {}

    public boolean isOk() {
      return true;
    }
  }

  @Test
  void operandStackUnderflowReportsMalformedBytecode() throws Exception {
    Method binaryOpInt =
        AsmLambdaAnalyzer.class.getDeclaredMethod(
            "binaryOpInt", Deque.class, IntBinaryOperator.class);
    binaryOpInt.setAccessible(true);

    AsmLambdaAnalyzer.UnsupportedLambdaBytecodeException exception =
        assertThrows(
            AsmLambdaAnalyzer.UnsupportedLambdaBytecodeException.class, () -> invoke(binaryOpInt));

    assertTrue(exception.getMessage().contains("operand stack underflow"));
  }

  @Test
  void binaryOpInt_divideByZeroPushesUnknownValue() throws Throwable {
    Method binaryOpInt =
        AsmLambdaAnalyzer.class.getDeclaredMethod(
            "binaryOpInt", Deque.class, IntBinaryOperator.class);
    binaryOpInt.setAccessible(true);
    Deque<Object> stack = new ArrayDeque<>();
    stack.push(constantValue(4));
    stack.push(constantValue(0));

    invoke(binaryOpInt, stack, (IntBinaryOperator) (left, right) -> left / right);

    assertEquals("INSTANCE", ((Enum<?>) stack.peek()).name());
  }

  private static void invoke(Method method) throws Throwable {
    invoke(method, new ArrayDeque<>(), (IntBinaryOperator) Integer::sum);
  }

  private static void invoke(Method method, Deque<?> stack, IntBinaryOperator operator)
      throws Throwable {
    try {
      method.invoke(null, stack, operator);
    } catch (InvocationTargetException e) {
      throw e.getCause();
    }
  }

  private static Object constantValue(Object value) throws ReflectiveOperationException {
    Class<?> constantValue =
        Class.forName("run.ratchet.ri.payload.AsmLambdaAnalyzer$ConstantValue");
    Constructor<?> constructor = constantValue.getDeclaredConstructor(Object.class);
    constructor.setAccessible(true);
    return constructor.newInstance(value);
  }
}
