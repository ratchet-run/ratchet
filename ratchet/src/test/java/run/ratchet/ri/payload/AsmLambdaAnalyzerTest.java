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
import run.ratchet.api.SerializableConsumer;
import run.ratchet.ri.payload.AsmLambdaAnalyzer.RuntimeParameter;

class AsmLambdaAnalyzerTest {

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
    assertEquals(null, step.receiver());
    assertEquals(List.of(new RuntimeParameter(1)), step.arguments());
    UnboundPredicate predicate = Target::isOk;
    var predicateStep =
        AsmLambdaAnalyzer.inspect(
                LambdaSerialization.toSerializedLambda(predicate, "Expected lambda"))
            .last();
    assertEquals(null, predicateStep.receiver());
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
