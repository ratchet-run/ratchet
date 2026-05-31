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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.function.IntBinaryOperator;
import org.junit.jupiter.api.Test;

class AsmLambdaAnalyzerTest {

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
