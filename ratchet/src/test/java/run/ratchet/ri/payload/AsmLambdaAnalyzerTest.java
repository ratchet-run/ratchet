package run.ratchet.ri.payload;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

  private static void invoke(Method method) throws Throwable {
    try {
      method.invoke(null, new ArrayDeque<>(), (IntBinaryOperator) Integer::sum);
    } catch (InvocationTargetException e) {
      throw e.getCause();
    }
  }
}
