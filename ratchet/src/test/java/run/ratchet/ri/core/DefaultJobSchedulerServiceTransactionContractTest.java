package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import jakarta.transaction.Transactional;
import java.lang.reflect.Method;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

class DefaultJobSchedulerServiceTransactionContractTest {

  @Test
  void listenerRegistration_isNotSupportedTransactionAttribute() throws NoSuchMethodException {
    assertNotSupported("addEventListener");
    assertNotSupported("removeEventListener");
  }

  private static void assertNotSupported(String methodName) throws NoSuchMethodException {
    Method method = DefaultJobSchedulerService.class.getMethod(methodName, Consumer.class);
    Transactional transactional = method.getAnnotation(Transactional.class);

    assertNotNull(transactional, methodName + " must declare the public API TX attribute");
    assertEquals(Transactional.TxType.NOT_SUPPORTED, transactional.value());
  }
}
