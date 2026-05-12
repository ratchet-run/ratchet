package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import jakarta.transaction.Transactional;
import java.lang.reflect.Method;
import java.time.Duration;
import java.time.ZoneId;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import run.ratchet.api.SerializableCheckedRunnable;

class DefaultJobSchedulerServiceTransactionContractTest {

  @Test
  void listenerRegistration_isNotSupportedTransactionAttribute() throws NoSuchMethodException {
    assertNotSupported("addEventListener");
    assertNotSupported("removeEventListener");
  }

  @Test
  void builderFactories_areSupportsTransactionAttribute() throws NoSuchMethodException {
    assertSupports("enqueue", SerializableCheckedRunnable.class);
    assertSupports("schedule", Duration.class, SerializableCheckedRunnable.class);
    assertSupports("enqueueBatch", String.class);
    assertSupports("streamingBatch", String.class);
    assertSupports(
        "scheduleRecurring", String.class, ZoneId.class, SerializableCheckedRunnable.class);
  }

  private static void assertNotSupported(String methodName) throws NoSuchMethodException {
    Method method = DefaultJobSchedulerService.class.getMethod(methodName, Consumer.class);
    Transactional transactional = method.getAnnotation(Transactional.class);

    assertNotNull(transactional, methodName + " must declare the public API TX attribute");
    assertEquals(Transactional.TxType.NOT_SUPPORTED, transactional.value());
  }

  private static void assertSupports(String methodName, Class<?>... parameterTypes)
      throws NoSuchMethodException {
    Method method = DefaultJobSchedulerService.class.getMethod(methodName, parameterTypes);
    Transactional transactional = method.getAnnotation(Transactional.class);

    assertNotNull(transactional, methodName + " must declare the public API TX attribute");
    assertEquals(Transactional.TxType.SUPPORTS, transactional.value());
  }
}
