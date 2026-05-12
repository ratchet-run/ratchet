package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import jakarta.transaction.Transactional;
import java.lang.reflect.Method;
import java.util.List;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobBuilder;

class DefaultJobCreationServiceTransactionContractTest {

  @Test
  void submitMethodsDeclareRequiredTransactionAttribute() throws NoSuchMethodException {
    List<Method> submitMethods =
        List.of(
            DefaultJobCreationService.class.getMethod("submit", JobBuilder.class),
            DefaultJobCreationService.class.getMethod("submit", DefaultBatchBuilder.class),
            DefaultJobCreationService.class.getMethod("submit", DefaultStreamingBatchBuilder.class),
            DefaultJobCreationService.class.getMethod("submit", DefaultRecurringJobBuilder.class));

    for (Method method : submitMethods) {
      Transactional transactional = method.getAnnotation(Transactional.class);
      assertNotNull(transactional, method + " must declare the public API TX attribute");
      assertEquals(Transactional.TxType.REQUIRED, transactional.value());
    }
  }
}
