package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import jakarta.transaction.Transactional;
import jakarta.transaction.Transactional.TxType;
import org.junit.jupiter.api.Test;

class PostExecutionHandlerTransactionContractTest {

  @Test
  void handlerUsesRequiresNewAndRollsBackOnCheckedExceptions() {
    Transactional transactional = PostExecutionHandler.class.getAnnotation(Transactional.class);

    assertNotNull(transactional);
    assertEquals(TxType.REQUIRES_NEW, transactional.value());
    assertArrayEquals(new Class<?>[] {Exception.class}, transactional.rollbackOn());
  }
}
