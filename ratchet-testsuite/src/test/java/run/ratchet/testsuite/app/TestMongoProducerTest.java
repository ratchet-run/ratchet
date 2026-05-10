package run.ratchet.testsuite.app;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Modifier;
import org.junit.jupiter.api.Test;

class TestMongoProducerTest {

  @Test
  void producedClientReadDuringShutdown_isVolatile() throws Exception {
    assertTrue(
        Modifier.isVolatile(TestMongoProducer.class.getDeclaredField("client").getModifiers()));
  }
}
