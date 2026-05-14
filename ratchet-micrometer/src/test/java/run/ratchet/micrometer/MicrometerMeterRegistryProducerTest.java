package run.ratchet.micrometer;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import io.micrometer.core.instrument.MeterRegistry;
import jakarta.inject.Singleton;
import java.lang.reflect.Method;
import org.junit.jupiter.api.Test;

class MicrometerMeterRegistryProducerTest {

  @Test
  void defaultRegistryProducerDeclaresSingletonScope() throws NoSuchMethodException {
    // @Singleton (not @ApplicationScoped) so Weld doesn't try to proxy abstract MeterRegistry
    // (WELD-001435). Same instance semantics, no proxy required.
    Method method = MicrometerMeterRegistryProducer.class.getMethod("defaultRegistry");

    assertNotNull(method.getAnnotation(Singleton.class));
  }

  @Test
  void defaultRegistryReturnsSharedRegistryInstance() {
    MicrometerMeterRegistryProducer producer = new MicrometerMeterRegistryProducer();

    MeterRegistry first = producer.defaultRegistry();
    MeterRegistry second = producer.defaultRegistry();

    assertSame(first, second);
  }
}
