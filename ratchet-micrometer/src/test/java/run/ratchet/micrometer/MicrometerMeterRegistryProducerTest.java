package run.ratchet.micrometer;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import io.micrometer.core.instrument.MeterRegistry;
import jakarta.enterprise.context.ApplicationScoped;
import java.lang.reflect.Method;
import org.junit.jupiter.api.Test;

class MicrometerMeterRegistryProducerTest {

  @Test
  void defaultRegistryProducerDeclaresApplicationScope() throws NoSuchMethodException {
    Method method = MicrometerMeterRegistryProducer.class.getMethod("defaultRegistry");

    assertNotNull(method.getAnnotation(ApplicationScoped.class));
  }

  @Test
  void defaultRegistryReturnsSharedRegistryInstance() {
    MicrometerMeterRegistryProducer producer = new MicrometerMeterRegistryProducer();

    MeterRegistry first = producer.defaultRegistry();
    MeterRegistry second = producer.defaultRegistry();

    assertSame(first, second);
  }
}
